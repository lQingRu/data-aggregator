package com.dataaggregator.worker;

import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.workflow.HybridChunkSearchWorkflow;
import com.dataaggregator.workflow.WorkerCommand;
import com.dataaggregator.workflow.WorkerCompletionEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class HybridChunkSearchWorkers {

    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
    private static final int LEXICAL_DEFAULT_LIMIT = 10_000;
    private static final int SEMANTIC_DEFAULT_LIMIT = 1_500;

    private final MockInvestmentResearchCorpus corpus;
    private final DataAggregatorProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final RabbitTemplate rabbitTemplate;

    public HybridChunkSearchWorkers(
            MockInvestmentResearchCorpus corpus,
            DataAggregatorProperties properties,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            RabbitTemplate rabbitTemplate) {
        this.corpus = corpus;
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Transactional
    public void runLexicalRetrieval(WorkerCommand command) {
        if (!canCommit(command)) {
            return;
        }

        RetrievalFilters filters = retrievalFilters(command);
        List<String> keywords = queryTerms(textParam(command, "keywords"));
        List<RankedChunk> rankedChunks = corpus.chunks().stream()
                .filter(filters::matches)
                .map(chunk -> new ScoredChunk(chunk, lexicalScore(chunk, keywords)))
                .filter(scored -> keywords.isEmpty() || scored.score() > 0)
                .sorted(Comparator.comparing(ScoredChunk::score)
                        .reversed()
                        .thenComparing(scored -> scored.chunk().publishedAt(), Comparator.reverseOrder())
                        .thenComparing(scored -> scored.chunk().chunkId()))
                .limit(limit(command, LEXICAL_DEFAULT_LIMIT))
                .map(new Ranker())
                .toList();

        for (RankedChunk rankedChunk : rankedChunks) {
            upsertContribution(
                    command,
                    rankedChunk.chunk(),
                    "source_contribution",
                    rankedChunk.rank(),
                    decimal(rankedChunk.score(), 4),
                    contributionPayload(
                            rankedChunk.chunk(), Map.of("lexical_rank", rankedChunk.rank(), "match_terms", keywords)));
        }
        markStepCompleted(command);
        publishCompletionAfterCommit(completedEvent(command, rankedChunks.size()));
    }

    @Transactional
    public void runSemanticRetrieval(WorkerCommand command) {
        if (!canCommit(command)) {
            return;
        }

        RetrievalFilters filters = retrievalFilters(command);
        List<String> questionTerms = queryTerms(textParam(command, "question"));
        List<RankedChunk> rankedChunks = corpus.chunks().stream()
                .filter(filters::matches)
                .map(chunk -> new ScoredChunk(chunk, semanticScore(chunk, questionTerms)))
                .sorted(Comparator.comparing(ScoredChunk::score)
                        .reversed()
                        .thenComparing(scored -> scored.chunk().publishedAt(), Comparator.reverseOrder())
                        .thenComparing(scored -> scored.chunk().chunkId()))
                .limit(limit(command, SEMANTIC_DEFAULT_LIMIT))
                .map(new Ranker())
                .toList();

        for (RankedChunk rankedChunk : rankedChunks) {
            upsertContribution(
                    command,
                    rankedChunk.chunk(),
                    "source_contribution",
                    rankedChunk.rank(),
                    decimal(rankedChunk.score(), 4),
                    contributionPayload(rankedChunk.chunk(), Map.of("question_terms", questionTerms)));
        }
        markStepCompleted(command);
        publishCompletionAfterCommit(completedEvent(command, rankedChunks.size()));
    }

    @Transactional
    public void runMockRelevanceScoring(WorkerCommand command) {
        if (!canCommit(command) || !isTerminal(command.operationId(), HybridChunkSearchWorkflow.SEMANTIC_RETRIEVAL)) {
            return;
        }

        List<SemanticContribution> semanticContributions = jdbcTemplate.query(
                """
                select chunk_id, rank, score
                from worker_contributions
                where search_run_id = ? and workflow_step_id = ?
                order by rank, chunk_id
                """,
                (rs, rowNum) ->
                        new SemanticContribution(rs.getString("chunk_id"), rs.getInt("rank"), rs.getDouble("score")),
                command.searchRunId(),
                HybridChunkSearchWorkflow.SEMANTIC_RETRIEVAL);
        List<String> questionTerms = queryTerms(textParam(command, "question"));

        for (SemanticContribution semanticContribution : semanticContributions) {
            InvestmentResearchChunk chunk = corpus.findByChunkId(semanticContribution.chunkId())
                    .orElseThrow(() -> new IllegalStateException("Unknown chunk: " + semanticContribution.chunkId()));
            double relevanceScore = relevanceScore(chunk, questionTerms, semanticContribution.semanticScore());
            upsertContribution(
                    command,
                    chunk,
                    "relevance_score",
                    semanticContribution.rank(),
                    decimal(relevanceScore, 2),
                    contributionPayload(chunk, Map.of("source_step", HybridChunkSearchWorkflow.SEMANTIC_RETRIEVAL)));
        }
        markStepCompleted(command);
        publishCompletionAfterCommit(completedEvent(command, semanticContributions.size()));
    }

    @Transactional
    public void runSnapshotProjection(WorkerCommand command) {
        if (!canCommit(command) || !projectionDependenciesAreTerminal(command.operationId())) {
            return;
        }

        List<ProjectedContribution> contributions = jdbcTemplate.query(
                """
                select chunk_id, workflow_step_id, contribution_type, rank, score
                from worker_contributions
                where search_run_id = ?
                order by chunk_id, workflow_step_id
                """,
                (rs, rowNum) -> new ProjectedContribution(
                        rs.getString("chunk_id"),
                        rs.getString("workflow_step_id"),
                        rs.getString("contribution_type"),
                        rs.getObject("rank", Integer.class),
                        rs.getBigDecimal("score")),
                command.searchRunId());
        Map<String, ProjectedItem> projectedItems = projectedItems(contributions);
        List<ProjectedItem> rankedItems = projectedItems.values().stream()
                .sorted(Comparator.comparing(
                                ProjectedItem::relevanceScore, Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(ProjectedItem::lexicalRank, Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(item -> item.chunk().publishedAt(), Comparator.reverseOrder())
                        .thenComparing(item -> item.chunk().chunkId()))
                .toList();

        int defaultRank = 1;
        for (ProjectedItem projectedItem : rankedItems) {
            upsertResultItem(command, projectedItem, defaultRank);
            defaultRank++;
        }
        updateSnapshotSchema(command.resultSnapshotId());
        markStepCompleted(command);
        publishCompletionAfterCommit(completedEvent(command, rankedItems.size()));
    }

    private Map<String, ProjectedItem> projectedItems(List<ProjectedContribution> contributions) {
        Map<String, ProjectedItem> projectedItems = new LinkedHashMap<>();
        for (ProjectedContribution contribution : contributions) {
            InvestmentResearchChunk chunk = corpus.findByChunkId(contribution.chunkId())
                    .orElseThrow(() -> new IllegalStateException("Unknown chunk: " + contribution.chunkId()));
            ProjectedItem existing = projectedItems.computeIfAbsent(
                    contribution.chunkId(), ignored -> new ProjectedItem(chunk, null, null, new LinkedHashSet<>()));
            if ("source_contribution".equals(contribution.contributionType())) {
                existing.sourceContributions().add(contribution.workflowStepId());
            }
            if (HybridChunkSearchWorkflow.LEXICAL_RETRIEVAL.equals(contribution.workflowStepId())) {
                existing.setLexicalRank(contribution.rank());
            }
            if (HybridChunkSearchWorkflow.MOCK_RELEVANCE_SCORING.equals(contribution.workflowStepId())) {
                existing.setRelevanceScore(contribution.score());
            }
        }
        return projectedItems;
    }

    private boolean canCommit(WorkerCommand command) {
        Boolean canCommit = jdbcTemplate.queryForObject(
                """
                select exists (
                  select 1
                  from search_runs sr
                  join async_runs ar on ar.id = sr.async_run_id
                  join async_run_steps step on step.async_run_id = ar.id
                  where sr.id = ?
                    and ar.status in ('queued', 'running', 'waiting_retry')
                    and step.workflow_step_id = ?
                    and step.status = 'queued'
                    and step.attempt_count = ?
                )
                """,
                Boolean.class,
                command.searchRunId(),
                command.workflowStepId(),
                command.attempt());
        return Boolean.TRUE.equals(canCommit);
    }

    private boolean projectionDependenciesAreTerminal(String operationId) {
        return isTerminal(operationId, HybridChunkSearchWorkflow.LEXICAL_RETRIEVAL)
                && isTerminal(operationId, HybridChunkSearchWorkflow.SEMANTIC_RETRIEVAL)
                && isTerminal(operationId, HybridChunkSearchWorkflow.MOCK_RELEVANCE_SCORING);
    }

    private boolean isTerminal(String operationId, String workflowStepId) {
        Boolean terminal = jdbcTemplate.queryForObject(
                """
                select exists (
                  select 1
                  from async_run_steps
                  where async_run_id = ?
                    and workflow_step_id = ?
                    and status in (
                      'completed', 'completed_with_warnings', 'failed', 'cancelled', 'superseded'
                    )
                )
                """,
                Boolean.class,
                operationId,
                workflowStepId);
        return Boolean.TRUE.equals(terminal);
    }

    private void upsertContribution(
            WorkerCommand command,
            InvestmentResearchChunk chunk,
            String contributionType,
            Integer rank,
            BigDecimal score,
            Map<String, Object> payload) {
        jdbcTemplate.update(
                """
                insert into worker_contributions (
                  id, search_run_id, workflow_step_id, chunk_id, contribution_type, rank, score, payload_json
                ) values (?, ?, ?, ?, ?, ?, ?, cast(? as jsonb))
                on conflict (search_run_id, workflow_step_id, chunk_id)
                do update set
                  contribution_type = excluded.contribution_type,
                  rank = excluded.rank,
                  score = excluded.score,
                  payload_json = excluded.payload_json
                """,
                stableId("contrib", command.searchRunId(), command.workflowStepId(), chunk.chunkId()),
                command.searchRunId(),
                command.workflowStepId(),
                chunk.chunkId(),
                contributionType,
                rank,
                score,
                json(payload));
    }

    private void upsertResultItem(WorkerCommand command, ProjectedItem item, int defaultRank) {
        InvestmentResearchChunk chunk = item.chunk();
        jdbcTemplate.update(
                """
                insert into result_items (
                  id, result_snapshot_id, chunk_id, parent_entity_id, parent_title, parent_type, source_name,
                  ticker, company_name, sector, region, published_at, author, chunk_index, chunk_text,
                  relevance_score, lexical_rank, default_rank, source_contributions_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, cast(? as jsonb))
                on conflict (result_snapshot_id, chunk_id)
                do update set
                  parent_entity_id = excluded.parent_entity_id,
                  parent_title = excluded.parent_title,
                  parent_type = excluded.parent_type,
                  source_name = excluded.source_name,
                  ticker = excluded.ticker,
                  company_name = excluded.company_name,
                  sector = excluded.sector,
                  region = excluded.region,
                  published_at = excluded.published_at,
                  author = excluded.author,
                  chunk_index = excluded.chunk_index,
                  chunk_text = excluded.chunk_text,
                  relevance_score = excluded.relevance_score,
                  lexical_rank = excluded.lexical_rank,
                  default_rank = excluded.default_rank,
                  source_contributions_json = excluded.source_contributions_json
                """,
                stableId("item", command.resultSnapshotId(), chunk.chunkId()),
                command.resultSnapshotId(),
                chunk.chunkId(),
                chunk.parentEntityId(),
                chunk.parentTitle(),
                chunk.parentType(),
                chunk.sourceName(),
                chunk.ticker(),
                chunk.companyName(),
                chunk.sector(),
                chunk.region(),
                Timestamp.from(chunk.publishedAt()),
                chunk.author(),
                chunk.chunkIndex(),
                chunk.chunkText(),
                item.relevanceScore(),
                item.lexicalRank(),
                defaultRank,
                json(new ArrayList<>(item.sourceContributions())));
    }

    private void updateSnapshotSchema(String resultSnapshotId) {
        jdbcTemplate.update(
                """
                update result_snapshots
                set status = 'ready',
                    schema_json = cast(? as jsonb),
                    default_sort_json = cast(? as jsonb),
                    ready_at = coalesce(ready_at, now())
                where id = ?
                """,
                json(snapshotSchema(resultSnapshotId)),
                json(defaultSort()),
                resultSnapshotId);
    }

    private Map<String, Object> snapshotSchema(String resultSnapshotId) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("snapshot_id", resultSnapshotId);
        schema.put(
                "fields",
                List.of(
                        field("parent_title", "string", true, true, false, false, false),
                        field("parent_type", "enum", true, true, true, false, false),
                        field("source_name", "enum", true, true, true, false, false),
                        field("ticker", "string", true, true, true, false, false),
                        field("company_name", "string", true, true, true, false, false),
                        field("sector", "enum", true, true, true, false, false),
                        field("region", "enum", true, true, true, false, false),
                        field("published_at", "datetime", true, true, false, false, false),
                        field("author", "string", true, true, true, false, true),
                        field("relevance_score", "number", true, true, false, true, true),
                        field("lexical_rank", "number", true, true, false, true, true)));
        schema.put("default_sort", defaultSort());
        return schema;
    }

    private List<Map<String, Object>> defaultSort() {
        return List.of(
                Map.of("field", "relevance_score", "direction", "desc", "nulls", "last"),
                Map.of("field", "published_at", "direction", "desc", "nulls", "last"));
    }

    private Map<String, Object> field(
            String name,
            String type,
            boolean filterable,
            boolean sortable,
            boolean groupable,
            boolean aggregatable,
            boolean nullable) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("name", name);
        field.put("type", type);
        field.put("filterable", filterable);
        field.put("sortable", sortable);
        field.put("groupable", groupable);
        field.put("aggregatable", aggregatable);
        field.put("nullable", nullable);
        return field;
    }

    private void markStepCompleted(WorkerCommand command) {
        jdbcTemplate.update(
                """
                update async_run_steps
                set status = 'completed', completed_at = now(), updated_at = now()
                where async_run_id = ? and workflow_step_id = ? and attempt_count = ?
                """,
                command.operationId(),
                command.workflowStepId(),
                command.attempt());
    }

    private WorkerCompletionEvent completedEvent(WorkerCommand command, int contributionCount) {
        return new WorkerCompletionEvent(
                stableId("evt", command.searchRunId(), command.workflowStepId(), String.valueOf(command.attempt())),
                command.operationId(),
                command.searchRunId(),
                command.workflowStepId(),
                "completed",
                contributionCount,
                0,
                command.attempt(),
                null,
                Instant.now());
    }

    private void publishCompletionAfterCommit(WorkerCompletionEvent event) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            publishCompletion(event);
            return;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                publishCompletion(event);
            }
        });
    }

    private void publishCompletion(WorkerCompletionEvent event) {
        rabbitTemplate.send(
                properties.workflow().completionEventQueue(),
                MessageBuilder.withBody(jsonBytes(event))
                        .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                        .build());
    }

    private RetrievalFilters retrievalFilters(WorkerCommand command) {
        return RetrievalFilters.from(jsonObject(command.params().getOrDefault("retrieval_filters", Map.of())));
    }

    private Map<String, Object> contributionPayload(InvestmentResearchChunk chunk, Map<String, Object> extraValues) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("chunk_id", chunk.chunkId());
        payload.put("parent_entity_id", chunk.parentEntityId());
        payload.put("parent_title", chunk.parentTitle());
        payload.put("parent_type", chunk.parentType());
        payload.put("source_name", chunk.sourceName());
        payload.put("ticker", chunk.ticker());
        payload.put("company_name", chunk.companyName());
        payload.put("sector", chunk.sector());
        payload.put("region", chunk.region());
        payload.put("published_at", chunk.publishedAt().toString());
        payload.put("author", chunk.author());
        payload.put("chunk_index", chunk.chunkIndex());
        payload.putAll(extraValues);
        return payload;
    }

    private Map<String, Object> jsonObject(Object value) {
        return objectMapper.convertValue(value, JSON_OBJECT);
    }

    private String textParam(WorkerCommand command, String name) {
        Object value = command.params().get(name);
        return value == null ? "" : value.toString();
    }

    private int limit(WorkerCommand command, int defaultLimit) {
        Object value = command.params().get("limit");
        if (value instanceof Number number) {
            return Math.min(number.intValue(), defaultLimit);
        }
        if (value != null) {
            return Math.min(Integer.parseInt(value.toString()), defaultLimit);
        }
        return defaultLimit;
    }

    private List<String> queryTerms(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        for (String token : text.toLowerCase().split("[^a-z0-9]+")) {
            if (token.length() > 2) {
                terms.add(token);
            }
        }
        return List.copyOf(terms);
    }

    private double lexicalScore(InvestmentResearchChunk chunk, List<String> terms) {
        String haystack = searchableText(chunk);
        double score = 0;
        for (String term : terms) {
            if (haystack.contains(term)) {
                score += 1.0;
            }
        }
        if (!terms.isEmpty() && haystack.contains(String.join(" ", terms))) {
            score += 2.0;
        }
        return score;
    }

    private double semanticScore(InvestmentResearchChunk chunk, List<String> questionTerms) {
        String haystack = searchableText(chunk);
        double overlap = 0;
        for (String term : questionTerms) {
            if (haystack.contains(term)) {
                overlap += 1.0;
            }
        }
        double normalizedOverlap = questionTerms.isEmpty() ? 0.0 : overlap / questionTerms.size();
        return normalizedOverlap * 0.75 + stableUnitScore(chunk.chunkId(), questionTerms) * 0.25;
    }

    private double relevanceScore(InvestmentResearchChunk chunk, List<String> questionTerms, double semanticScore) {
        double domainBoost = "financials".equals(chunk.sector()) ? 1.15 : 1.0;
        double rawScore = Math.min(
                10.0, (semanticScore * 8.0 + stableUnitScore(chunk.ticker(), questionTerms) * 2.0) * domainBoost);
        return Math.max(0.0, rawScore);
    }

    private String searchableText(InvestmentResearchChunk chunk) {
        return (chunk.parentTitle() + " " + chunk.ticker() + " " + chunk.companyName() + " " + chunk.sector() + " "
                        + chunk.region() + " " + chunk.chunkText())
                .toLowerCase();
    }

    private double stableUnitScore(String value, List<String> terms) {
        int hash = (value + "|" + String.join(",", terms)).hashCode() & 0x7fffffff;
        return (hash % 10_000) / 10_000.0;
    }

    private BigDecimal decimal(double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }

    private String stableId(String prefix, String... parts) {
        String raw = String.join(":", parts);
        return prefix + "_"
                + UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8))
                        .toString()
                        .replace("-", "");
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Could not serialize JSON value", exception);
        }
    }

    private byte[] jsonBytes(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Could not serialize JSON value", exception);
        }
    }

    private record ScoredChunk(InvestmentResearchChunk chunk, double score) {}

    private record RankedChunk(InvestmentResearchChunk chunk, int rank, double score) {}

    private final class Ranker implements java.util.function.Function<ScoredChunk, RankedChunk> {

        private int nextRank = 1;

        @Override
        public RankedChunk apply(ScoredChunk scoredChunk) {
            return new RankedChunk(scoredChunk.chunk(), nextRank++, scoredChunk.score());
        }
    }

    private record SemanticContribution(String chunkId, int rank, double semanticScore) {}

    private record ProjectedContribution(
            String chunkId, String workflowStepId, String contributionType, Integer rank, BigDecimal score) {}

    private static final class ProjectedItem {

        private final InvestmentResearchChunk chunk;
        private BigDecimal relevanceScore;
        private Integer lexicalRank;
        private final Set<String> sourceContributions;

        private ProjectedItem(
                InvestmentResearchChunk chunk,
                BigDecimal relevanceScore,
                Integer lexicalRank,
                Set<String> sourceContributions) {
            this.chunk = chunk;
            this.relevanceScore = relevanceScore;
            this.lexicalRank = lexicalRank;
            this.sourceContributions = sourceContributions;
        }

        private InvestmentResearchChunk chunk() {
            return chunk;
        }

        private BigDecimal relevanceScore() {
            return relevanceScore;
        }

        private void setRelevanceScore(BigDecimal relevanceScore) {
            this.relevanceScore = relevanceScore;
        }

        private Integer lexicalRank() {
            return lexicalRank;
        }

        private void setLexicalRank(Integer lexicalRank) {
            this.lexicalRank = lexicalRank;
        }

        private Set<String> sourceContributions() {
            return sourceContributions;
        }
    }
}
