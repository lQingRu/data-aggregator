package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.support.IntegrationTestContainers;
import com.dataaggregator.worker.HybridChunkSearchWorkers;
import com.dataaggregator.workflow.HybridChunkSearchStartRequest;
import com.dataaggregator.workflow.HybridChunkSearchWorkflow;
import com.dataaggregator.workflow.SearchRunIds;
import com.dataaggregator.workflow.WorkerCommand;
import com.dataaggregator.workflow.WorkerCompletionEvent;
import com.dataaggregator.workflow.WorkflowOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(properties = "spring.rabbitmq.listener.simple.auto-startup=false")
class MockInvestmentResearchWorkersIT extends IntegrationTestContainers {

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private DataAggregatorProperties properties;

    @Autowired
    private HybridChunkSearchWorkers workers;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private WorkflowOrchestrator orchestrator;

    @BeforeEach
    void purgeQueues() {
        workflow().stepDefinitions().forEach(step -> amqpAdmin.purgeQueue(step.queue(), true));
        amqpAdmin.purgeQueue(properties.workflow().completionEventQueue(), true);
    }

    @Test
    void lexicalRetrievalWritesFilteredRankedContributionsBeforeCompletionEvent() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        WorkerCommand lexicalCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        workers.runLexicalRetrieval(lexicalCommand);

        List<Integer> ranks = jdbcTemplate.queryForList(
                """
                select rank
                from worker_contributions
                where search_run_id = ? and workflow_step_id = 'lexical_retrieval'
                order by rank
                """,
                Integer.class,
                ids.searchRunId());
        WorkerCompletionEvent event = receiveCompletionEvent();

        assertThat(ranks).isNotEmpty();
        assertThat(ranks)
                .containsExactlyElementsOf(
                        IntStream.rangeClosed(1, ranks.size()).boxed().toList());
        assertThat(nonMatchingFinancialFilterRows(ids.searchRunId(), "lexical_retrieval"))
                .isZero();
        assertThat(stepStatus(ids.operationId(), "lexical_retrieval")).isEqualTo("completed");
        assertThat(event.workflowStepId()).isEqualTo("lexical_retrieval");
        assertThat(event.status()).isEqualTo("completed");
        assertThat(event.contributionCount()).isEqualTo(ranks.size());
    }

    @Test
    void semanticRetrievalAndRelevanceScoringAreDeterministicAndIdempotent() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        WorkerCommand semanticCommand =
                receiveCommand(workflow().semanticRetrieval().queue());

        workers.runSemanticRetrieval(semanticCommand);
        WorkerCompletionEvent semanticEvent = receiveCompletionEvent();
        orchestrator.handleWorkerCompletion(semanticEvent);

        WorkerCommand scoringCommand =
                receiveCommand(workflow().mockRelevanceScoring().queue());
        workers.runMockRelevanceScoring(scoringCommand);
        List<Double> firstScores = relevanceScores(ids.searchRunId());

        resetStepForSameAttempt(ids.operationId(), "mock_relevance_scoring", scoringCommand.attempt());
        workers.runMockRelevanceScoring(scoringCommand);
        List<Double> secondScores = relevanceScores(ids.searchRunId());

        assertThat(nonMatchingFinancialFilterRows(ids.searchRunId(), "semantic_retrieval"))
                .isZero();
        assertThat(firstScores).isNotEmpty();
        assertThat(firstScores).allSatisfy(score -> assertThat(score).isBetween(0.0, 10.0));
        assertThat(secondScores).containsExactlyElementsOf(firstScores);
        assertThat(contributionCount(ids.searchRunId(), "mock_relevance_scoring"))
                .isEqualTo(contributionCount(ids.searchRunId(), "semantic_retrieval"));
    }

    @Test
    void snapshotProjectionMaterializesResultItemsAndMarksSnapshotReady() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        WorkerCommand lexicalCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        WorkerCommand semanticCommand =
                receiveCommand(workflow().semanticRetrieval().queue());

        workers.runLexicalRetrieval(lexicalCommand);
        orchestrator.handleWorkerCompletion(receiveCompletionEvent());
        workers.runSemanticRetrieval(semanticCommand);
        orchestrator.handleWorkerCompletion(receiveCompletionEvent());

        WorkerCommand scoringCommand =
                receiveCommand(workflow().mockRelevanceScoring().queue());
        workers.runMockRelevanceScoring(scoringCommand);
        orchestrator.handleWorkerCompletion(receiveCompletionEvent());

        WorkerCommand projectionCommand =
                receiveCommand(workflow().snapshotProjection().queue());
        workers.runSnapshotProjection(projectionCommand);

        WorkerCompletionEvent projectionEvent = receiveCompletionEvent();
        Integer itemCount = jdbcTemplate.queryForObject(
                "select count(*) from result_items where result_snapshot_id = ?",
                Integer.class,
                ids.resultSnapshotId());
        Integer rankedItemCount = jdbcTemplate.queryForObject(
                """
                select count(*)
                from result_items
                where result_snapshot_id = ?
                  and default_rank > 0
                  and jsonb_array_length(source_contributions_json) > 0
                """,
                Integer.class,
                ids.resultSnapshotId());

        assertThat(itemCount).isPositive();
        assertThat(rankedItemCount).isEqualTo(itemCount);
        assertThat(snapshotStatus(ids.resultSnapshotId())).isEqualTo("ready");
        assertThat(projectionEvent.workflowStepId()).isEqualTo("snapshot_projection");
        assertThat(projectionEvent.contributionCount()).isEqualTo(itemCount);

        orchestrator.handleWorkerCompletion(projectionEvent);
        assertThat(runStatus(ids.operationId())).isEqualTo("completed");
    }

    private HybridChunkSearchStartRequest searchRequest() {
        return new HybridChunkSearchStartRequest(
                "user_test",
                "digital wallet adoption",
                "Which markets show the strongest payment growth?",
                Map.of(
                        "sector",
                        List.of("financials"),
                        "ticker",
                        List.of("V", "MA"),
                        "region",
                        List.of("APAC", "Global"),
                        "published_at",
                        Map.of("from", "2025-01-01T00:00:00Z", "to", "2026-12-31T23:59:59Z")),
                Map.of("field", "relevance_score", "direction", "desc"));
    }

    private WorkerCommand receiveCommand(String queue) throws Exception {
        Message message = rabbitTemplate.receive(queue, 5000);
        assertThat(message).as("message in %s", queue).isNotNull();
        return objectMapper.readValue(message.getBody(), WorkerCommand.class);
    }

    private WorkerCompletionEvent receiveCompletionEvent() throws Exception {
        Message message = rabbitTemplate.receive(properties.workflow().completionEventQueue(), 5000);
        assertThat(message).as("worker completion event").isNotNull();
        return objectMapper.readValue(message.getBody(), WorkerCompletionEvent.class);
    }

    private int nonMatchingFinancialFilterRows(String searchRunId, String workflowStepId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                select count(*)
                from worker_contributions
                where search_run_id = ?
                  and workflow_step_id = ?
                  and not (
                    payload_json ->> 'sector' = 'financials'
                    and payload_json ->> 'ticker' in ('V', 'MA')
                    and payload_json ->> 'region' in ('APAC', 'Global')
                  )
                """,
                Integer.class,
                searchRunId,
                workflowStepId);
        return count == null ? 0 : count;
    }

    private List<Double> relevanceScores(String searchRunId) {
        return jdbcTemplate.queryForList(
                """
                select score::double precision
                from worker_contributions
                where search_run_id = ? and workflow_step_id = 'mock_relevance_scoring'
                order by rank, chunk_id
                """,
                Double.class,
                searchRunId);
    }

    private int contributionCount(String searchRunId, String workflowStepId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                select count(*)
                from worker_contributions
                where search_run_id = ? and workflow_step_id = ?
                """,
                Integer.class,
                searchRunId,
                workflowStepId);
        return count == null ? 0 : count;
    }

    private void resetStepForSameAttempt(String operationId, String workflowStepId, int attempt) {
        jdbcTemplate.update(
                """
                update async_run_steps
                set status = 'queued', attempt_count = ?, completed_at = null, updated_at = now()
                where async_run_id = ? and workflow_step_id = ?
                """,
                attempt,
                operationId,
                workflowStepId);
    }

    private String stepStatus(String operationId, String workflowStepId) {
        return jdbcTemplate.queryForObject(
                """
                select status
                from async_run_steps
                where async_run_id = ? and workflow_step_id = ?
                """,
                String.class,
                operationId,
                workflowStepId);
    }

    private String snapshotStatus(String snapshotId) {
        return jdbcTemplate.queryForObject(
                "select status from result_snapshots where id = ?", String.class, snapshotId);
    }

    private String runStatus(String operationId) {
        return jdbcTemplate.queryForObject("select status from async_runs where id = ?", String.class, operationId);
    }

    private HybridChunkSearchWorkflow workflow() {
        return HybridChunkSearchWorkflow.defaultWorkflow(properties.workflow().hybridChunkSearchVersion());
    }
}
