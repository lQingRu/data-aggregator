package com.dataaggregator.workflow;

import com.dataaggregator.config.DataAggregatorProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
@Profile("!worker")
public class WorkflowOrchestrator {

    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
    private static final List<String> ACTIVE_RUN_STATUSES = List.of("queued", "running", "waiting_retry");
    private static final List<String> TERMINAL_STEP_STATUSES =
            List.of("completed", "completed_with_warnings", "failed", "cancelled", "superseded");

    private final DataAggregatorProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final RabbitTemplate rabbitTemplate;

    public WorkflowOrchestrator(
            DataAggregatorProperties properties,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            RabbitTemplate rabbitTemplate) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Transactional
    public SearchRunIds startHybridChunkSearch(HybridChunkSearchStartRequest request) {
        SearchRunIds ids = new SearchRunIds(prefixedId("sr"), prefixedId("run"), prefixedId("snap"), prefixedId("op"));
        HybridChunkSearchWorkflow workflow = configuredWorkflow();

        jdbcTemplate.update(
                """
                insert into async_runs (
                  id, operation_type, status, scope_type, scope_id, user_id, current_step,
                  completed_steps, total_steps, warnings_json, metadata_json
                ) values (?, 'search_run', 'queued', 'result_snapshot', ?, ?, null, 0, ?, '[]'::jsonb, '{}'::jsonb)
                """,
                ids.operationId(),
                ids.resultSnapshotId(),
                request.userId(),
                workflow.stepDefinitions().size());
        jdbcTemplate.update(
                """
                insert into search_requests (
                  id, user_id, workflow, keywords, question, retrieval_filters_json, initial_sort_json
                ) values (?, ?, ?, ?, ?, cast(? as jsonb), cast(? as jsonb))
                """,
                ids.searchRequestId(),
                request.userId(),
                HybridChunkSearchWorkflow.WORKFLOW_ID,
                request.keywords(),
                request.question(),
                json(request.retrievalFilters()),
                json(request.initialSort()));
        jdbcTemplate.update(
                """
                insert into search_runs (
                  id, async_run_id, search_request_id, workflow, workflow_config_version, result_snapshot_id
                ) values (?, ?, ?, ?, ?, ?)
                """,
                ids.searchRunId(),
                ids.operationId(),
                ids.searchRequestId(),
                HybridChunkSearchWorkflow.WORKFLOW_ID,
                workflow.version(),
                ids.resultSnapshotId());
        jdbcTemplate.update(
                """
                insert into result_snapshots (
                  id, search_run_id, user_id, status, schema_json, default_sort_json
                ) values (?, ?, ?, 'pending', '{}'::jsonb,
                  '[{"field":"relevance_score","direction":"desc","nulls":"last"}]'::jsonb)
                """,
                ids.resultSnapshotId(),
                ids.searchRunId(),
                request.userId());

        for (WorkflowStepDefinition step : workflow.stepDefinitions()) {
            jdbcTemplate.update(
                    """
                    insert into async_run_steps (
                      id, async_run_id, workflow_step_id, status, required, attempt_count, max_attempts
                    ) values (?, ?, ?, 'queued', ?, 0, ?)
                    """,
                    prefixedId("step"),
                    ids.operationId(),
                    step.id(),
                    step.required(),
                    step.maxAttempts());
        }

        SearchRunContext context = loadAndLockContext(ids.searchRunId());
        scheduleReadySteps(context, workflow);
        return ids;
    }

    @RabbitListener(queues = "${data-aggregator.workflow.completion-event-queue}")
    @Transactional
    public void receiveWorkerCompletion(WorkerCompletionEvent event) {
        handleWorkerCompletion(event);
    }

    @Transactional
    public void handleWorkerCompletion(WorkerCompletionEvent event) {
        if (!"completed".equals(event.status()) && !"failed".equals(event.status())) {
            throw new IllegalArgumentException("Unsupported worker completion status: " + event.status());
        }
        recoverSearchRun(event.searchRunId());
    }

    @Transactional
    public void recoverSearchRun(String searchRunId) {
        advanceFromDurableState(searchRunId);
    }

    private void advanceFromDurableState(String searchRunId) {
        SearchRunContext context = loadAndLockContext(searchRunId);
        if (!ACTIVE_RUN_STATUSES.contains(context.runStatus())) {
            return;
        }

        HybridChunkSearchWorkflow workflow = configuredWorkflow();
        if (processDurableFailures(context, workflow)) {
            return;
        }

        markOptionalDependentsBlockedByWarnings(context, workflow);
        scheduleReadySteps(context, workflow);
        refreshRunProgress(context);
        completeRunIfProjectionFinished(context, workflow);
    }

    @Transactional
    public boolean canWorkerCommit(WorkerCommand command) {
        SearchRunContext context = loadAndLockContext(command.searchRunId());
        if (!ACTIVE_RUN_STATUSES.contains(context.runStatus())) {
            return false;
        }

        StepState step = loadStep(context.operationId(), command.workflowStepId());
        return "queued".equals(step.status()) && step.attemptCount() == command.attempt();
    }

    private boolean processDurableFailures(SearchRunContext context, HybridChunkSearchWorkflow workflow) {
        List<StepState> states = loadSteps(context.operationId());
        for (WorkflowStepDefinition step : workflow.stepDefinitions()) {
            StepState state = findStep(states, step.id());
            if (!"failed".equals(state.status())) {
                continue;
            }
            if (state.attemptCount() < state.maxAttempts()) {
                scheduleRetry(context, step, state.lastError());
                return true;
            }
            if (step.required()) {
                markRequiredFailure(context, state);
                return true;
            }
            appendWarning(context.operationId(), step.id(), state.lastError());
        }
        return false;
    }

    private void scheduleReadySteps(SearchRunContext context, HybridChunkSearchWorkflow workflow) {
        List<StepState> stepStates = loadSteps(context.operationId());
        for (WorkflowStepDefinition step : workflow.stepDefinitions()) {
            StepState state = findStep(stepStates, step.id());
            if (isReadyToSchedule(step, state, stepStates)) {
                scheduleStep(context, step, state.attemptCount() + 1, null);
            }
        }
    }

    private boolean isReadyToSchedule(WorkflowStepDefinition step, StepState state, List<StepState> stepStates) {
        return step.enabled()
                && "queued".equals(state.status())
                && state.attemptCount() == 0
                && dependenciesAreTerminal(step, stepStates);
    }

    private boolean dependenciesAreTerminal(WorkflowStepDefinition step, List<StepState> stepStates) {
        for (String dependency : step.dependsOn()) {
            if (!TERMINAL_STEP_STATUSES.contains(
                    findStep(stepStates, dependency).status())) {
                return false;
            }
        }
        return true;
    }

    private void scheduleRetry(SearchRunContext context, WorkflowStepDefinition step, String errorMessage) {
        StepState state = loadStep(context.operationId(), step.id());
        int nextAttempt = state.attemptCount() + 1;
        scheduleStep(context, step, nextAttempt, errorMessage);
        jdbcTemplate.update(
                """
                update async_runs
                set status = 'waiting_retry', current_step = ?, updated_at = now()
                where id = ?
                """,
                step.id(),
                context.operationId());
    }

    private void scheduleStep(
            SearchRunContext context, WorkflowStepDefinition step, int nextAttempt, String lastError) {
        jdbcTemplate.update(
                """
                update async_run_steps
                set status = 'queued', attempt_count = ?, last_error = ?, updated_at = now()
                where async_run_id = ? and workflow_step_id = ?
                """,
                nextAttempt,
                lastError,
                context.operationId(),
                step.id());
        jdbcTemplate.update(
                """
                update async_runs
                set status = 'running', current_step = ?, updated_at = now()
                where id = ?
                """,
                step.id(),
                context.operationId());
        publishAfterCommit(step.queue(), workerCommand(context, step, nextAttempt));
    }

    private WorkerCommand workerCommand(SearchRunContext context, WorkflowStepDefinition step, int attempt) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("keywords", context.keywords());
        params.put("question", context.question());
        params.put("retrieval_filters", jsonObject(context.retrievalFiltersJson()));
        params.put("limit", 1500);

        return new WorkerCommand(
                prefixedId("cmd"),
                context.operationId(),
                context.searchRunId(),
                HybridChunkSearchWorkflow.WORKFLOW_ID,
                step.id(),
                context.workflowConfigVersion(),
                context.resultSnapshotId(),
                context.userId(),
                params,
                attempt,
                Instant.now());
    }

    private void markRequiredFailure(SearchRunContext context, StepState failedStep) {
        appendWarning(context.operationId(), failedStep.workflowStepId(), failedStep.lastError());
        jdbcTemplate.update(
                """
                update async_runs
                set status = 'failed', current_step = ?, completed_steps = ?, updated_at = now()
                where id = ?
                """,
                failedStep.workflowStepId(),
                terminalStepCount(context.operationId()),
                context.operationId());
        jdbcTemplate.update(
                """
                update result_snapshots
                set status = 'failed'
                where id = ?
                """,
                context.resultSnapshotId());
    }

    private void markOptionalDependentsBlockedByWarnings(SearchRunContext context, HybridChunkSearchWorkflow workflow) {
        boolean changed;
        do {
            changed = false;
            List<StepState> states = loadSteps(context.operationId());
            for (WorkflowStepDefinition step : workflow.stepDefinitions()) {
                StepState state = findStep(states, step.id());
                if (step.required() || !"queued".equals(state.status()) || state.attemptCount() > 0) {
                    continue;
                }
                if (hasWarningDependency(step, states)) {
                    jdbcTemplate.update(
                            """
                            update async_run_steps
                            set status = 'completed_with_warnings', completed_at = now(), updated_at = now()
                            where async_run_id = ? and workflow_step_id = ?
                            """,
                            context.operationId(),
                            step.id());
                    appendWarning(context.operationId(), step.id(), "Skipped after optional dependency warning");
                    changed = true;
                }
            }
        } while (changed);
    }

    private boolean hasWarningDependency(WorkflowStepDefinition step, List<StepState> states) {
        for (String dependency : step.dependsOn()) {
            String dependencyStatus = findStep(states, dependency).status();
            if ("failed".equals(dependencyStatus) || "completed_with_warnings".equals(dependencyStatus)) {
                return true;
            }
        }
        return false;
    }

    private void refreshRunProgress(SearchRunContext context) {
        jdbcTemplate.update(
                """
                update async_runs
                set status = 'running', completed_steps = ?, updated_at = now()
                where id = ? and status in ('queued', 'running', 'waiting_retry')
                """,
                terminalStepCount(context.operationId()),
                context.operationId());
    }

    private void completeRunIfProjectionFinished(SearchRunContext context, HybridChunkSearchWorkflow workflow) {
        StepState projection =
                loadStep(context.operationId(), workflow.snapshotProjection().id());
        if (!"completed".equals(projection.status())) {
            return;
        }

        String finalStatus = warningCount(context.operationId()) > 0 ? "completed_with_warnings" : "completed";
        jdbcTemplate.update(
                """
                update async_runs
                set status = ?, current_step = null, completed_steps = total_steps, updated_at = now()
                where id = ?
                """,
                finalStatus,
                context.operationId());
        jdbcTemplate.update(
                """
                update result_snapshots
                set status = 'ready', ready_at = now()
                where id = ?
                """,
                context.resultSnapshotId());
    }

    private SearchRunContext loadAndLockContext(String searchRunId) {
        return jdbcTemplate.queryForObject(
                """
                select sr.id as search_run_id, sr.async_run_id, sr.result_snapshot_id, sr.workflow_config_version,
                  ar.status, ar.user_id, req.keywords, req.question, req.retrieval_filters_json::text
                from search_runs sr
                join async_runs ar on ar.id = sr.async_run_id
                join search_requests req on req.id = sr.search_request_id
                where sr.id = ?
                for update of ar
                """,
                (rs, rowNum) -> new SearchRunContext(
                        rs.getString("search_run_id"),
                        rs.getString("async_run_id"),
                        rs.getString("result_snapshot_id"),
                        rs.getInt("workflow_config_version"),
                        rs.getString("status"),
                        rs.getString("user_id"),
                        rs.getString("keywords"),
                        rs.getString("question"),
                        rs.getString("retrieval_filters_json")),
                searchRunId);
    }

    private List<StepState> loadSteps(String operationId) {
        return jdbcTemplate.query(
                """
                select workflow_step_id, status, required, attempt_count, max_attempts, last_error
                from async_run_steps
                where async_run_id = ?
                order by workflow_step_id
                """,
                (rs, rowNum) -> new StepState(
                        rs.getString("workflow_step_id"),
                        rs.getString("status"),
                        rs.getBoolean("required"),
                        rs.getInt("attempt_count"),
                        rs.getInt("max_attempts"),
                        rs.getString("last_error")),
                operationId);
    }

    private StepState loadStep(String operationId, String stepId) {
        return jdbcTemplate.queryForObject(
                """
                select workflow_step_id, status, required, attempt_count, max_attempts, last_error
                from async_run_steps
                where async_run_id = ? and workflow_step_id = ?
                """,
                (rs, rowNum) -> new StepState(
                        rs.getString("workflow_step_id"),
                        rs.getString("status"),
                        rs.getBoolean("required"),
                        rs.getInt("attempt_count"),
                        rs.getInt("max_attempts"),
                        rs.getString("last_error")),
                operationId,
                stepId);
    }

    private StepState findStep(List<StepState> states, String stepId) {
        for (StepState state : states) {
            if (state.workflowStepId().equals(stepId)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown workflow step: " + stepId);
    }

    private int terminalStepCount(String operationId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                select count(*)
                from async_run_steps
                where async_run_id = ? and status in ('completed', 'completed_with_warnings', 'failed',
                  'cancelled', 'superseded')
                """,
                Integer.class,
                operationId);
        return count == null ? 0 : count;
    }

    private int warningCount(String operationId) {
        Integer count = jdbcTemplate.queryForObject(
                "select jsonb_array_length(warnings_json) from async_runs where id = ?", Integer.class, operationId);
        return count == null ? 0 : count;
    }

    private void appendWarning(String operationId, String workflowStepId, String message) {
        String warningMessage = message == null ? "Worker step failed" : message;
        if (warningExists(operationId, workflowStepId, warningMessage)) {
            return;
        }

        Map<String, Object> warning = new LinkedHashMap<>();
        warning.put("workflow_step_id", workflowStepId);
        warning.put("message", warningMessage);
        List<Map<String, Object>> warnings = new ArrayList<>();
        warnings.add(warning);
        jdbcTemplate.update(
                """
                update async_runs
                set warnings_json = warnings_json || cast(? as jsonb), updated_at = now()
                where id = ?
                """,
                json(warnings),
                operationId);
    }

    private boolean warningExists(String operationId, String workflowStepId, String message) {
        Boolean exists = jdbcTemplate.queryForObject(
                """
                select exists (
                  select 1
                  from async_runs ar, jsonb_array_elements(ar.warnings_json) warning
                  where ar.id = ?
                    and warning ->> 'workflow_step_id' = ?
                    and warning ->> 'message' = ?
                )
                """,
                Boolean.class,
                operationId,
                workflowStepId,
                message);
        return Boolean.TRUE.equals(exists);
    }

    private HybridChunkSearchWorkflow configuredWorkflow() {
        return HybridChunkSearchWorkflow.defaultWorkflow(properties.workflow().hybridChunkSearchVersion())
                .withQueues(
                        properties.workflow().lexicalQueue(),
                        properties.workflow().semanticQueue(),
                        properties.workflow().relevanceScoreQueue(),
                        properties.workflow().snapshotProjectorQueue());
    }

    private Map<String, Object> jsonObject(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, JSON_OBJECT);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not parse stored JSON object", exception);
        }
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

    private void publishAfterCommit(String queue, WorkerCommand command) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            publishCommand(queue, command);
            return;
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                publishCommand(queue, command);
            }
        });
    }

    private void publishCommand(String queue, WorkerCommand command) {
        rabbitTemplate.send(
                queue,
                MessageBuilder.withBody(jsonBytes(command))
                        .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                        .build());
    }

    private String prefixedId(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private record SearchRunContext(
            String searchRunId,
            String operationId,
            String resultSnapshotId,
            int workflowConfigVersion,
            String runStatus,
            String userId,
            String keywords,
            String question,
            String retrievalFiltersJson) {}

    private record StepState(
            String workflowStepId,
            String status,
            boolean required,
            int attemptCount,
            int maxAttempts,
            String lastError) {}
}
