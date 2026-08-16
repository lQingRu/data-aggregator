package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.support.IntegrationTestContainers;
import com.dataaggregator.workflow.HybridChunkSearchStartRequest;
import com.dataaggregator.workflow.HybridChunkSearchWorkflow;
import com.dataaggregator.workflow.SearchRunIds;
import com.dataaggregator.workflow.WorkerCommand;
import com.dataaggregator.workflow.WorkerCompletionEvent;
import com.dataaggregator.workflow.WorkflowOrchestrator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
class WorkflowOrchestratorIT extends IntegrationTestContainers {

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private DataAggregatorProperties properties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @Autowired
    private WorkflowOrchestrator orchestrator;

    @BeforeEach
    void purgeWorkflowQueues() {
        workflow().stepDefinitions().forEach(step -> amqpAdmin.purgeQueue(step.queue(), true));
        amqpAdmin.purgeQueue(properties.workflow().completionEventQueue(), true);
    }

    @AfterEach
    void stopListenersBeforeContainersShutdown() {
        listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::stop);
    }

    @Test
    void startingSearchRunSchedulesInitialRetrievalSteps() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());

        WorkerCommand lexicalCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        WorkerCommand semanticCommand =
                receiveCommand(workflow().semanticRetrieval().queue());

        assertThat(lexicalCommand.workflowStepId()).isEqualTo("lexical_retrieval");
        assertThat(semanticCommand.workflowStepId()).isEqualTo("semantic_retrieval");
        assertThat(lexicalCommand.searchRunId()).isEqualTo(ids.searchRunId());
        assertThat(semanticCommand.resultSnapshotId()).isEqualTo(ids.resultSnapshotId());
        assertThat(stepAttemptCount(ids.operationId(), "lexical_retrieval")).isEqualTo(1);
        assertThat(stepAttemptCount(ids.operationId(), "semantic_retrieval")).isEqualTo(1);
        assertThat(stepAttemptCount(ids.operationId(), "mock_relevance_scoring"))
                .isZero();
        assertThat(runStatus(ids.operationId())).isEqualTo("running");
    }

    @Test
    void completionEventsAdvanceNewlyUnblockedDagSteps() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        markStepCompleted(ids.operationId(), "semantic_retrieval");
        orchestrator.handleWorkerCompletion(completed(ids, "semantic_retrieval"));

        WorkerCommand scoringCommand =
                receiveCommand(workflow().mockRelevanceScoring().queue());
        assertThat(scoringCommand.workflowStepId()).isEqualTo("mock_relevance_scoring");
        assertNoCommand(workflow().snapshotProjection().queue());

        markStepCompleted(ids.operationId(), "lexical_retrieval");
        orchestrator.handleWorkerCompletion(completed(ids, "lexical_retrieval"));
        assertNoCommand(workflow().snapshotProjection().queue());

        markStepCompleted(ids.operationId(), "mock_relevance_scoring");
        orchestrator.handleWorkerCompletion(completed(ids, "mock_relevance_scoring"));

        WorkerCommand projectionCommand =
                receiveCommand(workflow().snapshotProjection().queue());
        assertThat(projectionCommand.workflowStepId()).isEqualTo("snapshot_projection");
    }

    @Test
    void optionalFailureStillAllowsProjectionAndCompletesWithWarnings() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        markStepCompleted(ids.operationId(), "lexical_retrieval");
        orchestrator.handleWorkerCompletion(completed(ids, "lexical_retrieval"));
        markStepFailed(ids.operationId(), "semantic_retrieval", 3);
        orchestrator.handleWorkerCompletion(failed(ids, "semantic_retrieval", 3));

        WorkerCommand projectionCommand =
                receiveCommand(workflow().snapshotProjection().queue());
        assertThat(projectionCommand.workflowStepId()).isEqualTo("snapshot_projection");
        assertThat(stepStatus(ids.operationId(), "mock_relevance_scoring")).isEqualTo("completed_with_warnings");

        markStepCompleted(ids.operationId(), "snapshot_projection");
        orchestrator.handleWorkerCompletion(completed(ids, "snapshot_projection"));

        assertThat(runStatus(ids.operationId())).isEqualTo("completed_with_warnings");
        assertThat(snapshotStatus(ids.resultSnapshotId())).isEqualTo("ready");
    }

    @Test
    void requiredFailureAfterRetriesFailsOperation() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        markStepFailed(ids.operationId(), "lexical_retrieval", 3);
        orchestrator.handleWorkerCompletion(failed(ids, "lexical_retrieval", 3));

        assertThat(runStatus(ids.operationId())).isEqualTo("failed");
        assertNoCommand(workflow().snapshotProjection().queue());
    }

    @Test
    void failedStepBeforeRetryExhaustionSchedulesAnotherAttempt() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        WorkerCommand firstLexicalCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        markStepFailed(ids.operationId(), "lexical_retrieval", firstLexicalCommand.attempt());
        orchestrator.handleWorkerCompletion(failed(ids, "lexical_retrieval", firstLexicalCommand.attempt()));

        WorkerCommand retryCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        assertThat(retryCommand.attempt()).isEqualTo(2);
        assertThat(runStatus(ids.operationId())).isEqualTo("waiting_retry");
    }

    @Test
    void failedEventAttemptCannotExhaustRetriesWithoutPersistedAttempt() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        markStepFailed(ids.operationId(), "lexical_retrieval", 1);
        orchestrator.handleWorkerCompletion(failed(ids, "lexical_retrieval", 3));

        WorkerCommand retryCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        assertThat(retryCommand.attempt()).isEqualTo(2);
        assertThat(runStatus(ids.operationId())).isEqualTo("waiting_retry");
    }

    @Test
    void requiredProjectionFailureAfterRetriesFailsOperation() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());
        markStepCompleted(ids.operationId(), "lexical_retrieval");
        markStepCompleted(ids.operationId(), "semantic_retrieval");
        orchestrator.recoverSearchRun(ids.searchRunId());
        receiveCommand(workflow().mockRelevanceScoring().queue());
        markStepCompleted(ids.operationId(), "mock_relevance_scoring");
        orchestrator.recoverSearchRun(ids.searchRunId());
        receiveCommand(workflow().snapshotProjection().queue());

        markStepFailed(ids.operationId(), "snapshot_projection", 3);
        orchestrator.handleWorkerCompletion(failed(ids, "snapshot_projection", 3));

        assertThat(runStatus(ids.operationId())).isEqualTo("failed");
        assertThat(snapshotStatus(ids.resultSnapshotId())).isEqualTo("failed");
    }

    @Test
    void lostCompletionEventsCanBeRecoveredFromDurableState() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());
        markStepCompleted(ids.operationId(), "semantic_retrieval");

        orchestrator.recoverSearchRun(ids.searchRunId());

        WorkerCommand scoringCommand =
                receiveCommand(workflow().mockRelevanceScoring().queue());
        assertThat(scoringCommand.workflowStepId()).isEqualTo("mock_relevance_scoring");
    }

    @Test
    void cancelledRunDoesNotScheduleDownstreamWork() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());
        jdbcTemplate.update("update async_runs set status = 'cancelled' where id = ?", ids.operationId());
        markStepCompleted(ids.operationId(), "semantic_retrieval");

        orchestrator.handleWorkerCompletion(completed(ids, "semantic_retrieval"));

        assertNoCommand(workflow().mockRelevanceScoring().queue());
    }

    @Test
    void supersededRunDoesNotScheduleDownstreamWork() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());
        jdbcTemplate.update("update async_runs set status = 'superseded' where id = ?", ids.operationId());
        markStepCompleted(ids.operationId(), "semantic_retrieval");

        orchestrator.handleWorkerCompletion(completed(ids, "semantic_retrieval"));

        assertNoCommand(workflow().mockRelevanceScoring().queue());
    }

    @Test
    void workerCommitGuardRejectsInactiveOrStaleCommands() throws Exception {
        SearchRunIds ids = orchestrator.startHybridChunkSearch(searchRequest());
        WorkerCommand lexicalCommand =
                receiveCommand(workflow().lexicalRetrieval().queue());
        receiveCommand(workflow().semanticRetrieval().queue());

        assertThat(orchestrator.canWorkerCommit(lexicalCommand)).isTrue();

        WorkerCommand staleCommand = new WorkerCommand(
                lexicalCommand.commandId(),
                lexicalCommand.operationId(),
                lexicalCommand.searchRunId(),
                lexicalCommand.workflow(),
                lexicalCommand.workflowStepId(),
                lexicalCommand.workflowConfigVersion(),
                lexicalCommand.resultSnapshotId(),
                lexicalCommand.userId(),
                lexicalCommand.params(),
                lexicalCommand.attempt() + 1,
                lexicalCommand.createdAt());
        assertThat(orchestrator.canWorkerCommit(staleCommand)).isFalse();

        jdbcTemplate.update("update async_runs set status = 'superseded' where id = ?", ids.operationId());
        assertThat(orchestrator.canWorkerCommit(lexicalCommand)).isFalse();

        jdbcTemplate.update("update async_runs set status = 'cancelled' where id = ?", ids.operationId());
        assertThat(orchestrator.canWorkerCommit(lexicalCommand)).isFalse();
    }

    private HybridChunkSearchStartRequest searchRequest() {
        return new HybridChunkSearchStartRequest(
                "user_test",
                "digital wallet adoption",
                "Which markets show the strongest payment growth?",
                Map.of("sector", java.util.List.of("financials")),
                Map.of("field", "relevance_score", "direction", "desc"));
    }

    private WorkerCompletionEvent completed(SearchRunIds ids, String workflowStepId) {
        return new WorkerCompletionEvent(
                "evt_" + workflowStepId + "_" + ids.searchRunId(),
                ids.operationId(),
                ids.searchRunId(),
                workflowStepId,
                "completed",
                12,
                0,
                1,
                null,
                Instant.now());
    }

    private WorkerCompletionEvent failed(SearchRunIds ids, String workflowStepId, int attempt) {
        return new WorkerCompletionEvent(
                "evt_" + workflowStepId + "_" + ids.searchRunId(),
                ids.operationId(),
                ids.searchRunId(),
                workflowStepId,
                "failed",
                0,
                1,
                attempt,
                "worker failed",
                Instant.now());
    }

    private WorkerCommand receiveCommand(String queue) throws Exception {
        Message message = rabbitTemplate.receive(queue, 5000);
        assertThat(message).as("message in %s", queue).isNotNull();
        return objectMapper.readValue(message.getBody(), WorkerCommand.class);
    }

    private void assertNoCommand(String queue) {
        Message message = rabbitTemplate.receive(queue, 100);
        assertThat(message).as("unexpected message in %s", queue).isNull();
    }

    private void markStepCompleted(String operationId, String workflowStepId) {
        jdbcTemplate.update(
                """
                update async_run_steps
                set status = 'completed', completed_at = now(), updated_at = now()
                where async_run_id = ? and workflow_step_id = ?
                """,
                operationId,
                workflowStepId);
    }

    private void markStepFailed(String operationId, String workflowStepId, int attemptCount) {
        jdbcTemplate.update(
                """
                update async_run_steps
                set status = 'failed', attempt_count = ?, last_error = 'worker failed',
                  completed_at = now(), updated_at = now()
                where async_run_id = ? and workflow_step_id = ?
                """,
                attemptCount,
                operationId,
                workflowStepId);
    }

    private HybridChunkSearchWorkflow workflow() {
        return HybridChunkSearchWorkflow.defaultWorkflow(properties.workflow().hybridChunkSearchVersion());
    }

    private Integer stepAttemptCount(String operationId, String workflowStepId) {
        return jdbcTemplate.queryForObject(
                """
                select attempt_count
                from async_run_steps
                where async_run_id = ? and workflow_step_id = ?
                """,
                Integer.class,
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

    private String runStatus(String operationId) {
        return jdbcTemplate.queryForObject("select status from async_runs where id = ?", String.class, operationId);
    }

    private String snapshotStatus(String snapshotId) {
        return jdbcTemplate.queryForObject(
                "select status from result_snapshots where id = ?", String.class, snapshotId);
    }
}
