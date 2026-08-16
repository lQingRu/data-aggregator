package com.dataaggregator.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record WorkerCompletionEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("operation_id") String operationId,
        @JsonProperty("search_run_id") String searchRunId,
        @JsonProperty("workflow_step_id") String workflowStepId,
        String status,
        @JsonProperty("contribution_count") int contributionCount,
        @JsonProperty("warning_count") int warningCount,
        int attempt,
        @JsonProperty("error_message") String errorMessage,
        @JsonProperty("occurred_at") Instant occurredAt) {}
