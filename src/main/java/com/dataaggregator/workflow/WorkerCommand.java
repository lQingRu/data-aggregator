package com.dataaggregator.workflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;

public record WorkerCommand(
        @JsonProperty("command_id") String commandId,
        @JsonProperty("operation_id") String operationId,
        @JsonProperty("search_run_id") String searchRunId,
        String workflow,
        @JsonProperty("workflow_step_id") String workflowStepId,
        @JsonProperty("workflow_config_version") int workflowConfigVersion,
        @JsonProperty("result_snapshot_id") String resultSnapshotId,
        @JsonProperty("user_id") String userId,
        Map<String, Object> params,
        int attempt,
        @JsonProperty("created_at") Instant createdAt) {

    public WorkerCommand {
        params = Map.copyOf(params);
    }
}
