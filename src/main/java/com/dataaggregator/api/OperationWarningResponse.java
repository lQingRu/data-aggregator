package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OperationWarningResponse(@JsonProperty("workflow_step_id") String workflowStepId, String message) {}
