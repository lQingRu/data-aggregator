package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record OperationResponse(
        String id,
        String type,
        String status,
        @JsonProperty("scope_type") String scopeType,
        @JsonProperty("scope_id") String scopeId,
        @JsonProperty("current_step") String currentStep,
        @JsonProperty("completed_steps") int completedSteps,
        @JsonProperty("total_steps") int totalSteps,
        @JsonProperty("completed_units") Integer completedUnits,
        @JsonProperty("total_units") Integer totalUnits,
        List<OperationWarningResponse> warnings,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("updated_at") String updatedAt) {}
