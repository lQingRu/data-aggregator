package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SnapshotActivityResponse(
        @JsonProperty("scope_type") String scopeType,
        @JsonProperty("scope_id") String scopeId,
        List<OperationResponse> operations) {}
