package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SearchRequestCreateResponse(
        @JsonProperty("search_request_id") String searchRequestId,
        @JsonProperty("search_run_id") String searchRunId,
        @JsonProperty("result_snapshot_id") String resultSnapshotId,
        OperationResponse operation) {}
