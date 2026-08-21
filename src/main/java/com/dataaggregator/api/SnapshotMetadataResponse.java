package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SnapshotMetadataResponse(
        @JsonProperty("snapshot_id") String snapshotId,
        @JsonProperty("search_run_id") String searchRunId,
        String status,
        @JsonProperty("created_at") String createdAt,
        @JsonProperty("ready_at") String readyAt) {}
