package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SnapshotSchemaResponse(
        @JsonProperty("snapshot_id") String snapshotId,
        List<SnapshotSchemaFieldResponse> fields,
        @JsonProperty("default_sort") List<SnapshotSort> defaultSort) {}
