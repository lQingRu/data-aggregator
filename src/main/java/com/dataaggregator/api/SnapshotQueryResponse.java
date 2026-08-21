package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record SnapshotQueryResponse(
        @JsonProperty("snapshot_id") String snapshotId,
        List<SnapshotResultRow> rows,
        List<Map<String, Object>> groups,
        SnapshotPageResponse page) {}
