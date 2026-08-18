package com.dataaggregator.api;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ResultSnapshotController {

    private final ResultSnapshotQueryService queryService;

    public ResultSnapshotController(ResultSnapshotQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/result-snapshots/{snapshotId}")
    public Map<String, Object> snapshotMetadata(@PathVariable String snapshotId) {
        return queryService.snapshotMetadata(snapshotId);
    }

    @GetMapping("/result-snapshots/{snapshotId}/schema")
    public Map<String, Object> snapshotSchema(@PathVariable String snapshotId) {
        return queryService.snapshotSchema(snapshotId);
    }

    @PostMapping("/result-snapshots/{snapshotId}/query")
    public SnapshotQueryResponse querySnapshot(
            @PathVariable String snapshotId, @RequestBody(required = false) SnapshotQueryRequest request) {
        return queryService.query(snapshotId, request == null ? SnapshotQueryRequest.empty() : request);
    }
}
