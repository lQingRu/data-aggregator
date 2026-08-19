package com.dataaggregator.api;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
public class ResultSnapshotController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;
    private final ResultSnapshotQueryService queryService;

    public ResultSnapshotController(
            MockUserResolver userResolver, OperationService operationService, ResultSnapshotQueryService queryService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
        this.queryService = queryService;
    }

    @GetMapping("/result-snapshots/{snapshotId}")
    public Map<String, Object> snapshotMetadata(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return queryService.snapshotMetadata(snapshotId, userResolver.userId(mockUserId));
    }

    @GetMapping("/result-snapshots/{snapshotId}/activity")
    public Map<String, Object> snapshotActivity(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return operationService.snapshotActivity(snapshotId, userResolver.userId(mockUserId));
    }

    @GetMapping("/result-snapshots/{snapshotId}/schema")
    public Map<String, Object> snapshotSchema(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return queryService.snapshotSchema(snapshotId, userResolver.userId(mockUserId));
    }

    @PostMapping("/result-snapshots/{snapshotId}/query")
    public SnapshotQueryResponse querySnapshot(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId,
            @RequestBody(required = false) SnapshotQueryRequest request) {
        return queryService.query(
                snapshotId, userResolver.userId(mockUserId), request == null ? SnapshotQueryRequest.empty() : request);
    }
}
