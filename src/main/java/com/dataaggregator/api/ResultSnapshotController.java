package com.dataaggregator.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
@Tag(name = "Result Snapshots")
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
    @Operation(summary = "Get Result Snapshot metadata")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Result Snapshot readiness metadata."),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Result Snapshot does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SnapshotMetadataResponse snapshotMetadata(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return queryService.snapshotMetadata(snapshotId, userResolver.userId(mockUserId));
    }

    @GetMapping("/result-snapshots/{snapshotId}/activity")
    @Operation(summary = "Get Result Snapshot activity")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Operations associated with a Result Snapshot."),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Result Snapshot does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SnapshotActivityResponse snapshotActivity(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return operationService.snapshotActivity(snapshotId, userResolver.userId(mockUserId));
    }

    @GetMapping("/result-snapshots/{snapshotId}/schema")
    @Operation(summary = "Get Result Snapshot schema")
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Fields available for filtering, sorting, grouping, and aggregation."),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Result Snapshot does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SnapshotSchemaResponse snapshotSchema(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId) {
        return queryService.snapshotSchema(snapshotId, userResolver.userId(mockUserId));
    }

    @PostMapping("/result-snapshots/{snapshotId}/query")
    @Operation(summary = "Query a ready Result Snapshot")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = false,
            content =
                    @Content(
                            schema = @Schema(implementation = SnapshotQueryRequest.class),
                            examples =
                                    @ExampleObject(
                                            name = "filteredGroupedQuery",
                                            value =
                                                    """
                                                    {
                                                      "filters": [
                                                        {
                                                          "field": "sector",
                                                          "op": "in",
                                                          "value": ["financials"]
                                                        }
                                                      ],
                                                      "sort": [
                                                        {
                                                          "field": "relevance_score",
                                                          "direction": "desc",
                                                          "nulls": "last"
                                                        }
                                                      ],
                                                      "group_by": ["region"],
                                                      "aggregations": [
                                                        {
                                                          "name": "result_count",
                                                          "op": "count"
                                                        },
                                                        {
                                                          "name": "avg_relevance",
                                                          "op": "avg",
                                                          "field": "relevance_score"
                                                        }
                                                      ],
                                                      "page": {
                                                        "limit": 10,
                                                        "offset": 0
                                                      }
                                                    }
                                                    """)))
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Query result rows, dynamic groups, and bounded page metadata."),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request body, field, operator, sort, group, aggregation, or page bounds.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Result Snapshot does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "409",
                description = "Result Snapshot is not ready.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SnapshotQueryResponse querySnapshot(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String snapshotId,
            @Valid @RequestBody(required = false) SnapshotQueryRequest request) {
        return queryService.query(
                snapshotId, userResolver.userId(mockUserId), request == null ? SnapshotQueryRequest.empty() : request);
    }
}
