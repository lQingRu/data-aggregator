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
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
@Tag(name = "Search Requests")
public class SearchRequestController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;

    public SearchRequestController(MockUserResolver userResolver, OperationService operationService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
    }

    @PostMapping("/search-requests")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(
            summary = "Create a Search Request",
            description =
                    "Starts a Hybrid Chunk Search workflow and returns durable IDs plus the initial Operation state.")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            content =
                    @Content(
                            schema = @Schema(implementation = SearchRequestCreateRequest.class),
                            examples =
                                    @ExampleObject(
                                            name = "hybridChunkSearch",
                                            value =
                                                    """
                                                    {
                                                      "workflow": "hybrid_chunk_search",
                                                      "keywords": "digital wallet adoption",
                                                      "question": "Which markets show the strongest payment growth?",
                                                      "retrieval_filters": {
                                                        "sector": ["financials"],
                                                        "region": ["APAC", "Global"]
                                                      },
                                                      "initial_sort": {
                                                        "field": "relevance_score",
                                                        "direction": "desc"
                                                      }
                                                    }
                                                    """)))
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Search Request accepted and durable async work created."),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid request body.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SearchRequestCreateResponse createSearchRequest(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @Valid @RequestBody SearchRequestCreateRequest request) {
        return operationService.createSearchRequest(userResolver.userId(mockUserId), request);
    }
}
