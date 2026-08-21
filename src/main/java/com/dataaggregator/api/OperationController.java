package com.dataaggregator.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
@Tag(name = "Operations")
public class OperationController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;

    public OperationController(MockUserResolver userResolver, OperationService operationService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
    }

    @GetMapping("/operations/{operationId}")
    @Operation(summary = "Get an Operation")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Current Operation state."),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Operation does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public OperationResponse operation(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String operationId) {
        return operationService.operation(operationId, userResolver.userId(mockUserId));
    }

    @PostMapping("/operations/{operationId}/cancel")
    @Operation(summary = "Cancel an Operation")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Operation state after cancellation request."),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Operation does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public OperationResponse cancelOperation(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String operationId) {
        return operationService.cancelOperation(operationId, userResolver.userId(mockUserId));
    }
}
