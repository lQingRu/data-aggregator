package com.dataaggregator.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@Profile("!worker")
@Tag(name = "Events")
public class AsyncEventsController {

    private final AsyncEventStreamService eventStreamService;
    private final MockUserResolver userResolver;

    public AsyncEventsController(AsyncEventStreamService eventStreamService, MockUserResolver userResolver) {
        this.eventStreamService = eventStreamService;
        this.userResolver = userResolver;
    }

    @GetMapping(path = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Subscribe to async Operation notifications")
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                description = "Server-sent events stream of lightweight Operation invalidation hints.",
                content =
                        @Content(
                                mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                                schema = @Schema(type = "string"),
                                examples =
                                        @ExampleObject(
                                                name = "progress",
                                                value =
                                                        """
                                                        event: async_run_progressed
                                                        data: {"operation_id":"op_123","type":"search_run","status":"running","scope_type":"result_snapshot","scope_id":"snap_123","warning_count":0}
                                                        """))),
        @ApiResponse(
                responseCode = "400",
                description = "Invalid scope parameters.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "404",
                description = "The requested Result Snapshot event scope does not exist for the authenticated user.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))),
        @ApiResponse(
                responseCode = "500",
                description = "Unexpected server failure.",
                content = @Content(schema = @Schema(implementation = ApiErrorResponse.class)))
    })
    public SseEmitter events(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @RequestParam("scope_type") String scopeType,
            @RequestParam("scope_id") String scopeId)
            throws IOException {
        return eventStreamService.subscribe(scopeType, scopeId, userResolver.userId(mockUserId));
    }
}
