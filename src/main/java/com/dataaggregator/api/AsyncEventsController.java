package com.dataaggregator.api;

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
public class AsyncEventsController {

    private final AsyncEventStreamService eventStreamService;
    private final MockUserResolver userResolver;

    public AsyncEventsController(AsyncEventStreamService eventStreamService, MockUserResolver userResolver) {
        this.eventStreamService = eventStreamService;
        this.userResolver = userResolver;
    }

    @GetMapping(path = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @RequestParam("scope_type") String scopeType,
            @RequestParam("scope_id") String scopeId)
            throws IOException {
        return eventStreamService.subscribe(scopeType, scopeId, userResolver.userId(mockUserId));
    }
}
