package com.dataaggregator.api;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
public class SearchRequestController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;

    public SearchRequestController(MockUserResolver userResolver, OperationService operationService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
    }

    @PostMapping("/search-requests")
    public Map<String, Object> createSearchRequest(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @RequestBody SearchRequestCreateRequest request) {
        return operationService.createSearchRequest(userResolver.userId(mockUserId), request);
    }
}
