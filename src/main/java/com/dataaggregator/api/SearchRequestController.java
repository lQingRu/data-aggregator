package com.dataaggregator.api;

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
public class SearchRequestController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;

    public SearchRequestController(MockUserResolver userResolver, OperationService operationService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
    }

    @PostMapping("/search-requests")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SearchRequestCreateResponse createSearchRequest(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @Valid @RequestBody SearchRequestCreateRequest request) {
        return operationService.createSearchRequest(userResolver.userId(mockUserId), request);
    }
}
