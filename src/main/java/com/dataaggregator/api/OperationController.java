package com.dataaggregator.api;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("!worker")
public class OperationController {

    private final MockUserResolver userResolver;
    private final OperationService operationService;

    public OperationController(MockUserResolver userResolver, OperationService operationService) {
        this.userResolver = userResolver;
        this.operationService = operationService;
    }

    @GetMapping("/operations/{operationId}")
    public OperationResponse operation(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String operationId) {
        return operationService.operation(operationId, userResolver.userId(mockUserId));
    }

    @PostMapping("/operations/{operationId}/cancel")
    public OperationResponse cancelOperation(
            @RequestHeader(name = MockUserResolver.HEADER, required = false) String mockUserId,
            @PathVariable String operationId) {
        return operationService.cancelOperation(operationId, userResolver.userId(mockUserId));
    }
}
