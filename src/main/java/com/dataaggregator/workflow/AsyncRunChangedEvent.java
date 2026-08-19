package com.dataaggregator.workflow;

public record AsyncRunChangedEvent(String operationId, String userId, String scopeType, String scopeId) {}
