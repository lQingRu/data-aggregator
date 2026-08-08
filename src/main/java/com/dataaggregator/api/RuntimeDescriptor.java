package com.dataaggregator.api;

public record RuntimeDescriptor(String application, String runtimeMode, String mockUserId, int workflowConfigVersion) {}
