package com.dataaggregator.workflow;

import java.util.List;

public record WorkflowStepDefinition(
        String id, String queue, boolean required, boolean enabled, int maxAttempts, List<String> dependsOn) {

    public WorkflowStepDefinition {
        dependsOn = List.copyOf(dependsOn);
    }
}
