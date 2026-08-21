package com.dataaggregator.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record SnapshotAggregation(
        @NotBlank @Pattern(regexp = "[a-z][a-z0-9_]*") String name,
        @NotBlank @Pattern(regexp = "count|avg") String op,
        String field) {}
