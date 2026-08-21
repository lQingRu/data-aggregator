package com.dataaggregator.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record SnapshotSort(
        @NotBlank String field,
        @Pattern(regexp = "asc|desc") String direction,
        @Pattern(regexp = "first|last") String nulls) {}
