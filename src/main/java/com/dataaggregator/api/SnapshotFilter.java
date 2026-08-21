package com.dataaggregator.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record SnapshotFilter(
        @NotBlank String field, @NotBlank @Pattern(regexp = "in|eq|gt|gte|lt|lte") String op, @NotNull Object value) {}
