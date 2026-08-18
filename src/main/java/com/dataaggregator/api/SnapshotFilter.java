package com.dataaggregator.api;

public record SnapshotFilter(String field, String op, Object value) {}
