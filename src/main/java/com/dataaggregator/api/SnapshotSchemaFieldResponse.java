package com.dataaggregator.api;

public record SnapshotSchemaFieldResponse(
        String name,
        String type,
        boolean filterable,
        boolean sortable,
        boolean groupable,
        boolean aggregatable,
        boolean nullable) {}
