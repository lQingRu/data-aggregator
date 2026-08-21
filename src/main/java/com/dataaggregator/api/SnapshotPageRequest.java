package com.dataaggregator.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record SnapshotPageRequest(@Min(1) @Max(MAX_LIMIT) Integer limit, @Min(0) Integer offset) {

    private static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 500;

    public int resolvedLimit() {
        if (limit == null) {
            return DEFAULT_LIMIT;
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("Page limit must be between 1 and " + MAX_LIMIT);
        }
        return limit;
    }

    public int resolvedOffset() {
        if (offset == null) {
            return 0;
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Page offset must be greater than or equal to 0");
        }
        return offset;
    }

    public static SnapshotPageRequest defaultPage() {
        return new SnapshotPageRequest(DEFAULT_LIMIT, 0);
    }
}
