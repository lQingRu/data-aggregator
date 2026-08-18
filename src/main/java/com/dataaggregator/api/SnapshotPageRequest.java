package com.dataaggregator.api;

public record SnapshotPageRequest(Integer limit, Integer offset) {

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 500;

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
