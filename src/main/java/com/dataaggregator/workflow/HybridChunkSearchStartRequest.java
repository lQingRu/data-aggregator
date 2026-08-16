package com.dataaggregator.workflow;

import java.util.Map;

public record HybridChunkSearchStartRequest(
        String userId,
        String keywords,
        String question,
        Map<String, Object> retrievalFilters,
        Map<String, Object> initialSort) {

    public HybridChunkSearchStartRequest {
        retrievalFilters = Map.copyOf(retrievalFilters);
        initialSort = Map.copyOf(initialSort);
    }
}
