package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record SearchRequestCreateRequest(
        String workflow,
        String keywords,
        String question,
        @JsonProperty("retrieval_filters") Map<String, Object> retrievalFilters,
        @JsonProperty("initial_sort") Map<String, Object> initialSort) {

    public SearchRequestCreateRequest {
        retrievalFilters = retrievalFilters == null ? Map.of() : Map.copyOf(retrievalFilters);
        initialSort = initialSort == null ? Map.of() : Map.copyOf(initialSort);
    }
}
