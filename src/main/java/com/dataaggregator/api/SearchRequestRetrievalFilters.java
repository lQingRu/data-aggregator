package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record SearchRequestRetrievalFilters(
        List<@NotBlank String> sector,
        List<@NotBlank String> ticker,
        List<@NotBlank String> region,
        @Valid @JsonProperty("published_at") SearchRequestTimeRange publishedAt) {

    public SearchRequestRetrievalFilters {
        sector = sector == null ? List.of() : List.copyOf(sector);
        ticker = ticker == null ? List.of() : List.copyOf(ticker);
        region = region == null ? List.of() : List.copyOf(region);
    }

    @AssertTrue(message = "At least one retrieval filter must be provided")
    public boolean hasFilter() {
        return !sector.isEmpty() || !ticker.isEmpty() || !region.isEmpty() || publishedAt != null;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> values = new LinkedHashMap<>();
        if (!sector.isEmpty()) {
            values.put("sector", sector);
        }
        if (!ticker.isEmpty()) {
            values.put("ticker", ticker);
        }
        if (!region.isEmpty()) {
            values.put("region", region);
        }
        if (publishedAt != null) {
            values.put("published_at", publishedAt.asMap());
        }
        return Map.copyOf(values);
    }
}
