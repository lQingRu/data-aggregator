package com.dataaggregator.api;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Pattern;
import java.util.LinkedHashMap;
import java.util.Map;

public record SearchRequestTimeRange(
        @Pattern(regexp = "\\d{4}-\\d{2}-\\d{2}T.*Z") String from,
        @Pattern(regexp = "\\d{4}-\\d{2}-\\d{2}T.*Z") String to) {

    @AssertTrue(message = "At least one published_at bound must be provided")
    public boolean hasBound() {
        return from != null || to != null;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> values = new LinkedHashMap<>();
        if (from != null) {
            values.put("from", from);
        }
        if (to != null) {
            values.put("to", to);
        }
        return Map.copyOf(values);
    }
}
