package com.dataaggregator.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.LinkedHashMap;
import java.util.Map;

public record SearchRequestInitialSort(
        @NotBlank @Pattern(regexp = "relevance_score|published_at|lexical_rank|ticker|company_name|sector|region")
                String field,
        @Pattern(regexp = "asc|desc") String direction,
        @Pattern(regexp = "first|last") String nulls) {

    public Map<String, Object> asMap() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("field", field);
        if (direction != null) {
            values.put("direction", direction);
        }
        if (nulls != null) {
            values.put("nulls", nulls);
        }
        return Map.copyOf(values);
    }
}
