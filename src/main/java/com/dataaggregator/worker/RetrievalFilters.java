package com.dataaggregator.worker;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

record RetrievalFilters(
        Set<String> sectors, Set<String> tickers, Set<String> regions, Instant publishedFrom, Instant publishedTo) {

    static RetrievalFilters from(Map<String, Object> rawFilters) {
        Object publishedAt = rawFilters.get("published_at");
        Map<?, ?> publishedAtFilter = publishedAt instanceof Map<?, ?> map ? map : Map.of();
        return new RetrievalFilters(
                stringSet(rawFilters.get("sector")),
                stringSet(rawFilters.get("ticker")),
                stringSet(rawFilters.get("region")),
                instantValue(publishedAtFilter.get("from")),
                instantValue(publishedAtFilter.get("to")));
    }

    boolean matches(InvestmentResearchChunk chunk) {
        return matchesValue(sectors, chunk.sector())
                && matchesValue(tickers, chunk.ticker())
                && matchesValue(regions, chunk.region())
                && (publishedFrom == null || !chunk.publishedAt().isBefore(publishedFrom))
                && (publishedTo == null || !chunk.publishedAt().isAfter(publishedTo));
    }

    private static boolean matchesValue(Set<String> allowedValues, String actualValue) {
        return allowedValues.isEmpty() || allowedValues.contains(actualValue);
    }

    private static Set<String> stringSet(Object value) {
        if (!(value instanceof Collection<?> rawValues)) {
            return Set.of();
        }
        LinkedHashSet<String> strings = new LinkedHashSet<>();
        for (Object rawValue : rawValues) {
            if (rawValue != null) {
                strings.add(rawValue.toString());
            }
        }
        return Set.copyOf(strings);
    }

    private static Instant instantValue(Object value) {
        return value == null ? null : Instant.parse(value.toString());
    }
}
