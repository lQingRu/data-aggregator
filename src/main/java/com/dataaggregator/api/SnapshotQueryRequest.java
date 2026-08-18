package com.dataaggregator.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record SnapshotQueryRequest(
        List<SnapshotFilter> filters,
        List<SnapshotSort> sort,
        @JsonProperty("group_by") List<String> groupBy,
        List<SnapshotAggregation> aggregations,
        SnapshotPageRequest page) {

    public SnapshotQueryRequest {
        filters = filters == null ? List.of() : List.copyOf(filters);
        sort = sort == null ? List.of() : List.copyOf(sort);
        groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
        aggregations = aggregations == null ? List.of() : List.copyOf(aggregations);
        page = page == null ? SnapshotPageRequest.defaultPage() : page;
    }

    public static SnapshotQueryRequest empty() {
        return new SnapshotQueryRequest(List.of(), List.of(), List.of(), List.of(), SnapshotPageRequest.defaultPage());
    }
}
