package com.dataaggregator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ResultSnapshotQueryService {

    private static final TypeReference<Map<String, Object>> JSON_OBJECT = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> JSON_OBJECT_LIST = new TypeReference<>() {};
    private static final Pattern RESPONSE_NAME = Pattern.compile("[a-z][a-z0-9_]*");
    private static final Set<String> RESULT_ITEM_SCHEMA_COLUMNS = Set.of(
            "author",
            "company_name",
            "lexical_rank",
            "parent_title",
            "parent_type",
            "published_at",
            "region",
            "relevance_score",
            "sector",
            "source_name",
            "ticker");
    private static final String ROW_COLUMNS =
            """
            chunk_id, parent_entity_id, parent_title, parent_type, source_name, ticker, company_name,
            sector, region, published_at, author, chunk_text,
            relevance_score::double precision as relevance_score, lexical_rank, source_contributions_json::text
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ResultSnapshotQueryService(NamedParameterJdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> snapshotMetadata(String snapshotId, String userId) {
        SnapshotRecord snapshot = loadSnapshot(snapshotId, userId);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("snapshot_id", snapshot.id());
        metadata.put("search_run_id", snapshot.searchRunId());
        metadata.put("status", snapshot.status());
        metadata.put("created_at", snapshot.createdAt().toString());
        metadata.put(
                "ready_at",
                snapshot.readyAt() == null ? null : snapshot.readyAt().toString());
        return metadata;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> snapshotSchema(String snapshotId, String userId) {
        return loadSchema(loadSnapshot(snapshotId, userId));
    }

    @Transactional(readOnly = true)
    public SnapshotQueryResponse query(String snapshotId, String userId, SnapshotQueryRequest request) {
        SnapshotRecord snapshot = loadSnapshot(snapshotId, userId);
        if (!"ready".equals(snapshot.status())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Result snapshot is not ready: " + snapshotId);
        }
        SnapshotSchema schema = SnapshotSchema.from(loadSchema(snapshot));
        MapSqlParameterSource params = new MapSqlParameterSource("snapshotId", snapshotId);
        List<String> whereClauses = new ArrayList<>();
        whereClauses.add("result_snapshot_id = :snapshotId");
        appendFilters(request.filters(), schema, whereClauses, params);

        int limit = resolvedLimit(request.page());
        int offset = resolvedOffset(request.page());
        String whereSql = String.join(" and ", whereClauses);
        String orderSql = orderSql(request.sort().isEmpty() ? defaultSort(snapshot) : request.sort(), schema);

        int total = jdbcTemplate.queryForObject(
                "select count(*) from result_items where " + whereSql, params, Integer.class);
        params.addValue("limit", limit);
        params.addValue("offset", offset);
        List<Map<String, Object>> rows = jdbcTemplate.query(
                "select " + ROW_COLUMNS + " from result_items where " + whereSql + orderSql
                        + " limit :limit offset :offset",
                params,
                this::row);
        List<Map<String, Object>> groups = groups(request.groupBy(), request.aggregations(), schema, whereSql, params);

        return new SnapshotQueryResponse(snapshotId, rows, groups, new SnapshotPageResponse(limit, offset, total));
    }

    private void appendFilters(
            List<SnapshotFilter> filters,
            SnapshotSchema schema,
            List<String> whereClauses,
            MapSqlParameterSource params) {
        int index = 0;
        for (SnapshotFilter filter : filters) {
            SnapshotField field = schema.requireField(filter.field());
            if (!field.filterable()) {
                badRequest("Field is not filterable: " + filter.field());
            }
            String paramName = "filter" + index;
            whereClauses.add(filterSql(field, filter, paramName, params));
            index++;
        }
    }

    private String filterSql(
            SnapshotField field, SnapshotFilter filter, String paramName, MapSqlParameterSource params) {
        String op = normalized(filter.op());
        validateFilterOperator(field, op);
        return switch (op) {
            case "in" -> {
                if (!(filter.value() instanceof Collection<?> values) || values.isEmpty()) {
                    badRequest("The in operator requires a non-empty array value");
                }
                params.addValue(paramName, filter.value());
                yield field.column() + " in (:" + paramName + ")";
            }
            case "eq" -> {
                params.addValue(paramName, filter.value());
                yield field.column() + typedComparison(" = ", field, paramName);
            }
            case "gte" -> {
                params.addValue(paramName, filter.value());
                yield field.column() + typedComparison(" >= ", field, paramName);
            }
            case "lte" -> {
                params.addValue(paramName, filter.value());
                yield field.column() + typedComparison(" <= ", field, paramName);
            }
            case "gt" -> {
                params.addValue(paramName, filter.value());
                yield field.column() + typedComparison(" > ", field, paramName);
            }
            case "lt" -> {
                params.addValue(paramName, filter.value());
                yield field.column() + typedComparison(" < ", field, paramName);
            }
            default -> throw badRequest("Unsupported filter operator: " + filter.op());
        };
    }

    private void validateFilterOperator(SnapshotField field, String op) {
        if ("in".equals(op) || "eq".equals(op)) {
            return;
        }
        if (("number".equals(field.type()) || "datetime".equals(field.type()))
                && List.of("gt", "gte", "lt", "lte").contains(op)) {
            return;
        }
        badRequest("Unsupported filter operator for field " + field.name() + ": " + op);
    }

    private String typedComparison(String operator, SnapshotField field, String paramName) {
        if ("datetime".equals(field.type())) {
            return operator + "cast(:" + paramName + " as timestamptz)";
        }
        return operator + ":" + paramName;
    }

    private String orderSql(List<SnapshotSort> sorts, SnapshotSchema schema) {
        if (sorts.isEmpty()) {
            return " order by default_rank asc";
        }

        List<String> orderClauses = new ArrayList<>();
        for (SnapshotSort sort : sorts) {
            SnapshotField field = schema.requireField(sort.field());
            if (!field.sortable()) {
                badRequest("Field is not sortable: " + sort.field());
            }
            String direction = normalized(sort.direction() == null ? "asc" : sort.direction());
            if (!"asc".equals(direction) && !"desc".equals(direction)) {
                badRequest("Unsupported sort direction: " + sort.direction());
            }
            String nulls = normalized(sort.nulls() == null ? "last" : sort.nulls());
            if (!"first".equals(nulls) && !"last".equals(nulls)) {
                badRequest("Unsupported null sort behavior: " + sort.nulls());
            }
            orderClauses.add(field.column() + " " + direction + " nulls " + nulls);
        }
        orderClauses.add("default_rank asc");
        return " order by " + String.join(", ", orderClauses);
    }

    private List<Map<String, Object>> groups(
            List<String> groupBy,
            List<SnapshotAggregation> aggregations,
            SnapshotSchema schema,
            String whereSql,
            MapSqlParameterSource params) {
        if (groupBy.isEmpty() && aggregations.isEmpty()) {
            return List.of();
        }
        if (groupBy.isEmpty()) {
            badRequest("At least one group_by field is required when aggregations are requested");
        }

        List<SnapshotField> groupFields = new ArrayList<>();
        for (String groupField : groupBy) {
            SnapshotField field = schema.requireField(groupField);
            if (!field.groupable()) {
                badRequest("Field is not groupable: " + groupField);
            }
            groupFields.add(field);
        }

        List<AggregationProjection> projections = aggregationProjections(aggregations, schema);
        List<String> selectClauses = new ArrayList<>();
        for (SnapshotField groupField : groupFields) {
            selectClauses.add(groupField.column());
        }
        for (AggregationProjection projection : projections) {
            selectClauses.add(projection.sql());
        }

        String groupColumns = String.join(
                ", ", groupFields.stream().map(SnapshotField::column).toList());
        return jdbcTemplate.query(
                "select "
                        + String.join(", ", selectClauses)
                        + " from result_items where "
                        + whereSql
                        + " group by "
                        + groupColumns
                        + " order by "
                        + groupColumns,
                params,
                (rs, rowNum) -> groupRow(rs, groupFields, projections));
    }

    private List<AggregationProjection> aggregationProjections(
            List<SnapshotAggregation> aggregations, SnapshotSchema schema) {
        if (aggregations.isEmpty()) {
            return List.of(new AggregationProjection("result_count", "count(*)::integer as result_count"));
        }

        List<AggregationProjection> projections = new ArrayList<>();
        for (SnapshotAggregation aggregation : aggregations) {
            String name = aggregation.name();
            if (name == null || !RESPONSE_NAME.matcher(name).matches()) {
                badRequest("Aggregation name must be snake_case: " + name);
            }
            String op = normalized(aggregation.op());
            if ("count".equals(op)) {
                projections.add(new AggregationProjection(name, "count(*)::integer as " + name));
                continue;
            }
            if ("avg".equals(op)) {
                SnapshotField field = schema.requireField(aggregation.field());
                if (!field.aggregatable()) {
                    badRequest("Field is not aggregatable: " + aggregation.field());
                }
                projections.add(
                        new AggregationProjection(name, "avg(" + field.column() + ")::double precision as " + name));
                continue;
            }
            badRequest("Unsupported aggregation operator: " + aggregation.op());
        }
        return projections;
    }

    private Map<String, Object> row(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("chunk_id", rs.getString("chunk_id"));
        row.put("parent_entity_id", rs.getString("parent_entity_id"));
        row.put("parent_title", rs.getString("parent_title"));
        row.put("parent_type", rs.getString("parent_type"));
        row.put("source_name", rs.getString("source_name"));
        row.put("ticker", rs.getString("ticker"));
        row.put("company_name", rs.getString("company_name"));
        row.put("sector", rs.getString("sector"));
        row.put("region", rs.getString("region"));
        row.put("published_at", rs.getTimestamp("published_at").toInstant().toString());
        row.put("author", rs.getString("author"));
        row.put("chunk_text", rs.getString("chunk_text"));
        row.put("relevance_score", numericValue(rs, "relevance_score"));
        row.put("lexical_rank", rs.getObject("lexical_rank", Integer.class));
        row.put("source_contributions", sourceContributions(rs.getString("source_contributions_json")));
        return row;
    }

    private Map<String, Object> groupRow(
            ResultSet rs, List<SnapshotField> groupFields, List<AggregationProjection> projections)
            throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        Map<String, Object> key = new LinkedHashMap<>();
        for (SnapshotField groupField : groupFields) {
            key.put(groupField.name(), rs.getObject(groupField.column()));
        }
        row.put("key", key);
        for (AggregationProjection projection : projections) {
            row.put(projection.name(), numericValue(rs, projection.name()));
        }
        return row;
    }

    private Object numericValue(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        if (value instanceof BigDecimal decimal) {
            return decimal.doubleValue();
        }
        return value;
    }

    private List<String> sourceContributions(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, new TypeReference<>() {});
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not parse source contributions", exception);
        }
    }

    private List<SnapshotSort> defaultSort(SnapshotRecord snapshot) {
        List<Map<String, Object>> sortObjects = readJson(snapshot.defaultSortJson(), JSON_OBJECT_LIST);
        List<SnapshotSort> sorts = new ArrayList<>();
        for (Map<String, Object> sortObject : sortObjects) {
            sorts.add(new SnapshotSort(
                    stringValue(sortObject.get("field")),
                    stringValue(sortObject.get("direction")),
                    stringValue(sortObject.get("nulls"))));
        }
        return sorts;
    }

    private Map<String, Object> loadSchema(SnapshotRecord snapshot) {
        Map<String, Object> schema = readJson(snapshot.schemaJson(), JSON_OBJECT);
        if (!schema.containsKey("fields")) {
            badRequest("Snapshot schema is not available for snapshot: " + snapshot.id());
        }
        return schema;
    }

    private SnapshotRecord loadSnapshot(String snapshotId, String userId) {
        try {
            return jdbcTemplate.queryForObject(
                    """
                    select id, search_run_id, status, schema_json::text, default_sort_json::text, created_at, ready_at
                    from result_snapshots
                    where id = :snapshotId and user_id = :userId
                    """,
                    new MapSqlParameterSource("snapshotId", snapshotId).addValue("userId", userId),
                    (rs, rowNum) -> new SnapshotRecord(
                            rs.getString("id"),
                            rs.getString("search_run_id"),
                            rs.getString("status"),
                            rs.getString("schema_json"),
                            rs.getString("default_sort_json"),
                            rs.getTimestamp("created_at").toInstant(),
                            timestampInstant(rs.getTimestamp("ready_at"))));
        } catch (EmptyResultDataAccessException exception) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Unknown result snapshot: " + snapshotId, exception);
        }
    }

    private Instant timestampInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private int resolvedLimit(SnapshotPageRequest page) {
        try {
            return page.resolvedLimit();
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    private int resolvedOffset(SnapshotPageRequest page) {
        try {
            return page.resolvedOffset();
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    private <T> T readJson(String rawJson, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(rawJson, typeReference);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not parse stored snapshot JSON", exception);
        }
    }

    private String normalized(String value) {
        if (value == null || value.isBlank()) {
            badRequest("Required query value is missing");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private ResponseStatusException badRequest(String message) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }

    private record SnapshotRecord(
            String id,
            String searchRunId,
            String status,
            String schemaJson,
            String defaultSortJson,
            Instant createdAt,
            Instant readyAt) {}

    private record SnapshotSchema(Map<String, SnapshotField> fields) {

        private static SnapshotSchema from(Map<String, Object> schema) {
            Object fieldsValue = schema.get("fields");
            if (!(fieldsValue instanceof List<?> fieldObjects)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Snapshot schema fields must be an array");
            }

            Map<String, SnapshotField> fields = new LinkedHashMap<>();
            for (Object fieldValue : fieldObjects) {
                if (!(fieldValue instanceof Map<?, ?> fieldObject)) {
                    throw new ResponseStatusException(
                            HttpStatus.BAD_REQUEST, "Snapshot schema field must be an object");
                }
                SnapshotField field = SnapshotField.from(fieldObject);
                fields.put(field.name(), field);
            }
            return new SnapshotSchema(fields);
        }

        private SnapshotField requireField(String fieldName) {
            SnapshotField field = fields.get(fieldName);
            if (field == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported snapshot field: " + fieldName);
            }
            return field;
        }
    }

    private record SnapshotField(
            String name,
            String column,
            String type,
            boolean filterable,
            boolean sortable,
            boolean groupable,
            boolean aggregatable) {

        private static SnapshotField from(Map<?, ?> fieldObject) {
            String name = value(fieldObject, "name");
            if (!RESULT_ITEM_SCHEMA_COLUMNS.contains(name)) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported snapshot field: " + name);
            }
            return new SnapshotField(
                    name,
                    name,
                    value(fieldObject, "type"),
                    booleanValue(fieldObject, "filterable"),
                    booleanValue(fieldObject, "sortable"),
                    booleanValue(fieldObject, "groupable"),
                    booleanValue(fieldObject, "aggregatable"));
        }

        private static String value(Map<?, ?> fieldObject, String name) {
            Object value = fieldObject.get(name);
            if (value == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Snapshot schema field missing " + name);
            }
            return value.toString();
        }

        private static boolean booleanValue(Map<?, ?> fieldObject, String name) {
            Object value = fieldObject.get(name);
            if (value instanceof Boolean bool) {
                return bool;
            }
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Snapshot schema field missing boolean " + name);
        }
    }

    private record AggregationProjection(String name, String sql) {}
}
