package com.dataaggregator.api;

import com.dataaggregator.workflow.AsyncRunChangedEvent;
import com.dataaggregator.workflow.HybridChunkSearchStartRequest;
import com.dataaggregator.workflow.HybridChunkSearchWorkflow;
import com.dataaggregator.workflow.SearchRunIds;
import com.dataaggregator.workflow.WorkflowOrchestrator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@Profile("!worker")
public class OperationService {

    private static final TypeReference<List<Map<String, Object>>> WARNING_LIST = new TypeReference<>() {};
    private static final List<String> ACTIVE_STATUSES = List.of("queued", "running", "waiting_retry");

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final WorkflowOrchestrator orchestrator;

    public OperationService(
            NamedParameterJdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            ApplicationEventPublisher eventPublisher,
            WorkflowOrchestrator orchestrator) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.orchestrator = orchestrator;
    }

    @Transactional
    public Map<String, Object> createSearchRequest(String userId, SearchRequestCreateRequest request) {
        if (!HybridChunkSearchWorkflow.WORKFLOW_ID.equals(request.workflow())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Unsupported search workflow: " + request.workflow());
        }
        if (request.keywords() == null || request.keywords().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search request keywords are required");
        }
        if (request.question() == null || request.question().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search request question is required");
        }
        if (request.retrievalFilters().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search request retrieval_filters are required");
        }
        if (request.initialSort().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Search request initial_sort is required");
        }
        SearchRunIds ids = orchestrator.startHybridChunkSearch(new HybridChunkSearchStartRequest(
                userId, request.keywords(), request.question(), request.retrievalFilters(), request.initialSort()));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("search_request_id", ids.searchRequestId());
        response.put("search_run_id", ids.searchRunId());
        response.put("result_snapshot_id", ids.resultSnapshotId());
        response.put("operation", operation(ids.operationId(), userId));
        return response;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> operation(String operationId, String userId) {
        return loadOperation(operationId, userId);
    }

    @Transactional
    public Map<String, Object> cancelOperation(String operationId, String userId) {
        Map<String, Object> operation = loadOperation(operationId, userId);
        if (ACTIVE_STATUSES.contains(operation.get("status"))) {
            jdbcTemplate.update(
                    """
                    update async_runs
                    set status = 'cancelled', current_step = null, updated_at = now()
                    where id = :operationId and user_id = :userId
                    """,
                    params(operationId, userId));
            jdbcTemplate.update(
                    """
                    update async_run_steps
                    set status = 'cancelled', completed_at = coalesce(completed_at, now()), updated_at = now()
                    where async_run_id = :operationId and status in ('queued', 'running', 'waiting_retry')
                    """,
                    params(operationId, userId));
            if ("result_snapshot".equals(operation.get("scope_type"))) {
                jdbcTemplate.update(
                        """
                        update result_snapshots
                        set status = 'cancelled'
                        where id = :snapshotId and user_id = :userId and status = 'pending'
                        """,
                        new MapSqlParameterSource("snapshotId", operation.get("scope_id")).addValue("userId", userId));
            }
        }
        eventPublisher.publishEvent(new AsyncRunChangedEvent(
                operationId,
                userId,
                operation.get("scope_type").toString(),
                operation.get("scope_id").toString()));
        return loadOperation(operationId, userId);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> snapshotActivity(String snapshotId, String userId) {
        ensureOwnedSnapshot(snapshotId, userId);
        Map<String, Object> activity = new LinkedHashMap<>();
        activity.put("scope_type", "result_snapshot");
        activity.put("scope_id", snapshotId);
        activity.put("operations", operationsForScope("result_snapshot", snapshotId, userId));
        return activity;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> operationsForScope(String scopeType, String scopeId, String userId) {
        if (!"result_snapshot".equals(scopeType)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported event scope type: " + scopeType);
        }
        ensureOwnedSnapshot(scopeId, userId);
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("scopeType", scopeType)
                .addValue("scopeId", scopeId)
                .addValue("userId", userId);
        return jdbcTemplate.query(
                """
                select id, operation_type, status, scope_type, scope_id, current_step, completed_steps, total_steps,
                  completed_units, total_units, warnings_json::text, created_at, updated_at
                from async_runs
                where scope_type = :scopeType and scope_id = :scopeId and user_id = :userId
                order by created_at desc
                """,
                params,
                this::operationRow);
    }

    public Map<String, Object> ssePayload(Map<String, Object> operation) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("operation_id", operation.get("id"));
        payload.put("type", operation.get("type"));
        payload.put("status", operation.get("status"));
        payload.put("scope_type", operation.get("scope_type"));
        payload.put("scope_id", operation.get("scope_id"));
        payload.put("current_step", operation.get("current_step"));
        payload.put("completed_steps", operation.get("completed_steps"));
        payload.put("total_steps", operation.get("total_steps"));
        payload.put("completed_units", operation.get("completed_units"));
        payload.put("total_units", operation.get("total_units"));
        payload.put("warning_count", ((List<?>) operation.get("warnings")).size());
        payload.put("changed_at", operation.get("updated_at"));
        return payload;
    }

    public String sseEventName(Map<String, Object> operation) {
        return switch (operation.get("status").toString()) {
            case "completed", "completed_with_warnings" -> "async_run_completed";
            case "failed" -> "async_run_failed";
            case "cancelled" -> "async_run_cancelled";
            default -> "async_run_progressed";
        };
    }

    public boolean isSnapshotReady(Map<String, Object> operation) {
        return "result_snapshot".equals(operation.get("scope_type"))
                && List.of("completed", "completed_with_warnings").contains(operation.get("status"));
    }

    private Map<String, Object> loadOperation(String operationId, String userId) {
        try {
            return jdbcTemplate.queryForObject(
                    """
                    select id, operation_type, status, scope_type, scope_id, current_step, completed_steps, total_steps,
                      completed_units, total_units, warnings_json::text, created_at, updated_at
                    from async_runs
                    where id = :operationId and user_id = :userId
                    """,
                    params(operationId, userId),
                    this::operationRow);
        } catch (EmptyResultDataAccessException exception) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown operation: " + operationId, exception);
        }
    }

    private Map<String, Object> operationRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> operation = new LinkedHashMap<>();
        operation.put("id", rs.getString("id"));
        operation.put("type", rs.getString("operation_type"));
        operation.put("status", rs.getString("status"));
        operation.put("scope_type", rs.getString("scope_type"));
        operation.put("scope_id", rs.getString("scope_id"));
        operation.put("current_step", rs.getString("current_step"));
        operation.put("completed_steps", rs.getInt("completed_steps"));
        operation.put("total_steps", rs.getInt("total_steps"));
        operation.put("completed_units", rs.getObject("completed_units", Integer.class));
        operation.put("total_units", rs.getObject("total_units", Integer.class));
        operation.put("warnings", warnings(rs.getString("warnings_json")));
        operation.put("created_at", instant(rs.getTimestamp("created_at")).toString());
        operation.put("updated_at", instant(rs.getTimestamp("updated_at")).toString());
        return operation;
    }

    private List<Map<String, Object>> warnings(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, WARNING_LIST);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not parse operation warnings", exception);
        }
    }

    private void ensureOwnedSnapshot(String snapshotId, String userId) {
        Boolean exists = jdbcTemplate.queryForObject(
                """
                select exists (
                  select 1 from result_snapshots where id = :snapshotId and user_id = :userId
                )
                """,
                new MapSqlParameterSource("snapshotId", snapshotId).addValue("userId", userId),
                Boolean.class);
        if (!Boolean.TRUE.equals(exists)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown result snapshot: " + snapshotId);
        }
    }

    private MapSqlParameterSource params(String operationId, String userId) {
        return new MapSqlParameterSource("operationId", operationId).addValue("userId", userId);
    }

    private Instant instant(Timestamp timestamp) {
        return timestamp.toInstant();
    }
}
