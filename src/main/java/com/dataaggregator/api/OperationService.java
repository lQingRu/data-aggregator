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

    private static final TypeReference<List<OperationWarningResponse>> WARNING_LIST = new TypeReference<>() {};
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
    public SearchRequestCreateResponse createSearchRequest(String userId, SearchRequestCreateRequest request) {
        if (!HybridChunkSearchWorkflow.WORKFLOW_ID.equals(request.workflow())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Unsupported search workflow: " + request.workflow());
        }
        SearchRunIds ids = orchestrator.startHybridChunkSearch(new HybridChunkSearchStartRequest(
                userId,
                request.keywords(),
                request.question(),
                request.retrievalFilters().asMap(),
                request.initialSort().asMap()));

        return new SearchRequestCreateResponse(
                ids.searchRequestId(), ids.searchRunId(), ids.resultSnapshotId(), operation(ids.operationId(), userId));
    }

    @Transactional(readOnly = true)
    public OperationResponse operation(String operationId, String userId) {
        return loadOperation(operationId, userId);
    }

    @Transactional
    public OperationResponse cancelOperation(String operationId, String userId) {
        OperationResponse operation = loadOperation(operationId, userId);
        if (ACTIVE_STATUSES.contains(operation.status())) {
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
            if ("result_snapshot".equals(operation.scopeType())) {
                jdbcTemplate.update(
                        """
                        update result_snapshots
                        set status = 'cancelled'
                        where id = :snapshotId and user_id = :userId and status = 'pending'
                        """,
                        new MapSqlParameterSource("snapshotId", operation.scopeId()).addValue("userId", userId));
            }
        }
        eventPublisher.publishEvent(
                new AsyncRunChangedEvent(operationId, userId, operation.scopeType(), operation.scopeId()));
        return loadOperation(operationId, userId);
    }

    @Transactional(readOnly = true)
    public SnapshotActivityResponse snapshotActivity(String snapshotId, String userId) {
        ensureOwnedSnapshot(snapshotId, userId);
        return new SnapshotActivityResponse(
                "result_snapshot", snapshotId, operationsForScope("result_snapshot", snapshotId, userId));
    }

    @Transactional(readOnly = true)
    public List<OperationResponse> operationsForScope(String scopeType, String scopeId, String userId) {
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

    public Map<String, Object> ssePayload(OperationResponse operation) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("operation_id", operation.id());
        payload.put("type", operation.type());
        payload.put("status", operation.status());
        payload.put("scope_type", operation.scopeType());
        payload.put("scope_id", operation.scopeId());
        payload.put("current_step", operation.currentStep());
        payload.put("completed_steps", operation.completedSteps());
        payload.put("total_steps", operation.totalSteps());
        payload.put("completed_units", operation.completedUnits());
        payload.put("total_units", operation.totalUnits());
        payload.put("warning_count", operation.warnings().size());
        payload.put("changed_at", operation.updatedAt());
        return payload;
    }

    public String sseEventName(OperationResponse operation) {
        return switch (operation.status()) {
            case "completed", "completed_with_warnings" -> "async_run_completed";
            case "failed" -> "async_run_failed";
            case "cancelled" -> "async_run_cancelled";
            default -> "async_run_progressed";
        };
    }

    public boolean isSnapshotReady(OperationResponse operation) {
        return "result_snapshot".equals(operation.scopeType())
                && List.of("completed", "completed_with_warnings").contains(operation.status());
    }

    private OperationResponse loadOperation(String operationId, String userId) {
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

    private OperationResponse operationRow(ResultSet rs, int rowNum) throws SQLException {
        return new OperationResponse(
                rs.getString("id"),
                rs.getString("operation_type"),
                rs.getString("status"),
                rs.getString("scope_type"),
                rs.getString("scope_id"),
                rs.getString("current_step"),
                rs.getInt("completed_steps"),
                rs.getInt("total_steps"),
                rs.getObject("completed_units", Integer.class),
                rs.getObject("total_units", Integer.class),
                warnings(rs.getString("warnings_json")),
                instant(rs.getTimestamp("created_at")).toString(),
                instant(rs.getTimestamp("updated_at")).toString());
    }

    private List<OperationWarningResponse> warnings(String rawJson) {
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
