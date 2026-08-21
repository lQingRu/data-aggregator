package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.api.AsyncEventStreamService;
import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.support.IntegrationTestContainers;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OperationApiIT extends IntegrationTestContainers {

    @LocalServerPort
    private int port;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private DataAggregatorProperties properties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AsyncEventStreamService eventStreamService;

    @BeforeEach
    void resetState() {
        amqpAdmin.purgeQueue(properties.workflow().lexicalQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().semanticQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().relevanceScoreQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().snapshotProjectorQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().completionEventQueue(), true);

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            jdbcTemplate.update(
                    "delete from result_items where result_snapshot_id in ('snap_owned', 'snap_ready', 'snap_other')");
            jdbcTemplate.update("delete from result_snapshots where id in ('snap_owned', 'snap_ready', 'snap_other')");
            jdbcTemplate.update("delete from search_runs where id in ('run_owned', 'run_ready', 'run_other')");
            jdbcTemplate.update("delete from search_requests where id in ('sr_owned', 'sr_ready', 'sr_other')");
            jdbcTemplate.update("delete from async_runs where id in ('op_owned', 'op_ready', 'op_other')");
            insertOwnedOperation("owned", "user_alpha", "running", "pending");
            insertOwnedOperation("ready", "user_alpha", "completed", "ready");
            insertOwnedOperation("other", "user_beta", "running", "pending");
        });
    }

    @AfterEach
    void stopListenersBeforeContainersShutdown() {
        eventStreamService.closeAll();
        listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::stop);
    }

    @Test
    void searchRequestCreationReturnsDurableIdsAndOperationWithoutEventsUrl() {
        Map<String, Object> request = Map.of(
                "workflow",
                "hybrid_chunk_search",
                "keywords",
                "digital wallet adoption",
                "question",
                "Which markets show the strongest payment growth?",
                "retrieval_filters",
                Map.of("sector", List.of("financials"), "region", List.of("APAC", "Global")),
                "initial_sort",
                Map.of("field", "relevance_score", "direction", "desc"));

        ResponseEntity<Map> response = restTemplate.exchange(
                uri("/search-requests"),
                HttpMethod.POST,
                new HttpEntity<>(request, userHeaders("user_alpha")),
                Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.containsKey("search_request_id")).isTrue();
        assertThat(body.containsKey("search_run_id")).isTrue();
        assertThat(body.containsKey("result_snapshot_id")).isTrue();
        assertThat(body.containsKey("operation")).isTrue();
        assertThat(body.containsKey("events_url")).isFalse();
        Map<?, ?> operation = (Map<?, ?>) body.get("operation");
        assertThat(operation.get("id")).asString().startsWith("op_");
        assertThat(operation.get("type")).isEqualTo("search_run");
        assertThat(operation.get("status")).isIn("queued", "running");
        assertThat(operation.get("scope_type")).isEqualTo("result_snapshot");
        assertThat(operation.get("scope_id")).isEqualTo(body.get("result_snapshot_id"));
        assertThat(operation.containsKey("completed_steps")).isTrue();
        assertThat(operation.containsKey("total_steps")).isTrue();
        assertThat(operation.containsKey("warnings")).isTrue();
        assertThat(operation.containsKey("created_at")).isTrue();
        assertThat(operation.containsKey("updated_at")).isTrue();
    }

    @Test
    void operationLookupCancelAndSnapshotActivityReturnRefetchableState() {
        ResponseEntity<Map> lookup = restTemplate.exchange(
                uri("/operations/op_owned"), HttpMethod.GET, new HttpEntity<>(userHeaders("user_alpha")), Map.class);

        assertThat(lookup.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(lookup.getBody()).containsEntry("id", "op_owned");
        assertThat(lookup.getBody()).containsEntry("status", "running");
        assertThat((List<?>) lookup.getBody().get("warnings")).hasSize(1);

        ResponseEntity<Map> activity = restTemplate.exchange(
                uri("/result-snapshots/snap_owned/activity"),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_alpha")),
                Map.class);

        assertThat(activity.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(activity.getBody()).containsEntry("scope_type", "result_snapshot");
        assertThat(activity.getBody()).containsEntry("scope_id", "snap_owned");
        assertThat(operationIds(activity.getBody())).containsExactly("op_owned");

        ResponseEntity<Map> cancelled = restTemplate.exchange(
                uri("/operations/op_owned/cancel"),
                HttpMethod.POST,
                new HttpEntity<>(userHeaders("user_alpha")),
                Map.class);

        assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(cancelled.getBody()).containsEntry("id", "op_owned");
        assertThat(cancelled.getBody()).containsEntry("status", "cancelled");
    }

    @Test
    void sseEmitsNotificationOnlyProgressHintForOwnedSnapshot() {
        SseResponse response = readEvents("/events?scope_type=result_snapshot&scope_id=snap_owned", "user_alpha", 1);

        assertThat(response.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.contentType()).startsWith(MediaType.TEXT_EVENT_STREAM_VALUE);
        assertThat(response.body()).contains("event:async_run_progressed");
        assertThat(response.body()).contains("\"operation_id\":\"op_owned\"");
        assertThat(response.body()).contains("\"scope_id\":\"snap_owned\"");
        assertThat(response.body()).doesNotContain("rows");
        assertThat(response.body()).doesNotContain("chunk_text");

        SseResponse readyResponse =
                readEvents("/events?scope_type=result_snapshot&scope_id=snap_ready", "user_alpha", 2);
        assertThat(readyResponse.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(readyResponse.body()).contains("event:async_run_completed");
        assertThat(readyResponse.body()).contains("event:snapshot_ready");
        assertThat(readyResponse.body()).doesNotContain("rows");
        assertThat(readyResponse.body()).doesNotContain("chunk_text");
    }

    @Test
    void usersCannotAccessAnotherUsersOperationsSnapshotsOrEvents() {
        HttpHeaders intruder = userHeaders("user_alpha");
        ResponseEntity<Map> operation = restTemplate.exchange(
                uri("/operations/op_other"), HttpMethod.GET, new HttpEntity<>(intruder), Map.class);
        ResponseEntity<Map> snapshot = restTemplate.exchange(
                uri("/result-snapshots/snap_other"), HttpMethod.GET, new HttpEntity<>(intruder), Map.class);
        ResponseEntity<Map> schema = restTemplate.exchange(
                uri("/result-snapshots/snap_other/schema"), HttpMethod.GET, new HttpEntity<>(intruder), Map.class);
        ResponseEntity<Map> activity = restTemplate.exchange(
                uri("/result-snapshots/snap_other/activity"), HttpMethod.GET, new HttpEntity<>(intruder), Map.class);
        ResponseEntity<Map> query = restTemplate.exchange(
                uri("/result-snapshots/snap_other/query"),
                HttpMethod.POST,
                new HttpEntity<>(Map.of(), intruder),
                Map.class);
        HttpHeaders sseHeaders = userHeaders("user_alpha");
        sseHeaders.setAccept(List.of(MediaType.TEXT_EVENT_STREAM));
        ResponseEntity<String> events = restTemplate.exchange(
                uri("/events?scope_type=result_snapshot&scope_id=snap_other"),
                HttpMethod.GET,
                new HttpEntity<>(sseHeaders),
                String.class);

        assertThat(operation.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(snapshot.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(schema.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(activity.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(query.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(events.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpHeaders userHeaders(String userId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Mock-User-Id", userId);
        return headers;
    }

    private SseResponse readEvents(String path, String userId, int eventCount) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) uri(path).toURL().openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", MediaType.TEXT_EVENT_STREAM_VALUE);
            connection.setRequestProperty("X-Mock-User-Id", userId);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            int status = connection.getResponseCode();
            String contentType = connection.getContentType();
            StringBuilder body = new StringBuilder();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                int eventsRead = 0;
                while ((line = reader.readLine()) != null) {
                    body.append(line).append('\n');
                    if (line.isBlank() && body.toString().contains("data:")) {
                        eventsRead++;
                        if (eventsRead >= eventCount) {
                            break;
                        }
                    }
                }
            }
            return new SseResponse(status, contentType, body.toString());
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Could not read SSE response", exception);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private List<String> operationIds(Map<?, ?> activity) {
        List<String> ids = new ArrayList<>();
        for (Object operation : (List<?>) activity.get("operations")) {
            ids.add(((Map<?, ?>) operation).get("id").toString());
        }
        return ids;
    }

    private void insertOwnedOperation(String suffix, String userId, String operationStatus, String snapshotStatus) {
        jdbcTemplate.update(
                """
                insert into async_runs (
                  id, operation_type, status, scope_type, scope_id, user_id, current_step, completed_steps,
                  total_steps, warnings_json
                ) values (
                  ?, 'search_run', ?, 'result_snapshot', ?, ?, 'semantic_retrieval', 1, 4,
                  '[{"workflow_step_id":"semantic_retrieval","message":"optional worker retrying"}]'::jsonb
                )
                """,
                "op_" + suffix,
                operationStatus,
                "snap_" + suffix,
                userId);
        jdbcTemplate.update(
                """
                insert into search_requests (
                  id, user_id, workflow, keywords, question, retrieval_filters_json, initial_sort_json
                ) values (?, ?, 'hybrid_chunk_search', 'payments', 'Question?', '{}'::jsonb, '{}'::jsonb)
                """,
                "sr_" + suffix,
                userId);
        jdbcTemplate.update(
                """
                insert into search_runs (
                  id, async_run_id, search_request_id, workflow, workflow_config_version, result_snapshot_id
                ) values (?, ?, ?, 'hybrid_chunk_search', 1, ?)
                """,
                "run_" + suffix,
                "op_" + suffix,
                "sr_" + suffix,
                "snap_" + suffix);
        jdbcTemplate.update(
                """
                insert into result_snapshots (
                  id, search_run_id, user_id, status, schema_json, default_sort_json
                ) values (?, ?, ?, ?, '{"snapshot_id":"snap"}'::jsonb, '[]'::jsonb)
                """,
                "snap_" + suffix,
                "run_" + suffix,
                userId,
                snapshotStatus);
    }

    private record SseResponse(int status, String contentType, String body) {}
}
