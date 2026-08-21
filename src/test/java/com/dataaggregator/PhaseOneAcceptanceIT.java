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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PhaseOneAcceptanceIT extends IntegrationTestContainers {

    @LocalServerPort
    private int port;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private AsyncEventStreamService eventStreamService;

    @Autowired
    private DataAggregatorProperties properties;

    @Autowired
    private Environment environment;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private TestRestTemplate restTemplate;

    private ConfigurableApplicationContext workerContext;

    @BeforeEach
    void purgeQueues() {
        listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::start);
        amqpAdmin.purgeQueue(properties.workflow().lexicalQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().semanticQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().relevanceScoreQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().snapshotProjectorQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().completionEventQueue(), true);
    }

    @AfterEach
    void closeWorkerContextAndSseStreams() {
        if (workerContext != null) {
            workerContext.close();
        }
        eventStreamService.closeAll();
        listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::stop);
    }

    @Test
    void searchRequestCompletesThroughWorkersAndMaterializesQueryableSnapshot() {
        Map<String, Object> createRequest = Map.of(
                "workflow",
                "hybrid_chunk_search",
                "keywords",
                "digital wallet adoption",
                "question",
                "Which markets show the strongest payment growth?",
                "retrieval_filters",
                Map.of(
                        "sector",
                        List.of("financials"),
                        "ticker",
                        List.of("V", "MA"),
                        "region",
                        List.of("APAC", "Global"),
                        "published_at",
                        Map.of("from", "2025-01-01T00:00:00Z", "to", "2026-12-31T23:59:59Z")),
                "initial_sort",
                Map.of("field", "relevance_score", "direction", "desc"));

        ResponseEntity<Map> created = restTemplate.exchange(
                uri("/search-requests"),
                HttpMethod.POST,
                new HttpEntity<>(createRequest, userHeaders("user_acceptance")),
                Map.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        Map<?, ?> createdBody = created.getBody();
        assertThat(createdBody).isNotNull();
        Map<?, ?> operation = (Map<?, ?>) createdBody.get("operation");
        String operationId = operation.get("id").toString();
        String searchRunId = createdBody.get("search_run_id").toString();
        String snapshotId = createdBody.get("result_snapshot_id").toString();

        assertThat(countRows("async_runs", "id", operationId)).isEqualTo(1);
        assertThat(countRows("search_runs", "id", searchRunId)).isEqualTo(1);
        assertThat(countRows("result_snapshots", "id", snapshotId)).isEqualTo(1);
        assertThat(countRows("result_items", "result_snapshot_id", snapshotId)).isZero();
        assertThat(queueDepth(properties.workflow().lexicalQueue())).isEqualTo(1);
        assertThat(queueDepth(properties.workflow().semanticQueue())).isEqualTo(1);

        SseReader sseReader = openSse("/events?scope_type=result_snapshot&scope_id=" + snapshotId, "user_acceptance");
        String initialEvent = sseReader.readEvent();
        assertThat(initialEvent).contains("event:async_run_progressed");
        assertThat(initialEvent).contains("\"operation_id\":\"" + operationId + "\"");
        assertThat(initialEvent).doesNotContain("rows");
        assertThat(initialEvent).doesNotContain("chunk_text");

        startWorkerProcess();

        Map<?, ?> completedOperation = waitForCompletedOperation(operationId, "user_acceptance");
        assertThat(completedOperation.get("status")).isEqualTo("completed");
        assertThat(completedOperation.get("completed_steps")).isEqualTo(4);
        assertThat(completedOperation.get("total_steps")).isEqualTo(4);

        String completedEvents = sseReader.readEventsUntil("event:snapshot_ready");
        assertThat(completedEvents).contains("event:async_run_completed");
        assertThat(completedEvents).contains("event:snapshot_ready");
        assertThat(completedEvents).doesNotContain("rows");
        assertThat(completedEvents).doesNotContain("chunk_text");
        sseReader.close();
        closeWorkerProcess();

        ResponseEntity<Map> metadata = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_acceptance")),
                Map.class);
        assertThat(metadata.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(metadata.getBody()).containsEntry("status", "ready");

        ResponseEntity<Map> activity = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/activity"),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_acceptance")),
                Map.class);
        assertThat(activity.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(operationIds(activity.getBody())).containsExactly(operationId);

        ResponseEntity<Map> schema = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/schema"),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_acceptance")),
                Map.class);
        assertThat(schema.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fieldNames(schema.getBody())).contains("sector", "ticker", "region", "relevance_score");

        Map<String, Long> queueDepthsBeforeQuery = workflowQueueDepths();
        Map<String, Object> queryRequest = Map.of(
                "filters",
                List.of(Map.of("field", "sector", "op", "in", "value", List.of("financials"))),
                "sort",
                List.of(Map.of("field", "relevance_score", "direction", "desc", "nulls", "last")),
                "group_by",
                List.of("region"),
                "aggregations",
                List.of(
                        Map.of("name", "result_count", "op", "count"),
                        Map.of("name", "avg_relevance", "op", "avg", "field", "relevance_score")),
                "page",
                Map.of("limit", 2, "offset", 0));
        ResponseEntity<Map> query = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/query"),
                HttpMethod.POST,
                new HttpEntity<>(queryRequest, userHeaders("user_acceptance")),
                Map.class);

        assertThat(query.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(query.getBody()).containsEntry("snapshot_id", snapshotId);
        assertQueryableRows(query.getBody());
        assertQueryableGroups(query.getBody());
        assertThat(workflowQueueDepths()).isEqualTo(queueDepthsBeforeQuery);

        Map<String, Object> unsupportedQuery =
                Map.of("filters", List.of(Map.of("field", "market_cap", "op", "in", "value", List.of("large"))));
        ResponseEntity<Map> rejectedQuery = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/query"),
                HttpMethod.POST,
                new HttpEntity<>(unsupportedQuery, userHeaders("user_acceptance")),
                Map.class);
        assertThat(rejectedQuery.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(workflowQueueDepths()).isEqualTo(queueDepthsBeforeQuery);
    }

    @Test
    void authorizationPreventsCrossUserSnapshotAccessAfterWorkflowCompletion() {
        Map<String, Object> createRequest = Map.of(
                "workflow",
                "hybrid_chunk_search",
                "keywords",
                "digital wallet adoption",
                "question",
                "Which markets show the strongest payment growth?",
                "retrieval_filters",
                Map.of("sector", List.of("financials")),
                "initial_sort",
                Map.of("field", "relevance_score", "direction", "desc"));

        ResponseEntity<Map> created = restTemplate.exchange(
                uri("/search-requests"),
                HttpMethod.POST,
                new HttpEntity<>(createRequest, userHeaders("user_owner")),
                Map.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        Map<?, ?> createdBody = created.getBody();
        Map<?, ?> operation = (Map<?, ?>) createdBody.get("operation");
        String operationId = operation.get("id").toString();
        String snapshotId = createdBody.get("result_snapshot_id").toString();

        startWorkerProcess();
        waitForCompletedOperation(operationId, "user_owner");

        ResponseEntity<Map> intruderOperation = restTemplate.exchange(
                uri("/operations/" + operationId),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_intruder")),
                Map.class);
        ResponseEntity<Map> intruderMetadata = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_intruder")),
                Map.class);
        ResponseEntity<Map> intruderSchema = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/schema"),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_intruder")),
                Map.class);
        ResponseEntity<Map> intruderActivity = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/activity"),
                HttpMethod.GET,
                new HttpEntity<>(userHeaders("user_intruder")),
                Map.class);
        ResponseEntity<Map> intruderQuery = restTemplate.exchange(
                uri("/result-snapshots/" + snapshotId + "/query"),
                HttpMethod.POST,
                new HttpEntity<>(Map.of(), userHeaders("user_intruder")),
                Map.class);
        HttpHeaders eventHeaders = userHeaders("user_intruder");
        eventHeaders.setAccept(List.of(MediaType.TEXT_EVENT_STREAM));
        ResponseEntity<String> intruderEvents = restTemplate.exchange(
                uri("/events?scope_type=result_snapshot&scope_id=" + snapshotId),
                HttpMethod.GET,
                new HttpEntity<>(eventHeaders),
                String.class);

        assertThat(intruderOperation.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(intruderMetadata.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(intruderSchema.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(intruderActivity.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(intruderQuery.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(intruderEvents.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private void startWorkerProcess() {
        if (workerContext != null) {
            return;
        }
        workerContext = new SpringApplicationBuilder(DataAggregatorApplication.class)
                .profiles("worker")
                .run(
                        "--spring.datasource.url=" + requiredProperty("spring.datasource.url"),
                        "--spring.datasource.username=" + requiredProperty("spring.datasource.username"),
                        "--spring.datasource.password=" + requiredProperty("spring.datasource.password"),
                        "--spring.rabbitmq.host=" + requiredProperty("spring.rabbitmq.host"),
                        "--spring.rabbitmq.port=" + requiredProperty("spring.rabbitmq.port"),
                        "--spring.rabbitmq.username=" + requiredProperty("spring.rabbitmq.username"),
                        "--spring.rabbitmq.password=" + requiredProperty("spring.rabbitmq.password"));
    }

    private void closeWorkerProcess() {
        if (workerContext == null) {
            return;
        }
        workerContext.close();
        workerContext = null;
    }

    private Map<?, ?> waitForCompletedOperation(String operationId, String userId) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        AssertionError lastError = null;
        while (Instant.now().isBefore(deadline)) {
            ResponseEntity<Map> response = restTemplate.exchange(
                    uri("/operations/" + operationId),
                    HttpMethod.GET,
                    new HttpEntity<>(userHeaders(userId)),
                    Map.class);
            try {
                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(response.getBody()).isNotNull();
                assertThat(response.getBody().get("status")).isEqualTo("completed");
                return response.getBody();
            } catch (AssertionError error) {
                lastError = error;
                sleep();
            }
        }
        throw lastError == null ? new AssertionError("Operation did not complete") : lastError;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpHeaders userHeaders(String userId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Mock-User-Id", userId);
        return headers;
    }

    private SseReader openSse(String path, String userId) {
        try {
            HttpURLConnection connection = (HttpURLConnection) uri(path).toURL().openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", MediaType.TEXT_EVENT_STREAM_VALUE);
            connection.setRequestProperty("X-Mock-User-Id", userId);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(20000);
            assertThat(connection.getResponseCode()).isEqualTo(HttpStatus.OK.value());
            assertThat(connection.getContentType()).startsWith(MediaType.TEXT_EVENT_STREAM_VALUE);
            return new SseReader(connection);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Could not open SSE response", exception);
        }
    }

    private long queueDepth(String queue) {
        Long count = rabbitTemplate.execute(channel -> channel.messageCount(queue));
        return count == null ? 0 : count;
    }

    private Map<String, Long> workflowQueueDepths() {
        return Map.of(
                properties.workflow().lexicalQueue(),
                queueDepth(properties.workflow().lexicalQueue()),
                properties.workflow().semanticQueue(),
                queueDepth(properties.workflow().semanticQueue()),
                properties.workflow().relevanceScoreQueue(),
                queueDepth(properties.workflow().relevanceScoreQueue()),
                properties.workflow().snapshotProjectorQueue(),
                queueDepth(properties.workflow().snapshotProjectorQueue()));
    }

    private int countRows(String table, String idColumn, String id) {
        Integer count = jdbcTemplate.queryForObject(
                "select count(*) from " + table + " where " + idColumn + " = ?", Integer.class, id);
        return count == null ? 0 : count;
    }

    private List<String> operationIds(Map<?, ?> activity) {
        List<String> ids = new ArrayList<>();
        for (Object operation : (List<?>) activity.get("operations")) {
            ids.add(((Map<?, ?>) operation).get("id").toString());
        }
        return ids;
    }

    private List<String> fieldNames(Map<?, ?> schema) {
        List<String> names = new ArrayList<>();
        for (Object field : (List<?>) schema.get("fields")) {
            names.add(((Map<?, ?>) field).get("name").toString());
        }
        return names;
    }

    private void assertQueryableRows(Map<?, ?> queryBody) {
        List<?> rows = (List<?>) queryBody.get("rows");
        assertThat(rows).hasSize(2);
        List<Double> relevanceScores = new ArrayList<>();
        for (Object row : rows) {
            Map<?, ?> resultItem = (Map<?, ?>) row;
            assertThat(resultItem.get("sector")).isEqualTo("financials");
            assertThat(resultItem.get("ticker")).isIn("V", "MA");
            assertThat(resultItem.get("region")).isIn("APAC", "Global");
            relevanceScores.add(((Number) resultItem.get("relevance_score")).doubleValue());
        }
        assertThat(relevanceScores).isSortedAccordingTo(java.util.Comparator.reverseOrder());
        Map<?, ?> page = (Map<?, ?>) queryBody.get("page");
        assertThat(page.get("limit")).isEqualTo(2);
        assertThat(page.get("offset")).isEqualTo(0);
        assertThat((Integer) page.get("total")).isGreaterThanOrEqualTo(rows.size());
    }

    private void assertQueryableGroups(Map<?, ?> queryBody) {
        List<?> groups = (List<?>) queryBody.get("groups");
        assertThat(groups).isNotEmpty();
        for (Object group : groups) {
            Map<?, ?> grouped = (Map<?, ?>) group;
            assertThat(((Map<?, ?>) grouped.get("key")).get("region")).isIn("APAC", "Global");
            assertThat((Integer) grouped.get("result_count")).isPositive();
            assertThat(((Number) grouped.get("avg_relevance")).doubleValue()).isBetween(0.0, 10.0);
        }
    }

    private String requiredProperty(String propertyName) {
        String value = environment.getProperty(propertyName);
        assertThat(value).as(propertyName).isNotBlank();
        return value;
    }

    private void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for operation completion", exception);
        }
    }

    private static final class SseReader implements AutoCloseable {

        private final HttpURLConnection connection;
        private final BufferedReader reader;

        private SseReader(HttpURLConnection connection) throws java.io.IOException {
            this.connection = connection;
            this.reader =
                    new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
        }

        private String readEventsUntil(String expectedText) {
            StringBuilder events = new StringBuilder();
            Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
            while (Instant.now().isBefore(deadline)) {
                String event = readEvent();
                events.append(event);
                if (events.toString().contains(expectedText)) {
                    return events.toString();
                }
            }
            throw new AssertionError("SSE stream did not emit " + expectedText + ": " + events);
        }

        private String readEvent() {
            try {
                StringBuilder event = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    event.append(line).append('\n');
                    if (line.isBlank() && event.toString().contains("data:")) {
                        return event.toString();
                    }
                }
                throw new AssertionError("SSE stream ended before another event");
            } catch (java.io.IOException exception) {
                throw new IllegalStateException("Could not read SSE response", exception);
            }
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (java.io.IOException ignored) {
                // Closing the HTTP connection is enough for test cleanup.
            }
            connection.disconnect();
        }
    }
}
