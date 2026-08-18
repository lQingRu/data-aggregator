package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dataaggregator.config.DataAggregatorProperties;
import com.dataaggregator.support.IntegrationTestContainers;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ResultSnapshotApiIT extends IntegrationTestContainers {

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
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @Autowired
    private TestRestTemplate restTemplate;

    @BeforeEach
    void seedReadySnapshot() {
        amqpAdmin.purgeQueue(properties.workflow().lexicalQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().semanticQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().relevanceScoreQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().snapshotProjectorQueue(), true);
        amqpAdmin.purgeQueue(properties.workflow().completionEventQueue(), true);

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            jdbcTemplate.update("delete from result_items where result_snapshot_id = 'snap_api'");
            jdbcTemplate.update("delete from result_snapshots where id = 'snap_api'");
            jdbcTemplate.update("delete from search_runs where id = 'run_api'");
            jdbcTemplate.update("delete from search_requests where id = 'sr_api'");
            jdbcTemplate.update("delete from async_runs where id = 'op_api'");
            jdbcTemplate.update(
                    """
                    insert into async_runs (
                      id, operation_type, status, scope_type, scope_id, user_id, completed_steps, total_steps
                    ) values ('op_api', 'search_run', 'completed', 'result_snapshot', 'snap_api', 'user_test', 4, 4)
                    """);
            jdbcTemplate.update(
                    """
                    insert into search_requests (
                      id, user_id, workflow, keywords, question, retrieval_filters_json, initial_sort_json
                    ) values (
                      'sr_api', 'user_test', 'hybrid_chunk_search', 'payments',
                      'Which markets show the strongest payment growth?', '{}'::jsonb, '{}'::jsonb
                    )
                    """);
            jdbcTemplate.update(
                    """
                    insert into search_runs (
                      id, async_run_id, search_request_id, workflow, workflow_config_version, result_snapshot_id
                    ) values ('run_api', 'op_api', 'sr_api', 'hybrid_chunk_search', 1, 'snap_api')
                    """);
            jdbcTemplate.update(
                    """
                    insert into result_snapshots (
                      id, search_run_id, user_id, status, schema_json, default_sort_json, ready_at
                    ) values (
                      'snap_api', 'run_api', 'user_test', 'ready',
                      cast(? as jsonb), cast(? as jsonb), now()
                    )
                    """,
                    schemaJson(),
                    """
                    [
                      {"field":"relevance_score","direction":"desc","nulls":"last"},
                      {"field":"published_at","direction":"desc","nulls":"last"}
                    ]
                    """);
            insertResultItem(
                    "item_api_1",
                    "chunk_api_1",
                    "APAC",
                    "financials",
                    "V",
                    "Visa Inc.",
                    "2026-01-15T00:00:00Z",
                    8.70,
                    2,
                    1);
            insertResultItem(
                    "item_api_2",
                    "chunk_api_2",
                    "Global",
                    "financials",
                    "MA",
                    "Mastercard Inc.",
                    "2026-02-01T00:00:00Z",
                    9.10,
                    1,
                    2);
            insertResultItem(
                    "item_api_3",
                    "chunk_api_3",
                    "Europe",
                    "technology",
                    "MSFT",
                    "Microsoft Corp.",
                    "2026-01-20T00:00:00Z",
                    null,
                    3,
                    3);
        });
    }

    @AfterEach
    void stopListenersBeforeContainersShutdown() {
        listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::stop);
    }

    @Test
    void resultSnapshotQueryFiltersSortsGroupsAggregatesAndDoesNotScheduleRetrieval() {
        Map<String, Object> request = Map.of(
                "filters",
                List.of(
                        Map.of("field", "sector", "op", "in", "value", List.of("financials")),
                        Map.of("field", "ticker", "op", "in", "value", List.of("V", "MA")),
                        Map.of("field", "region", "op", "in", "value", List.of("APAC", "Global"))),
                "sort",
                List.of(Map.of("field", "relevance_score", "direction", "desc", "nulls", "last")),
                "group_by",
                List.of("region"),
                "aggregations",
                List.of(
                        Map.of("name", "result_count", "op", "count"),
                        Map.of("name", "avg_relevance", "op", "avg", "field", "relevance_score")),
                "page",
                Map.of("limit", 10, "offset", 0));

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", request, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.get("snapshot_id")).isEqualTo("snap_api");
        List<String> rowChunkIds = new ArrayList<>();
        for (Object row : (List<?>) body.get("rows")) {
            rowChunkIds.add(((Map<?, ?>) row).get("chunk_id").toString());
        }
        assertThat(rowChunkIds).containsExactly("chunk_api_2", "chunk_api_1");
        assertThat(((Map<?, ?>) body.get("page")).get("total")).isEqualTo(2);
        assertThat((List<?>) body.get("groups")).hasSize(2);
        assertThat((List<?>) body.get("groups")).anySatisfy(group -> {
            Map<?, ?> grouped = (Map<?, ?>) group;
            assertThat(((Map<?, ?>) grouped.get("key")).get("region")).isEqualTo("Global");
            assertThat(grouped.get("result_count")).isEqualTo(1);
            assertThat((Number) grouped.get("avg_relevance")).hasToString("9.1");
        });
        assertWorkflowQueuesEmpty();
    }

    @Test
    void resultSnapshotSchemaEndpointReturnsStoredSchema() {
        ResponseEntity<Map> response =
                restTemplate.getForEntity("http://localhost:" + port + "/result-snapshots/snap_api/schema", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("snapshot_id")).isEqualTo("snap_api");
        List<String> fieldNames = new ArrayList<>();
        for (Object field : (List<?>) response.getBody().get("fields")) {
            fieldNames.add(((Map<?, ?>) field).get("name").toString());
        }
        assertThat(fieldNames).contains("sector", "ticker", "region", "relevance_score");
    }

    @Test
    void resultSnapshotMetadataEndpointReturnsReadinessMetadata() {
        ResponseEntity<Map> response =
                restTemplate.getForEntity("http://localhost:" + port + "/result-snapshots/snap_api", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("snapshot_id", "snap_api");
        assertThat(response.getBody()).containsEntry("status", "ready");
        assertThat(response.getBody()).containsEntry("search_run_id", "run_api");
    }

    @Test
    void resultSnapshotQueryRejectsUnsupportedFieldsOperatorsAndInvalidQueryRequests() {
        Map<String, Object> unsupportedField =
                Map.of("filters", List.of(Map.of("field", "market_cap", "op", "in", "value", List.of("large"))));
        Map<String, Object> unsupportedOperator =
                Map.of("filters", List.of(Map.of("field", "sector", "op", "contains", "value", "financials")));
        Map<String, Object> invalidOperatorForField =
                Map.of("filters", List.of(Map.of("field", "sector", "op", "gte", "value", "financials")));
        Map<String, Object> invalidSort =
                Map.of("sort", List.of(Map.of("field", "source_contributions", "direction", "desc")));
        Map<String, Object> invalidGroup = Map.of("group_by", List.of("published_at"));
        Map<String, Object> invalidAggregation = Map.of(
                "group_by",
                List.of("region"),
                "aggregations",
                List.of(Map.of("name", "avg_region", "op", "avg", "field", "region")));

        ResponseEntity<Map> fieldResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", unsupportedField, Map.class);
        ResponseEntity<Map> operatorResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", unsupportedOperator, Map.class);
        ResponseEntity<Map> fieldOperatorResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", invalidOperatorForField, Map.class);
        ResponseEntity<Map> sortResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", invalidSort, Map.class);
        ResponseEntity<Map> groupResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", invalidGroup, Map.class);
        ResponseEntity<Map> aggregationResponse = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", invalidAggregation, Map.class);

        assertThat(fieldResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(operatorResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(fieldOperatorResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(sortResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(groupResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(aggregationResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void resultSnapshotQueryRejectsSnapshotsThatAreNotReady() {
        jdbcTemplate.update("update result_snapshots set status = 'pending', ready_at = null where id = 'snap_api'");
        Map<String, Object> request = Map.of("page", Map.of("limit", 10, "offset", 0));

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/result-snapshots/snap_api/query", request, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    private void insertResultItem(
            String id,
            String chunkId,
            String region,
            String sector,
            String ticker,
            String companyName,
            String publishedAt,
            Double relevanceScore,
            int lexicalRank,
            int defaultRank) {
        jdbcTemplate.update(
                """
                insert into result_items (
                  id, result_snapshot_id, chunk_id, parent_entity_id, parent_title, parent_type, source_name,
                  ticker, company_name, sector, region, published_at, author, chunk_index, chunk_text,
                  relevance_score, lexical_rank, default_rank, source_contributions_json
                ) values (
                  ?, 'snap_api', ?, 'doc_api', 'Payments Market Outlook', 'report', 'Internal Research',
                  ?, ?, ?, ?, cast(? as timestamptz), 'Research Desk', 1,
                  'Digital wallet adoption accelerated across the region.', ?, ?, ?,
                  '["lexical_retrieval","semantic_retrieval"]'::jsonb
                )
                """,
                id,
                chunkId,
                ticker,
                companyName,
                sector,
                region,
                publishedAt,
                relevanceScore,
                lexicalRank,
                defaultRank);
    }

    private void assertWorkflowQueuesEmpty() {
        assertNoMessage(properties.workflow().lexicalQueue());
        assertNoMessage(properties.workflow().semanticQueue());
        assertNoMessage(properties.workflow().relevanceScoreQueue());
        assertNoMessage(properties.workflow().snapshotProjectorQueue());
    }

    private void assertNoMessage(String queue) {
        Message message = rabbitTemplate.receive(queue, 100);
        assertThat(message).as("unexpected message in %s", queue).isNull();
    }

    private String schemaJson() {
        return """
        {
          "snapshot_id": "snap_api",
          "fields": [
            {"name":"parent_title","type":"string","filterable":true,"sortable":true,"groupable":false,"aggregatable":false,"nullable":false},
            {"name":"parent_type","type":"enum","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"source_name","type":"enum","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"ticker","type":"string","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"company_name","type":"string","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"sector","type":"enum","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"region","type":"enum","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":false},
            {"name":"published_at","type":"datetime","filterable":true,"sortable":true,"groupable":false,"aggregatable":false,"nullable":false},
            {"name":"author","type":"string","filterable":true,"sortable":true,"groupable":true,"aggregatable":false,"nullable":true},
            {"name":"relevance_score","type":"number","filterable":true,"sortable":true,"groupable":false,"aggregatable":true,"nullable":true},
            {"name":"lexical_rank","type":"number","filterable":true,"sortable":true,"groupable":false,"aggregatable":true,"nullable":true}
          ],
          "default_sort": [
            {"field":"relevance_score","direction":"desc","nulls":"last"},
            {"field":"published_at","direction":"desc","nulls":"last"}
          ]
        }
        """;
    }
}
