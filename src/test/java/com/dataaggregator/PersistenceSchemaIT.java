package com.dataaggregator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dataaggregator.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class PersistenceSchemaIT extends IntegrationTestContainers {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void durableSearchRunGraphCanBePersisted() {
        insertSearchRunGraph("graph");

        Integer resultCount = jdbcTemplate.queryForObject(
                "select count(*) from result_items where result_snapshot_id = ?", Integer.class, "snap_graph");
        String operationStatus =
                jdbcTemplate.queryForObject("select status from async_runs where id = ?", String.class, "op_graph");

        assertThat(resultCount).isEqualTo(1);
        assertThat(operationStatus).isEqualTo("queued");
    }

    @Test
    void sourceContributionsAreIdempotentPerSearchRunStepAndChunk() {
        insertSearchRunGraph("dedupe");

        assertThatThrownBy(
                        () -> jdbcTemplate.update(
                                """
                        insert into worker_contributions (
                          id, search_run_id, workflow_step_id, chunk_id, contribution_type, rank, score, payload_json
                        ) values (
                          'contrib_dedupe_duplicate', 'run_dedupe', 'lexical_retrieval', 'chunk_000001',
                          'lexical', 2, 0.7000, '{}'::jsonb
                        )
                        """))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void resultItemsAreIdempotentPerSnapshotAndChunk() {
        insertSearchRunGraph("item_dedupe");

        assertThatThrownBy(
                        () -> jdbcTemplate.update(
                                """
                        insert into result_items (
                          id, result_snapshot_id, chunk_id, parent_entity_id, parent_title, parent_type, source_name,
                          ticker, company_name, sector, region, published_at, author, chunk_index, chunk_text,
                          relevance_score, lexical_rank, default_rank, source_contributions_json
                        ) values (
                          'item_dedupe_duplicate', 'snap_item_dedupe', 'chunk_000001', 'doc_0001',
                          'Asia Payments Market Outlook 2026', 'report', 'Internal Research', 'V', 'Visa Inc.',
                          'financials', 'APAC', '2026-01-15T00:00:00Z', 'Research Desk', 3,
                          'Digital wallet adoption accelerated across Southeast Asia...', 7.10, 2, 2, '[]'::jsonb
                        )
                        """))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void asyncRunStatusesSupportTheSharedLifecycleVocabulary() {
        String[] statuses = {
            "queued",
            "running",
            "waiting_retry",
            "completed",
            "completed_with_warnings",
            "failed",
            "cancelled",
            "superseded"
        };

        for (String runStatus : statuses) {
            jdbcTemplate.update(
                    """
                    insert into async_runs (
                      id, operation_type, status, scope_type, scope_id, user_id, total_steps
                    ) values (?, 'search_run', ?, 'result_snapshot', ?, 'user_test', 4)
                    """,
                    "op_status_" + runStatus,
                    runStatus,
                    "snap_status_" + runStatus);
        }

        Integer statusCount =
                jdbcTemplate.queryForObject("select count(distinct status) from async_runs", Integer.class);

        assertThat(statusCount).isEqualTo(statuses.length);
    }

    @Test
    void resultSnapshotSchemaStoresQueryableFieldCapabilities() {
        insertSearchRunGraph("schema");

        jdbcTemplate.update(
                """
                update result_snapshots
                set schema_json = cast(? as jsonb),
                    default_sort_json = cast(? as jsonb)
                where id = 'snap_schema'
                """,
                """
                {
                  "snapshot_id": "snap_schema",
                  "fields": [
                    {
                      "name": "parent_title",
                      "type": "string",
                      "filterable": true,
                      "sortable": true,
                      "groupable": false,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "parent_type",
                      "type": "enum",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "source_name",
                      "type": "enum",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "ticker",
                      "type": "string",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "company_name",
                      "type": "string",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "sector",
                      "type": "enum",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "region",
                      "type": "enum",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "published_at",
                      "type": "datetime",
                      "filterable": true,
                      "sortable": true,
                      "groupable": false,
                      "aggregatable": false,
                      "nullable": false
                    },
                    {
                      "name": "author",
                      "type": "string",
                      "filterable": true,
                      "sortable": true,
                      "groupable": true,
                      "aggregatable": false,
                      "nullable": true
                    },
                    {
                      "name": "relevance_score",
                      "type": "number",
                      "filterable": true,
                      "sortable": true,
                      "groupable": false,
                      "aggregatable": true,
                      "nullable": true
                    },
                    {
                      "name": "lexical_rank",
                      "type": "number",
                      "filterable": true,
                      "sortable": true,
                      "groupable": false,
                      "aggregatable": true,
                      "nullable": true
                    }
                  ]
                }
                """,
                """
                [
                  {
                    "field": "relevance_score",
                    "direction": "desc",
                    "nulls": "last"
                  },
                  {
                    "field": "published_at",
                    "direction": "desc",
                    "nulls": "last"
                  }
                ]
                """);

        Integer schemaFieldCount = jdbcTemplate.queryForObject(
                "select jsonb_array_length(schema_json -> 'fields') from result_snapshots where id = 'snap_schema'",
                Integer.class);
        Boolean relevanceScoreAggregatable = jdbcTemplate.queryForObject(
                "select (schema_json #>> '{fields,9,aggregatable}')::boolean "
                        + "from result_snapshots where id = 'snap_schema'",
                Boolean.class);
        Boolean relevanceScoreNullable = jdbcTemplate.queryForObject(
                "select (schema_json #>> '{fields,9,nullable}')::boolean "
                        + "from result_snapshots where id = 'snap_schema'",
                Boolean.class);
        String secondaryDefaultSort = jdbcTemplate.queryForObject(
                "select default_sort_json #>> '{1,field}' " + "from result_snapshots where id = 'snap_schema'",
                String.class);

        assertThat(schemaFieldCount).isEqualTo(11);
        assertThat(relevanceScoreAggregatable).isTrue();
        assertThat(relevanceScoreNullable).isTrue();
        assertThat(secondaryDefaultSort).isEqualTo("published_at");
    }

    @Test
    void jsonDocumentColumnsRejectWrongTopLevelShapes() {
        assertThatThrownBy(
                        () -> jdbcTemplate.update(
                                """
                        insert into async_runs (
                          id, operation_type, status, scope_type, scope_id, user_id, total_steps, warnings_json
                        ) values (
                          'op_bad_json', 'search_run', 'queued', 'result_snapshot', 'snap_bad_json',
                          'user_test', 4, '{}'::jsonb
                        )
                        """))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private void insertSearchRunGraph(String suffix) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            jdbcTemplate.update(
                    """
                    insert into async_runs (
                      id, operation_type, status, scope_type, scope_id, user_id, current_step,
                      completed_steps, total_steps, warnings_json, metadata_json
                    ) values (?, 'search_run', 'queued', 'result_snapshot', ?, 'user_test', null,
                      0, 4, '[]'::jsonb, '{}'::jsonb)
                    """,
                    "op_" + suffix,
                    "snap_" + suffix);
            jdbcTemplate.update(
                    """
                    insert into search_requests (
                      id, user_id, workflow, keywords, question, retrieval_filters_json, initial_sort_json
                    ) values (?, 'user_test', 'hybrid_chunk_search', 'digital wallet adoption',
                      'Which markets show the strongest payment growth?',
                      '{"sector":["financials"]}'::jsonb, '{"field":"relevance_score","direction":"desc"}'::jsonb)
                    """,
                    "sr_" + suffix);
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
                    ) values (?, ?, 'user_test', 'pending', '{}'::jsonb,
                      '[{"field":"relevance_score","direction":"desc"}]'::jsonb)
                    """,
                    "snap_" + suffix,
                    "run_" + suffix);
            jdbcTemplate.update(
                    """
                    insert into async_run_steps (
                      id, async_run_id, workflow_step_id, status, required, attempt_count, max_attempts
                    ) values (?, ?, 'lexical_retrieval', 'queued', true, 0, 3)
                    """,
                    "step_" + suffix,
                    "op_" + suffix);
            jdbcTemplate.update(
                    """
                    insert into worker_contributions (
                      id, search_run_id, workflow_step_id, chunk_id, contribution_type, rank, score, payload_json
                    ) values (?, ?, 'lexical_retrieval', 'chunk_000001', 'lexical', 1, 0.9100, '{}'::jsonb)
                    """,
                    "contrib_" + suffix,
                    "run_" + suffix);
            jdbcTemplate.update(
                    """
                    insert into result_items (
                      id, result_snapshot_id, chunk_id, parent_entity_id, parent_title, parent_type, source_name,
                      ticker, company_name, sector, region, published_at, author, chunk_index, chunk_text,
                      relevance_score, lexical_rank, default_rank, source_contributions_json
                    ) values (?, ?, 'chunk_000001', 'doc_0001', 'Asia Payments Market Outlook 2026', 'report',
                      'Internal Research', 'V', 'Visa Inc.', 'financials', 'APAC', '2026-01-15T00:00:00Z',
                      'Research Desk', 3, 'Digital wallet adoption accelerated across Southeast Asia...',
                      8.50, 1, 1, '[]'::jsonb)
                    """,
                    "item_" + suffix,
                    "snap_" + suffix);
        });
    }
}
