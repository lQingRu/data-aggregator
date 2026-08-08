# Phase 1 Implementation Spec

## Summary
Phase 1 implements a concrete `Hybrid Chunk Search` prototype for a mock Investment Research Corpus. The goal is to prove the async architecture, not retrieval quality: create a Search Request, execute a Workflow DAG through RabbitMQ workers, materialize a stable Result Snapshot in Postgres, notify the frontend through SSE, and query the snapshot synchronously for filtering, sorting, grouping, and aggregation.

## Tracking

Canonical implementation tracker: https://github.com/lQingRu/data-aggregator/issues/1

The tracker issue records implementation sequencing. GitHub issue state remains the source of truth for live task status.

## Execution Order and Parallelism

Do not start all implementation tickets in parallel from an empty repo. The phase-one runtime scaffold must land first because it chooses the stack, project layout, test runner, formatter/linter, CI checks, and local Postgres/RabbitMQ setup. Until that exists, other agents will likely conflict on package configuration, shared types, migrations, test harnesses, and runtime boundaries.

Recommended implementation order:

1. Implement the runtime scaffold, including minimum CI for formatting, linting, typechecking, tests, and build checks.
2. Implement durable Async Run and Result Snapshot schema.
3. Implement the Workflow DAG orchestrator and mock Investment Research Corpus workers.
4. Implement Result Snapshot projection/query APIs and Operation/SSE/auth APIs.
5. Implement the phase-one acceptance test suite.

After the scaffold exists, limited parallel work is reasonable. The schema ticket should still land before most orchestrator, worker, API, and query work. Agents working in parallel must coordinate around shared contracts for Worker Commands, Worker Completion Events, Snapshot Schema, Operation responses, and Result Snapshot query shapes.

## Chosen Phase 1 Stack

Use Java 21 and Spring Boot for the phase-one implementation. The POC should prove the architecture in the same backend ecosystem as the existing application rather than proving it in an unrelated runtime.

Required stack choices:

- Java 21
- Spring Boot
- Maven by default unless the existing backend standard is Gradle
- Spring Web for HTTP APIs
- Spring MVC SSE support for notification-only SSE unless the scaffold finds an existing reason to use WebFlux
- Postgres
- Flyway for database migrations
- RabbitMQ through Spring AMQP
- Docker Compose for local Postgres and RabbitMQ
- JUnit 5 for tests
- Testcontainers for integration tests that need Postgres and RabbitMQ
- GitHub Actions for CI

Persistence guidance:

- Use Spring Data JPA for ordinary lifecycle tables where it stays simple.
- Use explicit SQL, JDBC, or jOOQ-style query construction for Result Snapshot querying if dynamic filters, sorting, grouping, and aggregation become awkward through JPA.
- Do not let the scaffold agent replace RabbitMQ with an in-memory queue or a different broker.
- Do not let the scaffold agent replace Postgres with an embedded database for the main implementation. Embedded databases may be used only for narrow tests when they do not hide Postgres-specific behavior.

Minimum CI checks:

- formatting check
- lint or static analysis check
- build
- unit tests
- integration tests for Postgres/RabbitMQ-backed behavior when available

Scaffold boundary:

- The scaffold task should create project structure, configuration, local services, placeholder entrypoints, and CI.
- The scaffold task should not implement the durable schema, Workflow DAG orchestrator, workers, Result Snapshot query behavior, or real SSE behavior beyond placeholders needed to prove the application starts.

## Mock Domain
Use a mock Investment Research Corpus. Each Parent Entity is a stock-oriented report, memo, transcript, or filing. Each Result Item is a chunk from one Parent Entity.

This domain is intentionally plain:

- It has date fields for default sorting.
- It has ticker, company, sector, and region fields for grouping.
- It has chunk text for lexical and semantic retrieval.
- It has enough metadata to prove Snapshot Filters and Snapshot Schema validation.
- It does not require real stock prices, time-series behavior, market data vendors, portfolio holdings, transactions, or financial calculations.

## Mock Corpus Shape
Seed between 200 and 500 chunks. The data can be generated deterministically at startup or loaded from a fixture.

Each chunk has:

```json
{
  "chunk_id": "chunk_000001",
  "parent_entity_id": "doc_0001",
  "parent_title": "Asia Payments Market Outlook 2026",
  "parent_type": "report",
  "source_name": "Internal Research",
  "ticker": "V",
  "company_name": "Visa Inc.",
  "sector": "financials",
  "region": "APAC",
  "published_at": "2026-01-15T00:00:00Z",
  "author": "Research Desk",
  "chunk_index": 3,
  "chunk_text": "Digital wallet adoption accelerated across Southeast Asia..."
}
```

Use these allowed values unless the implementation has a strong reason to add more:

- `parent_type`: `report`, `memo`, `transcript`, `filing`
- `source_name`: `Internal Research`, `Partner Feed`, `Public Filing`, `Earnings Transcript`
- `ticker`: `V`, `MA`, `NVDA`, `TSM`, `XOM`, `AAPL`, `MSFT`, `UNH`
- `sector`: `financials`, `technology`, `energy`, `consumer`, `healthcare`, `industrials`
- `region`: `Global`, `North America`, `Europe`, `APAC`, `Latin America`

## Search Request API
Create a Search Request with:

```json
{
  "workflow": "hybrid_chunk_search",
  "keywords": "digital wallet adoption",
  "question": "Which markets show the strongest payment growth?",
  "retrieval_filters": {
    "sector": ["financials"],
    "ticker": ["V", "MA"],
    "region": ["APAC", "Global"],
    "published_at": {
      "from": "2025-01-01T00:00:00Z",
      "to": "2026-12-31T23:59:59Z"
    }
  },
  "initial_sort": {
    "field": "relevance_score",
    "direction": "desc"
  }
}
```

Response:

```json
{
  "search_request_id": "sr_...",
  "search_run_id": "run_...",
  "result_snapshot_id": "snap_...",
  "operation": {
    "id": "op_...",
    "type": "search_run",
    "status": "queued",
    "scope_type": "result_snapshot",
    "scope_id": "snap_..."
  }
}
```

Do not return `events_url`. Clients use the documented SSE endpoint.

## API Surface
Phase 1 needs these endpoints:

- `POST /search-requests`
- `GET /operations/{operation_id}`
- `POST /operations/{operation_id}/cancel`
- `GET /events?scope_type=result_snapshot&scope_id={result_snapshot_id}`
- `GET /result-snapshots/{result_snapshot_id}`
- `GET /result-snapshots/{result_snapshot_id}/activity`
- `GET /result-snapshots/{result_snapshot_id}/schema`
- `POST /result-snapshots/{result_snapshot_id}/query`

## Operation Shape
Expose async work to the frontend as an Operation:

```json
{
  "id": "op_...",
  "type": "search_run",
  "status": "running",
  "scope_type": "result_snapshot",
  "scope_id": "snap_...",
  "current_step": "semantic_retrieval",
  "completed_steps": 1,
  "total_steps": 4,
  "completed_units": null,
  "total_units": null,
  "warnings": [],
  "created_at": "2026-08-08T00:00:00Z",
  "updated_at": "2026-08-08T00:00:01Z"
}
```

Allowed statuses:

- `queued`
- `running`
- `waiting_retry`
- `completed`
- `completed_with_warnings`
- `failed`
- `cancelled`
- `superseded`

## Workflow DAG
Define the `Hybrid Chunk Search` Workflow DAG in typed code.

Steps:

| Step | Queue | Required | Depends On | Output |
| --- | --- | --- | --- | --- |
| `lexical_retrieval` | `search.lexical` | yes | none | Source Contributions |
| `semantic_retrieval` | `search.semantic` | no | none | Source Contributions |
| `mock_relevance_scoring` | `enrichment.relevance_score` | no | `semantic_retrieval` | Relevance Scores |
| `snapshot_projection` | `snapshot.projector` | yes | all terminal retrieval and scoring steps | Result Items |

The orchestrator owns DAG advancement. Workers never schedule downstream workers directly.

Default runtime behavior:

- `lexical_retrieval` is required because it produces the Base Result Set.
- `snapshot_projection` is required because it makes the Result Snapshot queryable.
- `semantic_retrieval` is enabled but optional.
- `mock_relevance_scoring` is enabled but optional.
- Optional step failure after all retries produces `completed_with_warnings` if the Base Result Set can still be projected.

Strict DAG tests may run the same Workflow DAG with `semantic_retrieval` and `mock_relevance_scoring` marked required. This is only to prove dependency and failure behavior; it is not the default runtime profile.

## Worker Commands
RabbitMQ carries transient Worker Commands. Commands include small immutable parameters directly, while Postgres remains the durable source of truth.

Example:

```json
{
  "command_id": "cmd_...",
  "operation_id": "op_...",
  "search_run_id": "run_...",
  "workflow": "hybrid_chunk_search",
  "workflow_step_id": "semantic_retrieval",
  "workflow_config_version": 1,
  "result_snapshot_id": "snap_...",
  "user_id": "user_...",
  "params": {
    "keywords": "digital wallet adoption",
    "question": "Which markets show the strongest payment growth?",
    "retrieval_filters": {
      "sector": ["financials"],
      "ticker": ["V", "MA"],
      "region": ["APAC", "Global"]
    },
    "limit": 1500
  },
  "attempt": 1,
  "created_at": "2026-08-08T00:00:00Z"
}
```

## Worker Completion Events
Workers write durable state first, then publish a lightweight Worker Completion Event.

Example:

```json
{
  "event_id": "evt_...",
  "operation_id": "op_...",
  "search_run_id": "run_...",
  "workflow_step_id": "semantic_retrieval",
  "status": "completed",
  "contribution_count": 120,
  "warning_count": 0,
  "occurred_at": "2026-08-08T00:00:03Z"
}
```

If a completion event is lost, the orchestrator must be able to recover from Postgres state.

## SSE Events
SSE events are notification-only. They never include result rows.

Use generic event names:

- `async_run_progressed`
- `async_run_completed`
- `async_run_failed`
- `async_run_cancelled`
- `snapshot_ready`

Example event payload:

```json
{
  "operation_id": "op_...",
  "type": "search_run",
  "status": "running",
  "scope_type": "result_snapshot",
  "scope_id": "snap_...",
  "current_step": "mock_relevance_scoring",
  "completed_steps": 2,
  "total_steps": 4,
  "warning_count": 0,
  "changed_at": "2026-08-08T00:00:04Z"
}
```

On reconnect, clients refetch `GET /result-snapshots/{id}/activity` or `GET /operations/{id}`. V1 does not persist SSE event replay.

## Snapshot Schema
Return this schema from `GET /result-snapshots/{id}/schema`:

```json
{
  "snapshot_id": "snap_...",
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
  ],
  "default_sort": [
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
}
```

## Snapshot Query API
Request:

```json
{
  "filters": [
    {
      "field": "sector",
      "op": "in",
      "value": ["financials", "technology"]
    },
    {
      "field": "published_at",
      "op": "gte",
      "value": "2025-01-01T00:00:00Z"
    }
  ],
  "sort": [
    {
      "field": "relevance_score",
      "direction": "desc",
      "nulls": "last"
    }
  ],
  "group_by": ["region"],
  "aggregations": [
    {
      "name": "result_count",
      "op": "count"
    },
    {
      "name": "avg_relevance",
      "op": "avg",
      "field": "relevance_score"
    }
  ],
  "page": {
    "limit": 50,
    "offset": 0
  }
}
```

Response:

```json
{
  "snapshot_id": "snap_...",
  "rows": [
    {
      "chunk_id": "chunk_000001",
      "parent_entity_id": "doc_0001",
      "parent_title": "Asia Payments Market Outlook 2026",
      "parent_type": "report",
      "source_name": "Internal Research",
      "ticker": "V",
      "company_name": "Visa Inc.",
      "sector": "financials",
      "region": "APAC",
      "published_at": "2026-01-15T00:00:00Z",
      "author": "Research Desk",
      "chunk_text": "Digital wallet adoption accelerated across Southeast Asia...",
      "relevance_score": 8.7,
      "lexical_rank": 12,
      "source_contributions": ["lexical_retrieval", "semantic_retrieval"]
    }
  ],
  "groups": [
    {
      "key": {
        "region": "APAC"
      },
      "result_count": 24,
      "avg_relevance": 7.9
    }
  ],
  "page": {
    "limit": 50,
    "offset": 0,
    "total": 120
  }
}
```

Reject unsupported fields, unsupported operators, and invalid sort/group/aggregation requests based on the Snapshot Schema.

## Suggested Database Tables
The implementation can adapt names to the chosen stack, but it should preserve these responsibilities.

### async_runs
- `id`
- `operation_type`
- `status`
- `scope_type`
- `scope_id`
- `user_id`
- `parent_run_id`
- `current_step`
- `completed_steps`
- `total_steps`
- `completed_units`
- `total_units`
- `warnings_json`
- `metadata_json`
- `created_at`
- `updated_at`

### async_run_steps
- `id`
- `async_run_id`
- `workflow_step_id`
- `status`
- `required`
- `attempt_count`
- `max_attempts`
- `last_error`
- `started_at`
- `completed_at`
- `updated_at`

### search_requests
- `id`
- `user_id`
- `workflow`
- `keywords`
- `question`
- `retrieval_filters_json`
- `initial_sort_json`
- `created_at`

### search_runs
- `id`
- `async_run_id`
- `search_request_id`
- `workflow`
- `workflow_config_version`
- `result_snapshot_id`
- `created_at`

### result_snapshots
- `id`
- `search_run_id`
- `user_id`
- `status`
- `schema_json`
- `default_sort_json`
- `created_at`
- `ready_at`

### worker_contributions
- `id`
- `search_run_id`
- `workflow_step_id`
- `chunk_id`
- `contribution_type`
- `rank`
- `score`
- `payload_json`
- `created_at`

Use a uniqueness constraint on `search_run_id`, `workflow_step_id`, and `chunk_id`.

### result_items
- `id`
- `result_snapshot_id`
- `chunk_id`
- `parent_entity_id`
- `parent_title`
- `parent_type`
- `source_name`
- `ticker`
- `company_name`
- `sector`
- `region`
- `published_at`
- `author`
- `chunk_index`
- `chunk_text`
- `relevance_score`
- `lexical_rank`
- `default_rank`
- `source_contributions_json`
- `created_at`

Use a uniqueness constraint on `result_snapshot_id` and `chunk_id`.

## Worker Behavior
Lexical retrieval:

- Reads keywords and Retrieval Filters.
- Searches the mock corpus using deterministic keyword matching.
- Writes up to 10,000 Source Contributions.
- Assigns `lexical_rank`.

Semantic retrieval:

- Reads question and Retrieval Filters.
- Uses deterministic mock scoring against the corpus.
- Writes up to 1,500 Source Contributions.
- Does not produce final `relevance_score`.

Mock relevance scoring:

- Runs only after semantic retrieval is terminal.
- Produces a 0 to 10 Relevance Score for semantic contributions.
- May use deterministic seeded scoring so tests are stable.

Snapshot projection:

- Runs after all required upstream steps are terminal and optional enabled steps are terminal.
- Produces one Result Item per `chunk_id`.
- Denormalizes parent fields onto Result Items.
- Computes `default_rank`.
- Marks the Result Snapshot ready.

## Auth Assumption
OAuth integration itself can be mocked in phase one, but every HTTP and SSE request must behave as if an authenticated `user_id` exists. Operations and Result Snapshots are only accessible to their owning user.

Do not model `tenant_id` in V1.

## Acceptance Tests
At minimum, prove these behaviors:

1. Creating a Search Request creates an Operation, Search Run, and empty Result Snapshot.
2. The orchestrator schedules initial lexical and semantic worker steps.
3. A worker writes durable state before its Worker Completion Event is processed.
4. Semantic retrieval completion unlocks mock relevance scoring.
5. The Snapshot Projector runs after required upstream work is terminal.
6. The Result Snapshot becomes ready and queryable.
7. Snapshot query filters by `sector`, `ticker`, and `region`.
8. Snapshot query sorts by `relevance_score desc` with nulls last.
9. Snapshot query groups by `region` and returns counts.
10. Snapshot query rejects a field not present in the Snapshot Schema.
11. Snapshot query does not enqueue retrieval work.
12. SSE emits progress hints without result rows.
13. Reconnecting clients can recover by fetching snapshot activity.
14. Optional semantic or scoring failure can produce `completed_with_warnings` when the Base Result Set is available.
15. Required lexical or projection failure fails the Operation after retries are exhausted.
16. Retrying a worker does not duplicate contributions or Result Items.
17. A user cannot access another user's Operation, Result Snapshot, or SSE Run Scope.

## Out of Scope
Phase 1 does not include:

- real Elasticsearch integration
- real vector search infrastructure
- real LLM inference
- real reranker integration
- score explanation generation
- Ad Hoc Enrichment APIs
- Enrichment Runs
- persisted SSE replay
- tenant modeling
- progressive result querying before snapshot readiness
- externally configurable Workflow DAGs
- a production-grade market data, portfolio, transaction, or personal finance model

## Links
- Glossary: `CONTEXT.md`
- Project overview: `README.md`
- ADRs: `docs/adr/`
