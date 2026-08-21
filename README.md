# Data Aggregator

Data Aggregator is an architecture test for asynchronous search and enrichment workflows. The core idea is to take a user search request, fan work out through an orchestrator and worker queue, materialize results into Postgres, and let the frontend query and interact with that stable result set.

The project is exploring a reusable pattern for expensive or progress-visible work. Search is the first workflow, but the same model is meant to support later async use cases such as score explanations, bulk enrichment, exports, and other batch operations.

## What Phase One Will Prove

Phase one is intended to prove that the backend can:

- accept a `Search Request`
- create a durable `Search Run`
- use an `Orchestrator` to schedule a `Workflow DAG`
- dispatch `Worker Command` messages through RabbitMQ
- let workers write durable state and emit `Worker Completion Event` updates
- materialize a `Result Snapshot` in Postgres
- let the frontend filter, sort, group, and aggregate on the snapshot without re-querying Elasticsearch
- notify the frontend of progress through SSE

## Core Ideas

- `Async Run` is the shared backend model for durable async work
- `Operation` is the frontend/API-facing term for an async run
- `Search Run` and `Enrichment Run` are specialized async runs
- `Result Snapshot` is the stable, queryable materialization of results
- `Snapshot Filter` never triggers primary-store retrieval
- `Retrieval Filter` shapes what enters the snapshot before retrieval finishes
- `Hybrid Chunk Search` is the first prototype workflow

## Architecture At A Glance

1. The API creates durable records in Postgres.
2. The orchestrator publishes worker commands to RabbitMQ.
3. Workers execute one step at a time and persist their outputs and step state.
4. The orchestrator advances the DAG when upstream steps finish.
5. The `Snapshot Projector` turns contributions and enrichments into queryable `Result Items`.
6. The frontend subscribes to SSE for progress updates and fetches authoritative data over HTTP.

## Phases

### Phase 1

Phase one proves the reusable async search architecture:

- mock `Investment Research Corpus`
- search request and run creation
- RabbitMQ-based worker dispatch
- lexical retrieval
- semantic retrieval
- mocked relevance scoring
- projection into Postgres
- snapshot querying for filtering, sorting, grouping, and aggregation
- notification-only SSE

### Phase 2

Phase two adds reusable ad hoc enrichment:

- score explanations
- batch progress
- rate limiting and backoff for expensive enrichment
- enrichment runs attached to existing result snapshots

## Domain Language

The project keeps its glossary in [CONTEXT.md](./CONTEXT.md).

## Design Decisions

The main architectural decisions are recorded in [docs/adr](./docs/adr/).

## Implementation Specs

The concrete phase-one build contract is recorded in [docs/specs/phase-1-implementation.md](./docs/specs/phase-1-implementation.md).

## Chosen Stack

Phase one uses Java 21 and Spring Boot so the architecture POC stays close to the existing backend environment. Use Maven by default unless the existing backend standard is Gradle. Use Postgres, RabbitMQ, Flyway, Spring AMQP, JUnit 5, Testcontainers, Docker Compose, and GitHub Actions.

Use Spring Data JPA for ordinary lifecycle persistence where it stays simple. Use explicit SQL or jOOQ-style query construction for Result Snapshot querying if dynamic filters, sorting, grouping, and aggregation become awkward through JPA.

## Runtime Scaffold

The scaffold is a minimal Spring Boot application. It creates clear homes for API code, worker code, persistence migrations, configuration, and tests without implementing the durable Async Run schema or the Hybrid Chunk Search workflow yet.

- API code: `src/main/java/com/dataaggregator/api`
- worker code: `src/main/java/com/dataaggregator/worker`
- persistence code: `src/main/java/com/dataaggregator/persistence`
- workflow definitions: `src/main/java/com/dataaggregator/workflow`
- application configuration: `src/main/resources/application.yml`
- worker profile configuration: `src/main/resources/application-worker.yml`
- Flyway migrations: `src/main/resources/db/migration`
- tests: `src/test/java`

### Local Services

Start Postgres and RabbitMQ:

```bash
docker compose up -d postgres rabbitmq
```

Postgres listens on `localhost:5432` with database, username, and password all set to `data_aggregator`. RabbitMQ listens on `localhost:5672`, and its management UI is available at `http://localhost:15672` with `guest` / `guest`.

### API Server

Run the API server:

```bash
./mvnw spring-boot:run
```

The placeholder runtime endpoint is available at:

```bash
curl http://localhost:8080/internal/runtime
```

Swagger UI is available at `http://localhost:8080/swagger-ui.html`, and the generated OpenAPI JSON is available at `http://localhost:8080/v3/api-docs`.

### Worker Process

Run the placeholder worker process:

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=worker
```

The worker declares and listens to the durable placeholder queue `data-aggregator.worker.placeholder`. Real Worker Commands, Workflow DAG scheduling, and Worker Completion Events are intentionally left for later implementation tickets.

### Tests And Checks

Run the full verification suite:

```bash
./mvnw verify
```

On Docker Desktop environments where Testcontainers cannot mount the Docker socket for Ryuk, run the same verification with Ryuk disabled:

```bash
TESTCONTAINERS_RYUK_DISABLED=true ./mvnw -B verify
```

Run individual checks:

```bash
./mvnw spotless:check
./mvnw checkstyle:check
./mvnw test
```

Integration tests use Testcontainers for Postgres and RabbitMQ, so Docker must be available locally.

## Roadmap

Phase-one scope and recommended implementation order live in [docs/specs/phase-1-implementation.md](./docs/specs/phase-1-implementation.md). Live implementation status is tracked in GitHub Issues.

## Status

The runtime scaffold is in place. See GitHub Issues for current implementation status.
