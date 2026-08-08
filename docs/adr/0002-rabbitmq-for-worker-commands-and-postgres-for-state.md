# RabbitMQ for Worker Commands and Postgres for State

## Summary
RabbitMQ will deliver transient Worker Commands and Worker Completion Events. Postgres will hold durable run state, step state, request records, warnings, retries, and materialized outputs.

## Context
The architecture needs fanout across worker types such as lexical retrieval, semantic retrieval, mocked relevance scoring, and projection. It also needs recovery when workers crash, messages are redelivered, or optional work fails after all retries. We decided that RabbitMQ is the delivery mechanism, not the system of record.

The API first creates durable records in Postgres for the Search Request, Search Run, and empty Result Snapshot. The orchestrator then publishes Worker Commands to RabbitMQ. Worker Commands may include small immutable search parameters directly, along with IDs such as `search_run_id`, `workflow_step_id`, and the workflow config version. This avoids unnecessary worker lookups on the hot path while keeping Postgres as the recovery and audit source.

## Decision
Workers write durable progress, outputs, and terminal step state to Postgres, then publish lightweight Worker Completion Events. The orchestrator consumes those events, reads durable state, advances the Workflow DAG, and publishes the next unblocked Worker Commands. If a completion event is lost, the system can recover from Postgres state.

## Considered Options
Considered options included putting all required payload only in RabbitMQ, having workers call each other directly, and using Postgres polling without broker events. RabbitMQ-only state would make recovery and user-visible status fragile. Direct worker chaining would spread DAG knowledge across workers. Polling alone would be simpler but less responsive and less representative of the intended fanout/fanin architecture.

## Consequences
Commands and small parameters may be duplicated between RabbitMQ and Postgres. That is acceptable for V1 because the duplicated data is immutable and simplifies worker execution. Retry policy remains a logical application concern owned by the orchestrator and step state; RabbitMQ handles delivery mechanics.
