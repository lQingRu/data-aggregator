# Phase One and Phase Two Scope

## Summary
Phase one proves the reusable async search architecture. Phase two adds Ad Hoc Enrichment such as expensive score explanations, batch progress, and richer rate limiting.

## Context
The project is an architecture test, not a deep retrieval or LLM inference project. Mocking Elasticsearch, vector search, and relevance scoring is acceptable when doing so keeps attention on queueing, orchestration, materialization, notification, and query behavior. We decided to split the work into two phases.

Phase one implements Hybrid Chunk Search on top of the shared Async Run model. It includes the Search Request API, Search Run creation, RabbitMQ Worker Commands, worker queues by worker type, lexical retrieval, semantic retrieval, mocked relevance scoring, the Snapshot Projector, Result Snapshot tables, Snapshot Schema, notification-only SSE, and the synchronous snapshot query endpoint for filtering, sorting, grouping, and aggregation. Snapshot querying is a normal API over Postgres and does not use async orchestration.

Phase two implements Ad Hoc Enrichment and Enrichment Runs. This includes user-triggered score explanations, batch progress, expensive/rate-limited enrichment handling, backoff, quotas, and SSE progress for enrichment work attached to an existing Result Snapshot. Ad Hoc Enrichment reuses the same async run, worker state, RabbitMQ, and SSE patterns, but it is not a new Search Run.

## Decision
The rule for choosing async orchestration is explicit: use it for work that is slow, expensive, rate-limited, retryable, batch-oriented, fanout/fanin, or user-visible as progress. Use a normal synchronous API for quick deterministic reads, especially Result Snapshot filtering, sorting, grouping, and aggregation after the snapshot is ready.

## Considered Options
Considered options included building ad hoc explanations in phase one, integrating real Elasticsearch immediately, and making every frontend action an async Operation. Those choices would obscure whether the architecture itself works. Phase one should be boring and convincing; phase two should test reuse under more expensive enrichment behavior.

## Consequences
Early demos may use mocked retrieval and scoring, but the architecture must be real enough to replace those mocks later. The phase split keeps the first milestone focused while preserving the requirement that future async use cases reuse the same model.
