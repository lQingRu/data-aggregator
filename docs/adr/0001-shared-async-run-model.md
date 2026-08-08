# Shared Async Run Model

## Summary
Expensive, retryable, or progress-visible work will use a shared Async Run model. Search Runs and Enrichment Runs are specialized Async Runs, while frontend-facing APIs expose them as Operations.

## Context
The project needs to test workflows where a user submits a search, backend work fans out to multiple workers, results are materialized, and the frontend learns when the work is ready. The same shape may later apply to ad hoc score explanations, bulk enrichment, exports, report generation, and other expensive or rate-limited operations. We decided to build a small shared async run model instead of creating one-off background flows for each use case.

An Async Run is a durable execution record with status, progress, warnings, ownership, and optional parent/scope relationships. Its shared statuses are `queued`, `running`, `waiting_retry`, `completed`, `completed_with_warnings`, `failed`, `cancelled`, and `superseded`. Search-specific and enrichment-specific details live in separate tables; shared lifecycle data lives in `async_runs`; flexible debug metadata may use JSONB when it is not part of query behavior.

## Decision
The public API should use the term Operation rather than job. A job sounds like queue internals, while Operation better describes user-visible work that may be searched, enriched, cancelled, retried, or observed. Endpoints can include `GET /operations/{id}` for a specific Operation and `GET /result-snapshots/{id}/activity` for page-level status. `POST /search-requests` should return IDs such as `search_request_id`, `search_run_id`, `result_snapshot_id`, and an Operation object, but should not return a derived `events_url`; clients can use the documented events endpoint.

## Considered Options
Considered options included making search-specific run tables only, storing all run details as JSON, and exposing queue/job terminology directly. Search-specific tables would make later enrichment/export workflows repeat the same lifecycle machinery. JSON-only storage would make filtering by status, owner, progress, and warnings harder. Job terminology would couple the frontend to implementation mechanics.

## Consequences
The first implementation must include `async_runs` and `async_run_steps` even though the first concrete workflow is search. This adds a little upfront modeling cost, but keeps the architecture reusable and gives the frontend one way to observe durable async work.
