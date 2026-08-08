# Notification-Only SSE for Async Activity

## Summary
The frontend will use SSE as a lightweight notification channel for async activity. SSE events hint that state changed; HTTP APIs remain the source of truth for status and result data.

## Context
The frontend needs to know when a Search Run has progressed, when a Result Snapshot is ready, and later when an Enrichment Run such as batch score explanations has progressed or completed. The project should support a reusable notification pattern for future expensive workflows rather than a search-specific ready event. We decided to use notification-only SSE.

Clients subscribe by Run Scope, initially through `GET /events?scope_type=result_snapshot&scope_id={id}`. A Result Snapshot page can then receive Search Run and Enrichment Run events relevant to that snapshot. Event names should be generic, such as `async_run_progressed`, `async_run_completed`, and `async_run_failed`, with payload fields identifying the run type and IDs.

## Decision
SSE payloads should be small and should not contain result rows. They may include IDs, status, step progress, unit progress, warning counts, and changed timestamps. The frontend refetches authoritative state through endpoints such as `GET /result-snapshots/{id}`, `GET /result-snapshots/{id}/activity`, `GET /operations/{id}`, and `POST /result-snapshots/{id}/query`.

Progress supports both steps and units. Search DAGs use step progress such as completed steps out of total steps. Batch enrichment uses unit progress such as explained items out of selected items. SSE reconnection does not require replay in V1; on reconnect, the frontend refetches current state from Postgres-backed APIs.

SSE subscriptions and HTTP endpoints are authorized using OAuth-derived `user_id`. OAuth identifies the user, and the application checks that the user owns or can access the Result Snapshot or Operation. The project will not model `tenant_id` in V1.

## Considered Options
Considered options included WebSockets with result payloads, polling only, and persisted event replay. WebSockets are more capable than needed for one-way notifications. Polling only would work but gives a weaker user experience and does not test the intended async notification path. Persisted replay is unnecessary while current state can be fetched from Postgres.

## Consequences
Frontend code must treat SSE events as invalidation hints, not authoritative data. The backend must expose good state/query endpoints because SSE will not carry full results.
