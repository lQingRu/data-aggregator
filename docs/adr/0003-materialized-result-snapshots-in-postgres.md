# Materialized Result Snapshots in Postgres

## Summary
Search results will be materialized into stable Result Snapshots in Postgres so filtering, sorting, grouping, and aggregation operate on already-retrieved data instead of re-querying Elasticsearch.

## Context
The primary datastore is Elasticsearch, but some user operations may be expensive or awkward there, especially when the initial candidate set can exceed 10,000 results. The project also needs to combine results from lexical retrieval, normal filtering, semantic retrieval, and later enrichment workers. We decided to materialize queryable Result Snapshots in Postgres.

A Result Snapshot is stable for the user's interaction. Snapshot Filters, sorting, grouping, and aggregation never trigger new retrieval from the primary datastore. Retrieval Filters are separate: they shape what enters the snapshot before retrieval completes. Refreshing or changing retrieval semantics creates a new Search Run and a new Result Snapshot.

The Result Item grain is one chunk-level row. The `chunk_id` is globally unique and acts as the result identity. Parent-specific fields, such as the parent date used for default sorting, may be denormalized onto the Result Item. The Snapshot Schema defines which fields are sortable, filterable, groupable, and aggregatable. Missing optional fields are nullable in storage, and the Snapshot Schema documents field availability and null behavior.

## Decision
The first prototype should support up to 10,000 non-semantic retrieval results in total and up to 1,500 semantic results, with the semantic count configurable. The default sort is workflow-specific: lexical/filter workflows default to `sort_date desc`, while semantic or hybrid workflows default to `relevance_score desc` then `sort_date desc`; users may override with validated Snapshot Schema fields.

## Considered Options
Considered options included querying Elasticsearch for every frontend filter/sort/group operation, keeping the secondary store as a disposable cache only, and modeling results at parent-document grain. Re-querying Elasticsearch would not test the intended architecture and could make expensive operations repeat. A disposable cache would not support user quotas and persisted result viewing. Parent-document grain would hide chunk-level semantic scoring.

## Consequences
Postgres becomes a durable read model for user-visible result exploration, not the source of truth for primary data. Snapshots may become stale relative to Elasticsearch; this is accepted. The UI should show creation time, and refresh should create a new Search Run.
