# Data Aggregator

Data Aggregator explores asynchronous search workflows that materialize search results for later analysis, retrieval, and interaction.

## Language

**Search Workflow**:
The configured retrieval workflow for a Search Request, including accepted inputs, retrieval workers, completion rules, and the Snapshot Schema.
_Avoid_: Use case, mothership, base

**Hybrid Chunk Search**:
The prototype Search Workflow that combines lexical retrieval, semantic retrieval, mocked relevance scoring, projection, status notifications, and snapshot querying.
_Avoid_: Hybrid workflow, test workflow

**Investment Research Corpus**:
The mock finance-flavored document corpus used by Hybrid Chunk Search. It contains research reports, memos, transcripts, and filings with stock-oriented metadata, but it is not a portfolio, transaction, or market-pricing model.
_Avoid_: Market research corpus, personal finance data model

**Workflow DAG**:
The dependency graph of worker steps in a Search Workflow. The Workflow DAG determines which workers can run immediately and which workers wait for upstream steps.
_Avoid_: Flow, pipeline, queue chain

**Search Request**:
A user's intent to run a search workflow. It captures what the user asked for, independent of any particular execution attempt.
_Avoid_: Query, search job

**Async Run**:
A durable asynchronous execution with observable status and progress. Search Runs and Enrichment Runs are specialized Async Runs.
_Avoid_: Background job, task

**Operation**:
The API-facing representation of an Async Run. Frontend APIs expose Operations while backend code may use the more precise Async Run model.
_Avoid_: Job run, task

**Search Run**:
One execution attempt for a Search Request. A Search Run progresses asynchronously and may complete with warnings when optional work fails.
_Avoid_: Request, job, task

**Result Snapshot**:
A stable materialized set of results produced by a Search Run. The user filters, sorts, groups, and aggregates within this set without changing the underlying retrieval.
_Avoid_: Secondary database, result cache, workspace

**Base Result Set**:
The required retrieval output that makes a Result Snapshot minimally useful. The Base Result Set remains available even when optional Result Enrichment fails.
_Avoid_: Mothership data, tier 1 data

**Result Item**:
One queryable row in a Result Snapshot. A Result Item may represent a chunk and carry parent-specific fields for grouping or display.
_Avoid_: Result row, hit

**Parent Entity**:
The larger source object that contains or owns a Result Item, such as a document containing a chunk.
_Avoid_: Parent, source document

**Snapshot Schema**:
The set of fields a Result Snapshot exposes for sorting, filtering, grouping, and aggregation.
_Avoid_: View schema, result shape

**Retrieval Filter**:
A filter applied before retrieval completes and before a Result Snapshot exists. Retrieval Filters shape which Result Items can enter the Result Snapshot.
_Avoid_: Initial filter, search filter

**Snapshot Filter**:
A filter applied to an existing Result Snapshot. Snapshot Filters never trigger retrieval from the primary datastore.
_Avoid_: Frontend filter, result filter

**Source Contribution**:
Evidence from a Search Worker that explains why a logical result appears in a Result Snapshot.
_Avoid_: Duplicate result, raw hit

**Search Worker**:
A component that contributes retrieved or derived results to a Search Run.
_Avoid_: Microservice, processor, consumer

**Orchestrator**:
The component that owns Search Run state, schedules ready Workflow DAG steps, and publishes status notifications.
_Avoid_: Worker, dispatcher

**Snapshot Projector**:
A Search Worker that turns worker contributions and enrichments into queryable Result Items in a Result Snapshot.
_Avoid_: Merger, finalizer

**Result Enrichment**:
Additional data or scores attached to retrieved results after initial retrieval. Semantic relevance scoring is one example of Result Enrichment, not a required part of every workflow.
_Avoid_: Use case, decoration, post-processing

**Ad Hoc Enrichment**:
A Result Enrichment requested after a Result Snapshot already exists.
_Avoid_: Follow-up use case, second-phase task

**Enrichment Run**:
One asynchronous execution of an Ad Hoc Enrichment request against existing Result Items in a Result Snapshot.
_Avoid_: Search Run, API request, background job

**Run Scope**:
The resource boundary used to subscribe to async activity notifications, such as a Result Snapshot page receiving Search Run and Enrichment Run updates.
_Avoid_: Subscription topic, channel

**Relevance Score**:
A score from 0 to 10 that estimates how relevant a Result Item is to the user's question. A Relevance Score may be produced by a mocked worker, a reranker, or an LLM-backed evaluator.
_Avoid_: LLM score, ranking score

**Score Explanation**:
An explanation of why a Relevance Score was assigned to a Result Item. Score Explanations may be generated during a Search Run or requested later as an ad hoc Result Enrichment.
_Avoid_: LLM reasoning, score reason

**Worker Command**:
A queue message from the Orchestrator instructing a Search Worker to execute a Workflow DAG step for a Search Run.
_Avoid_: Job payload, task message

**Worker Completion Event**:
A lightweight internal event that reports a Search Worker finished or failed a Workflow DAG step.
_Avoid_: Done message, worker notification
