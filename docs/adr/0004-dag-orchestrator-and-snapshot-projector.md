# DAG Orchestrator and Snapshot Projector

## Summary
Search Workflows will be represented as typed Workflow DAGs in code. The orchestrator owns DAG advancement and status; workers execute steps; the Snapshot Projector turns contributions and enrichments into queryable Result Items.

## Context
The prototype needs to prove dependencies such as semantic retrieval before mocked relevance scoring, and all required retrieval before projection. It also needs to support optional enrichment failure without losing the Base Result Set. We decided to model workflow execution as a DAG rather than hardcoding worker-to-worker chains.

The Workflow DAG lives in code for V1 because the project does not expect many workflows initially and typed definitions are easier to refactor during prototyping. A single-step async flow is still a valid DAG. The orchestrator is not a data worker; it creates or advances Async Runs, schedules ready DAG steps, records state, emits notifications, and applies retry/cancellation/supersession rules.

Workers do not schedule dependent workers directly. A worker writes durable step state and outputs, then publishes a Worker Completion Event. The orchestrator receives that event, evaluates the DAG, and publishes newly unblocked Worker Commands. This centralizes dependency logic and keeps retries, cancellation, and warnings coherent.

## Decision
Worker outputs use idempotent keys. Contributions use deterministic keys such as `search_run_id + workflow_step_id + result_item_key`, and logical step state uses `search_run_id + workflow_step_id`. The Snapshot Projector runs after required upstream nodes are complete. V1 does not expose progressive result querying, so projection can run once to create a stable queryable snapshot.

Hybrid Chunk Search is the first workflow. It includes lexical retrieval, semantic retrieval, mocked relevance scoring, projection, status notifications, and snapshot querying. Lexical retrieval and projection form the Base Result Set path. Semantic retrieval and mocked scoring are enabled for the prototype; for product-like resilience they may complete with warnings if optional, while stricter DAG tests can require them.

## Considered Options
Considered options included direct worker chaining, a database trigger model, external YAML workflow definitions, and making the orchestrator also perform projection. Direct chaining would distribute dependency knowledge. Database triggers would hide control flow. External config is premature for the expected workflow count. Combining orchestration and projection would blur state control with data shaping.

## Consequences
The orchestrator becomes a critical component and must be designed for multiple instances in the future. Advancing a Search Run should use DB row locks or an equivalent concurrency guard. This is more structure than a simple queue consumer, but it tests the architecture the project is meant to explore.
