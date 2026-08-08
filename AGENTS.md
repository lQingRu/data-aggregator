## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for this repo. See `docs/agents/issue-tracker.md`.

### Triage labels

This repo uses the default five triage labels. See `docs/agents/triage-labels.md`.

### Domain docs

This repo uses a single-context domain docs layout. See `docs/agents/domain.md`.

### Documentation closeout

At task closeout, update docs only when their source of truth changed: README for stable onboarding, `docs/specs` for phase scope or implementation order, GitHub Issues for live status, `docs/adr` for durable decisions, `CONTEXT.md` for domain language, and `docs/agents` for agent operating rules. Do not use README as a live tracker.

### Runtime and integration testing

For runtime scaffolds, container-backed integration tests, message consumers, local service orchestration, or CI verification work, see `docs/agents/testing-and-runtime.md`.
