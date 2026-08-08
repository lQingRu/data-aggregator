# Runtime Testing Principles

Use this when changing runtime scaffolds, integration tests, containerized test infrastructure, local service orchestration, message consumers, migrations, or CI verification.

## Principles

- Test an observable seam. A boot-only test is not enough unless it also proves something visible at the boundary.
- Prefer real infrastructure for scaffold integration tests when infrastructure behavior matters.
- Keep local environment workarounds visible. Environment-specific flags are not application defaults.
- Green Maven output is not the only completion criterion. Integration test logs should be free of lifecycle errors from services the test owns.
- CI and local commands should run the same verification goals where possible. If CI needs an environment variable, set it in the CI step rather than hiding it in application config.

## Integration Test Seams

Each integration test should assert the behavior it claims to prove. Examples:

- API runtime: make an HTTP request to a real endpoint and assert the response fields.
- Worker runtime: start the worker process/profile against a broker and assert the expected queue or subscription exists.
- Persistence wiring: run migrations against the target database type and assert behavior through startup, endpoint behavior, or a focused database assertion.

To confirm assertions ran, inspect the test reports. For Maven Failsafe, check `target/failsafe-reports/`. The useful signals are:

- the suite has `failures="0"` and `errors="0"`
- the `<testcase name="...">` entry matches the expected test method
- the Maven summary reports the expected number of tests, not only `BUILD SUCCESS`

## Containerized Tests

Use containerized infrastructure for integration tests that need production-like service behavior. Keep container setup in shared test support when multiple tests need the same infrastructure.

For this repo, Testcontainers may need this local command on Docker Desktop environments where Ryuk cannot mount the Docker socket:

```bash
TESTCONTAINERS_RYUK_DISABLED=true ./mvnw -B verify
```

Treat environment flags such as `TESTCONTAINERS_RYUK_DISABLED=true` as environment-specific:

- OK: local shell command, local-only script, or an ephemeral CI runner step that actually needs it.
- Not OK: checked-in application config, test code defaults, or a repo-wide default before the environment has proved it needs the workaround.

When container cleanup helpers are disabled, stale containers may remain after interrupted runs. Check with:

```bash
docker ps -a
```

## Consumer Lifecycle

Tests that start listeners or other consumers must stop them before containerized brokers or services shut down.

Example for Spring AMQP listener tests:

```java
@AfterEach
void stopListenersBeforeContainersShutdown() {
    listenerRegistry.getListenerContainers().forEach(MessageListenerContainer::stop);
}
```

This prevents passing tests from logging shutdown noise after the broker or service starts disappearing.

## CI

CI should include:

- formatting check
- static analysis check
- unit test phase
- full verification phase, including integration tests

If CI runs on GitHub-hosted Linux runners, try Ryuk enabled first. If the runner cannot support Ryuk, scope `TESTCONTAINERS_RYUK_DISABLED=true` to the `verify` step and keep the runner ephemeral.
