# Spring Boot Java for Phase One

## Summary
Phase one will use Java 21 and Spring Boot so the architecture POC validates the async workflow in the same backend ecosystem as the existing application.

## Context
The project is a POC for an async search and enrichment architecture, but the implementation still needs to be useful for future production decisions. The existing backend is Spring Boot Java, so choosing a different runtime would prove less about whether the architecture fits the system that may eventually ship.

The architecture needs HTTP APIs, SSE, durable Postgres state, RabbitMQ worker dispatch, database migrations, worker processes, and integration tests around real services. Spring Boot has mature support for those concerns through Spring Web, Spring AMQP, Flyway, JUnit 5, and Testcontainers.

## Decision
Use Java 21 and Spring Boot for phase one. Use Maven by default unless the existing backend standard is Gradle. Use Postgres as the durable datastore, RabbitMQ through Spring AMQP for Worker Commands and Worker Completion Events, Flyway for migrations, Docker Compose for local services, JUnit 5 and Testcontainers for tests, and GitHub Actions for CI.

Use Spring Data JPA for ordinary lifecycle persistence where it stays simple. Use explicit SQL, JDBC, or jOOQ-style query construction for Result Snapshot querying if dynamic filters, sorting, grouping, and aggregation become awkward through JPA.

## Considered Options
Considered options included Node.js with TypeScript, a lightweight Fastify-style service, and letting the scaffold agent choose a stack. Those options might be faster for a generic prototype, but they would make the POC less representative of the existing backend environment. Letting the scaffold agent choose the stack would also create unnecessary ambiguity for future agents.

## Consequences
The first scaffold task must create a Spring Boot project rather than evaluating runtime choices. CI should run formatting, static checks, build, and tests for the Java project. Future implementation tickets should preserve RabbitMQ and Postgres in the main architecture and should not substitute in-memory infrastructure for the actual POC behavior.
