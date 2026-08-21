# API Design and Documentation

Apply these rules when creating or modifying externally consumed APIs.

## API contracts must be documented

When an HTTP API is introduced or changed, update the API specification as part of the same change.

Prefer OpenAPI/Swagger when the framework and project support it.

The API specification should describe:

- endpoint path and HTTP method
- purpose of the endpoint
- request parameters
- request body schema
- response schemas
- relevant HTTP status codes
- authentication requirements
- validation constraints
- meaningful error responses

Do not consider an API change complete if the implementation and documented contract disagree.

## Prefer generated documentation from typed contracts

Where practical, derive OpenAPI schemas from the same typed models or schemas used by the application.

Avoid independently maintaining equivalent:

- runtime validation schemas
- application types
- API documentation schemas

when they can share a source of truth.

The goal is to reduce contract drift.

## Document errors explicitly

Do not document only the successful response.

Include meaningful failures such as:

- `400` invalid input
- `401` unauthenticated
- `403` unauthorized
- `404` resource not found
- `409` conflict
- `422` validation failure where applicable
- `429` rate limiting
- `5xx` service failures where externally meaningful

Use the conventions already established by the repository.

## Use stable response contracts

Do not expose arbitrary internal objects directly as API responses.

Define explicit request and response models.

Avoid leaking:

- database entities
- ORM models
- internal exception structures
- framework-specific objects
- implementation-only fields

## Validate at the boundary

All externally supplied values must be validated before entering trusted application logic.

Where possible, use schemas that provide both:

1. runtime validation
2. static type information

## Use appropriate HTTP semantics

Use HTTP methods and status codes according to their intended meaning.

Do not use `200 OK` for every outcome merely for convenience.

Prefer predictable resource-oriented APIs unless the domain clearly calls for command-style endpoints.

## Consider idempotency

For endpoints that may reasonably be retried, determine whether duplicate requests can cause incorrect behavior.

Pay particular attention to:

- payments
- order creation
- webhook handling
- job submission
- external side effects

Use idempotency mechanisms where required.

## Pagination

Collection endpoints that may grow significantly should support bounded pagination.

Do not introduce endpoints that can unintentionally return an unbounded dataset.

Where appropriate, document:

- page/cursor parameters
- limits
- continuation tokens
- sorting behavior

## Versioning and compatibility

Avoid breaking existing API consumers without an explicit requirement.

When changing an existing public contract, consider:

- backward compatibility
- optional additive fields
- deprecation
- versioning
- migration strategy

## Examples

For non-trivial endpoints, include useful request and response examples in the OpenAPI documentation when supported.

Examples should demonstrate realistic usage rather than placeholder values.

## Completion criteria

When changing an API, verify:

- implementation and documented contract agree
- request validation exists
- response types are explicit
- relevant failure cases are represented
- authentication and authorization are enforced where required
- tests cover important behavior
- OpenAPI/Swagger output remains valid
