# API contracts

This directory is the machine-readable HTTP/JSON contract, its examples and content lock.
Use [application CLI](../docs/application-cli.md) for commands and
[current status](../docs/status.md) for runtime availability.

## Versions and sources

| Contract | Version/source |
|---|---|
| HTTP | `/v1`; [OpenAPI 3.1.1](openapi.json) |
| JSON Schema | Draft 2020-12; [schemas](schemas/) |
| Job/status identifier | `compute-connector/v1alpha1` (not silently renamed to repository branding) |
| Result manifest / normalized config | Version `1`; config schema does not imply a TOML loader exists. |
| Bundle | `compute-relay/bundle/v1` |

[contract-manifest.json](contract-manifest.json) maps schemas to fixtures;
[contract.lock.json](contract.lock.json) records exact JSON bytes/hashes. [Examples](examples/)
include positive and deliberately invalid cases. A valid schema is not provider authorization,
verified file bytes or proof that a planned endpoint is enabled.

## Current handler inventory

There are eighteen composable operations. The local host enables all except the optional import
and HTTPS-ingestion components, and starts no provider/collection workers.

```text
GET  /healthz
GET  /readyz
GET  /v1/info
POST /v1/workspaces/{w}/objects
GET  /v1/workspaces/{w}/objects/{object_id}
POST /v1/workspaces/{w}/objects/import       (optional; disabled in local host)
POST /v1/workspaces/{w}/objects/ingest       (optional; disabled in local host)
POST /v1/workspaces/{w}/jobs/validate
POST /v1/workspaces/{w}/jobs
GET  /v1/workspaces/{w}/jobs/{j}
POST /v1/workspaces/{w}/jobs/{j}/cancel
POST /v1/workspaces/{w}/jobs/{j}/retry
POST /v1/workspaces/{w}/jobs/{j}/reconcile
POST /v1/workspaces/{w}/jobs/{j}/collect
GET  /v1/workspaces/{w}/operations/{operation_id}
GET  /v1/workspaces/{w}/jobs/{j}/artifacts
GET  /v1/workspaces/{w}/jobs/{j}/artifacts/{artifact_id}
GET  /v1/workspaces/{w}/jobs/{j}/artifacts/{artifact_id}/content
```

Only `/healthz` is public. Other operations require current workspace/token authority as defined
in OpenAPI. Readiness is local dependency readiness, not a GPU/profile/worker check. Artifact
operations require an explicit `attempt_id` query parameter. No provider logs, public event
stream, quota, cleanup or administrative HTTP service is implied by reserved schema fields.

## Important semantics

Upload/import/ingest require write scope and return 201 after verified byte/ownership commit.
Job creation requires write scope and one 8–256-byte printable non-whitespace ASCII
`Idempotency-Key`. A 202 returns the original durable job/attempt receipt; matching replays use
`idempotency_replay=true`, changed requests conflict. Validation/status use read scope and
neither execute work nor prove provider eligibility.

Control POSTs require operate scope, an explicit attempt and the same key rules. Retry requires
a nonblank non-secret reason. The `/retry` operation is `retry_compute`, preserving the source
attempt and returning a distinct `new_attempt_id`. Control replay uses `replay=true` and the
original receipt; GET reads current state. Cancellation is intent, reconcile observes, collect
requests transfer-only work. Handlers never call providers.

Artifact metadata/pages describe a historical publication. Binary content requires exact bytes
and the final `X-Compute-Relay-Verified` trailer; see [delivery](../docs/artifact-delivery.md).
No schema alone can validate successful stream completion, current authority or retained bytes.

Parsers reject duplicate keys, invalid Unicode, null/case-alias/unknown fields where closed,
trailing values and bounds violations before ambiguous data is lost by ordinary decoding.
Contextual validation also checks budgets, reserved environment names, workspace ownership,
profile policy and path collisions. Result validation binds nonce/input hashes, required outputs
and GPU/phase consistency before [publication](../docs/development/collection.md).

## Changing a contract

Update schema/OpenAPI, positive and negative fixtures, the explicit manifest and content lock
as one reviewed contract change; preserve backwards compatibility or document the break.
Run `go run ./cmd/devtool check` and applicable API/contract tests. Do not edit locks to conceal
a failing schema, or rename stable versions during a documentation cleanup.
