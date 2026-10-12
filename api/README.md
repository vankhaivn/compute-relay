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

Every serve mode composes the routes below except import and ingest, which stay disabled in the
local host; managed mode adds the routes in the next section. Handlers never dispatch provider
work, and log reads are observational.

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
GET  /v1/workspaces/{w}/jobs/{j}/logs
POST /v1/workspaces/{w}/jobs/{j}/cancel
POST /v1/workspaces/{w}/jobs/{j}/retry
POST /v1/workspaces/{w}/jobs/{j}/reconcile
POST /v1/workspaces/{w}/jobs/{j}/collect
POST /v1/workspaces/{w}/jobs/{j}/cleanup
GET  /v1/workspaces/{w}/operations/{operation_id}
GET  /v1/workspaces/{w}/jobs/{j}/artifacts
GET  /v1/workspaces/{w}/jobs/{j}/artifacts/{artifact_id}
GET  /v1/workspaces/{w}/jobs/{j}/artifacts/{artifact_id}/content
```

Only `/healthz` is public. Other operations require current workspace/token authority as defined
in OpenAPI. Readiness is local dependency readiness, not a GPU/profile/worker check. Artifact
and log operations require an explicit `attempt_id` query parameter. A public event stream is
not implied by these reads, and no read or preview deletes remote resources.

## Optional managed extension

[Managed connections](../docs/development/managed-connections.md) defines these provider
descriptor, connection and attempt-authorization routes. Managed serve composes
them and advertises `managed_connections` and `attempt_authorization` in runtime info. They keep
the existing job envelope through immutable selection profiles and separate `manage`/`execute`
scopes. An absent `selection.accelerator` on an older selection means unreported, not the current
descriptor's default.

```text
GET  /v1/workspaces/{w}/providers
GET  /v1/workspaces/{w}/connections
GET  /v1/workspaces/{w}/connections/{connection_id}
POST /v1/workspaces/{w}/connections
POST /v1/workspaces/{w}/connections/{connection_id}/actions
GET  /v1/workspaces/{w}/connection-operations/{operation_id}
POST /v1/workspaces/{w}/jobs/{j}/authorize
GET  /v1/workspaces/{w}/jobs/{j}/authorizations/{authorization_id}
POST /v1/workspaces/{w}/jobs/{j}/prepare
GET  /v1/workspaces/{w}/jobs/{j}/readiness
POST /v1/workspaces/{w}/jobs/{j}/start
GET  /v1/workspaces/{w}/jobs/{j}/gate-authorizations/{authorization_id}
POST /v1/workspaces/{w}/jobs/{j}/workload-secrets
GET  /v1/workspaces/{w}/jobs/{j}/workload-secrets/{authorization_id}
POST /v1/workspaces/{w}/jobs/{j}/workload-secrets/{authorization_id}/revoke
```

Optional `execution_control=prepare_then_start` uses separate finite staging and compute
authorities on providers advertising `separate_prepare_start`. Gate POSTs and receipt reads need
`execute` scope; readiness needs `read`. See [preparation gates](../docs/preparation-gates.md).

Managed SSH providers may also advertise `ephemeral_workload_secrets`. Declare slot names and
exposure modes on the gated job, then supply values after readiness is `ready`; all three secret
routes need `execute` scope, and writes require an `Idempotency-Key`. The start request binds the
returned `secret_authorization_id`. Values are base64 on the wire and are never part of durable
job or authorization receipts; see [workload secrets](../docs/workload-secrets.md).

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
requests transfer-only work. Cleanup (feature `job_cleanup`) requests deletion of one settled
attempt's remote resources; uncollected results need `discard_uncollected=true`. Its receipts use
kind `cleanup` and settle as `remote_deleted`, `already_absent` or `failed` only after a provider
worker applies them; see [controls](../docs/development/operations.md#remote-cleanup). Optional JobSpec `result_collection=manual` freezes explicit result
collection; omission keeps automatic behavior. Check the runtime's `manual_result_collection`
feature before admitting manual jobs. Additive status `collection` separates terminal waiting,
pending/active transfer, verification and published availability; [collection](../docs/development/collection.md)
defines dated output-byte progress and restart generations. While the active attempt is
`preparing`, additive status `preparation.progress` reports frozen input bytes an adapter has
handed to its provider upload (`staged_input_bytes`); it is absent when the adapter cannot observe
its upload and never means provider readiness.

## Reading attempt logs

Check authenticated `/v1/info` for `job_logs`, then request:

```text
GET /v1/workspaces/{w}/jobs/{j}/logs?attempt_id={original_attempt}&limit=100&cursor={opaque_cursor}
```

Use current read authority and an explicit original attempt, including historical attempts.
Omit `cursor` on the first request; `limit` defaults to 100 and accepts 1–100. Duplicate or unknown
query parameters and request bodies are rejected. The response contains `source`, `availability`,
`lines`, `next_cursor` and `truncated`, alongside exact workspace/job/attempt IDs. Runtime bounds
are 100 lines, 16 KiB per line, 256 KiB per page, a 512-byte cursor and a 10-second read deadline.
Treat lines as untrusted text and preserve a returned cursor even on an empty live page.

A legitimate pre-submit attempt or provider without log support returns `unavailable` with no
lines or cursor. HTTP 400 rejects invalid/foreign cursors; HTTP 409 `LOG_CURSOR_RESET` requires an
explicit restart with an empty cursor and a visible continuity gap. Pagination cannot recover
discarded output. EOF, `after_completion` and render messages do not replace job status or
verified result publication. Reads revalidate authority before releasing bytes and cannot create,
authorize, retry, reconcile, collect or cancel compute. Closing a reader leaves execution running.
HTTP 503 can report a fixed `log read unavailable: <reason>` message for operator diagnosis:
authentication/kernel reads, stream timeout, HTTP status class, redirect, invalid log format or
unavailable replay. These categories contain no provider response text, headers, URLs or secrets.
Preserve the last verified cursor after an unavailable read; these failures do not permit another
compute attempt. Local-admission-only mode returns unavailable pages without provider work.

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
