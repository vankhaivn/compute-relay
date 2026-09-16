# Durable attempt-scoped controls

> **Task:** M3-05, implemented offline; PR #15 merged.
>
> **Scope:** explicit cancellation, compute retry, reconciliation, collection tickets and
> durable operation status. M3-06 adds the separately composed [collector](collection.md),
> and M3-07 adds [retention and cleanup previews](retention.md). Production `serve`, artifact
> HTTP routes, live Kaggle and remote cleanup apply remain separate gates.

## HTTP and composition boundary

The composition root supplies `api.Config.Operations` with `internal/operations.Service`
backed by SQLite. A nil service disables these routes; it does not use an in-memory fallback.
The service, dispatch engine and collection engine are composed separately. HTTP handlers
never call a provider or run collection transfers.

| Route | Required workspace scope | Durable effect |
|---|---|---|
| `POST /v1/workspaces/{w}/jobs/{j}/cancel` | `operate` | Persist intent, prevent new dispatch when possible, otherwise request one-shot cancellation. |
| `POST /v1/workspaces/{w}/jobs/{j}/retry` | `operate` | Explicitly create one new attempt from the original verified frozen inputs. |
| `POST /v1/workspaces/{w}/jobs/{j}/reconcile` | `operate` | Request observation of existing identity; never repeat submission. |
| `POST /v1/workspaces/{w}/jobs/{j}/collect` | `operate` | Create a transfer-only ticket after terminal execution evidence. |
| `GET /v1/workspaces/{w}/operations/{operation_id}` | `read` | Read the current durable revision without provider polling or new work. |

Every POST requires exactly one `Idempotency-Key`: 8–256 printable ASCII bytes without
whitespace. The JSON body must explicitly name `attempt_id`; there is no implicit active
attempt. The body is limited to 4,096 bytes and optional `reason` to 512 UTF-8 bytes.
Retry requires a nonblank reason. Unknown fields, duplicate keys, null values, control
characters and invalid UTF-8 are rejected. Reasons are non-secret audit metadata, not
commands or provider instructions, and are never returned in the public response.

Existing bearer, workspace, Host/Origin, rate, body and deadline guards still apply. A
valid key never bypasses current token expiry/revocation or workspace authority. An absent
or foreign resource returns the same not-found result within the authorized workspace.

The request shapes are intentionally small:

```json
{"attempt_id":"att_original"}
```

```json
{"attempt_id":"att_original","reason":"Explicit retry of the original frozen inputs"}
```

These are request examples, not runnable production CLI commands. The second shape is
required for `/retry`. Use IDs from the actual job, never the illustrative IDs above.

## Receipt versus current operation

A successful POST returns `202 Accepted`, an operation view and `Location`, even when a
purely local effect has already completed in the transaction. The source `attempt_id`
remains explicit; retry additionally returns a distinct `new_attempt_id`.

The response includes `kind`, `status`, `revision`, timestamps, `effect`,
`remote_termination_confirmed`, `replay`, a null or sanitized structured `problem`, and
workspace-scoped `links.self`/`links.job`. No reason, credential reference, request hash,
provider resource ID, arbitrary diagnostic details or internal cause is serialized.

Idempotency is scoped by workspace and operation kind. The canonical request identity
includes the job, source attempt and reason. A matching key/request replays the original
committed acceptance receipt with `replay=true`, not a newly computed outcome. A changed
request with that key returns `409 IDEMPOTENCY_CONFLICT`. GET follows `Location` to read the
current revision with `replay=false`; it does not mutate the immutable acceptance receipt.

After an HTTP response is lost or commit acknowledgement is uncertain, preserve the same
key and request. Replaying them discovers an existing commit without making another
attempt. Do not substitute a new key or infer failure from the missing response. Replay is
checked before mutable target/input checks but after current authorization, so later state
changes do not destroy a valid receipt or revive revoked authority.

## Persistence and race boundaries

Migration 6 adds durable operation records, immutable receipts, scoped idempotency,
per-attempt cancellation/retry uniqueness, collection tickets and operation-linked events.
Earlier migration bytes, installation identity, job specifications, frozen inputs and
prewritten staging/submission identities are preserved. State, operation, events and any
new attempt/queue row commit together or roll back together.

Operations do not bypass the scheduler fence or reset one-shot mutation gates. Local
metadata transactions contain no network or blob reads. Retry verifies frozen local bytes
outside SQL, then rechecks authority, eligibility and the original snapshot in the commit.
Concurrent cancellation, completion, revocation or retry therefore cannot rely solely on
an earlier read. Only one retry can create a new attempt from a source attempt; a second
new key conflicts rather than creating another execution. Cancellation coalesces on the
source attempt instead of issuing repeated remote cancellation requests.

## Cancellation and observation

Before submission intent, cancellation can atomically prevent dispatch. The receipt says
`dispatch_prevented`; this is not evidence that a remote process was terminated, so
`remote_termination_confirmed` remains false. A staging resource may still require later
ownership-safe cleanup; cancellation never deletes it.

After submission may have occurred, the worker uses the exact frozen provider/account
binding and a verified cancellation capability. It commits a one-shot cancellation intent
before invoking the adapter. Lost acknowledgements, restart after intent, unsupported or
unverified capability and ambiguous outcomes are not invitations to repeat the mutation.
They remain unresolved/manual-required with possible remote capacity retained.

Acknowledgement means cancellation was requested or accepted, not that execution stopped.
Matching terminal observation is required for `cancellation_confirmed`. Completion that
wins the race remains completion; cancellation may be `too_late`. A terminal operation is
not rewritten later: use the attempt's current state for subsequent execution evidence.
Stopping the local worker or runtime is not remote cancellation.

Reconcile observes the existing staging/submission/execution identity without another
Prepare or Submit. A previously resolved terminal attempt can return a cached
`observation_refreshed` outcome without network I/O. Permanent identity/privacy failures
cannot be rearmed into unsafe work. Unknown observations never manufacture success or
clear possible-activity capacity merely because a local lease expired.

## Explicit compute retry

Retry requires a terminal eligible source attempt and no unresolved/possible/active remote
execution. A successful execution whose artifacts are missing must use collect, not compute
retry. The original bundle and input bytes must still be present and match the frozen pins;
missing, changed or expired inputs are not silently refetched from URLs.

M3-07 checks exact unexpired input inventory before local byte reads and again inside the
retry transaction. Bytes still awaiting physical sweep are not reusable after their tombstone.
A pre-expiry proof cannot bypass the commit check. Original control receipt replay remains
before these mutable checks, so expiry does not reinterpret a previously accepted request.

A successful retry atomically records a new attempt ID, nonce, queue entry and history event.
The job specification, provider/profile revision, input pins and source attempt history are
unchanged. New work still passes the scheduler and one-shot dispatch gates. The operation
succeeding means the new local attempt was created, not that its remote execution succeeded.

## Collection is a transfer-only handoff

Collect requires a specific attempt with matching persisted terminal execution evidence.
It creates a durable `accepted` / `collection_requested` ticket. It does not enqueue a
compute retry, refresh source inputs, submit a provider job or publish artifacts.

M3-06 supplies the separately composed ticket consumer, strict result verification and
atomic artifact publication. The collector uses one immutable result snapshot per attempt
and a fenced transfer lease. It publishes the complete metadata set and operation outcome
only after every selected blob is independently verified. `results_available` is not
fabricated by the control handler. Expired results are not recovered by rerunning compute.

When no collector is running, tickets remain pending. An interrupted accepted ticket can
be reclaimed after lease expiry; a committed failed ticket requires a new explicit collect
request/key for the same attempt. Replaying the old key still returns its original receipt.
Neither recovery path refreshes an already pinned result. See [collection](collection.md)
for limits, scoped result reads, directory semantics and failure evidence.

M3-07 expiry preserves publications and historical operation receipts while recording
`result.expired` with the attempt's changed availability. An old succeeded collect operation
is historical evidence, not a claim that its bytes are still retained; inspect current job
result state. Pending operations, held callbacks and unresolved collection work remain pins.
Remote cleanup preview is a separate operate-authorized read-only service, not a fifth
control mutation or a remote apply command. See [retention](retention.md).

## Error handling and operator recovery

Malformed requests, unsupported content types and oversized bodies return 400, 415 and 413
respectively. State, identity, inappropriate retry/collect and idempotency conflicts return
409. Store failures or ambiguous local acknowledgement return conservative structured
errors rather than success IDs or a blanket safe-to-rerun flag.

Read `safe_operation_retry`, `compute_may_have_started` and `recommended_action` independently.
For unresolved execution, inspect/reconcile the recorded identity; do not reset a journal,
delete an intent, clear capacity manually or treat resource deletion as cancellation.
Operation, execution, result availability and hardware-release evidence are distinct.

Database-only restore preserves only the receipts present in that snapshot. It neither
stops remote work nor restores missing blob bytes. Never activate original and restored
installations concurrently against the same provider identity. See [storage](storage.md).

## Verification and evidence

With the repository's pinned Go 1.27.1 toolchain and locked dependencies:

```text
go run ./cmd/devtool check
go run ./cmd/devtool test-race
go test -race ./internal/operations ./internal/dispatch ./internal/store/sqlite ./internal/api ./internal/contracts
```

Tests cover immutable receipts, concurrent retries, restart/lost acknowledgements,
cancellation/completion and staging races, event rollback, authority revocation during input
reads, unresolved retry rejection, exact binding and collection gating. Real-loopback HTTP
with temporary SQLite checks replay, current GET, scopes/revocation, strict bodies and
redaction. Serializer/schema tests compare 880 kind/status/effect/new-attempt/termination
combinations against the record validator; actual HTTP responses also validate against
published contracts, including `/v1/info`.

Local engineering had Go 1.23.2 and unavailable pinned-toolchain/module downloads. Local
formatting and Python Draft 2020-12 fixture/truth-table checks were run; no full local Go
1.27.1/modernc or local operation smoke result is claimed. PR #15 records exact-head offline
CI results for quality/race tests and native Linux/macOS/Windows tests/CGo-free builds.
CI is not an operator runtime and uses no Kaggle credentials, GPU or live-provider probes.
M3-07's expiry, stale-proof and preserved-receipt tests are recorded separately in PR #17.

M3-08's [fault matrix](fault-matrix.md) qualifies cancellation/completion races, missing remote
identity, explicit transfer recovery and preserved uncertainty with fresh named-test evidence.
[Recovery semantics](recovery.md) explains how to choose among receipt replay, reconcile,
collect, explicit compute retry and cancellation without interpreting an operation's success
as proof of remote termination or currently retained bytes.

See [ADR-0012](decisions/0012-attempt-scoped-durable-controls.md), [API contracts](../api/README.md),
[dispatch](dispatch.md), [collection](collection.md) and [retention](retention.md). The
[implementation plan](implementation-plan.md) owns the current owner-review/next-task gate.
Remote apply, artifact HTTP/CLI and production composition remain separate work.
