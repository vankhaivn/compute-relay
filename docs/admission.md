# Durable local job admission

> **Task:** M3-02; implemented offline, PR #12 in review.
>
> **Scope:** local acceptance and cached status, not dispatch, GPU allocation or a
> production `serve` command. M3-01 supplies the SQLite/workspace/token/object foundation.

## HTTP boundary

| Route | Authority | Meaning |
|---|---|---|
| `POST /v1/workspaces/{w}/jobs/validate` | Workspace `read` | Schema, semantics, object metadata and local profile-policy checks; no job creation. |
| `POST /v1/workspaces/{w}/jobs` | Workspace `write` | Commit an immutable job and first attempt before returning `202 Accepted`. |
| `GET /v1/workspaces/{w}/jobs/{j}` | Workspace `read` | Read durable cached state; never poll a provider or execute work. |

These routes are enabled only when the composition root supplies `api.Config.Jobs` with an
admission service backed by the SQLite repository. A nil service has no in-memory fallback.
Existing bearer, workspace, Host/Origin, body, deadline and rate limits remain in force.
The root API remains planned until production CLI/configuration and orchestration exist.

Creation requires exactly one `Idempotency-Key`: 8–256 printable ASCII bytes, excluding
whitespace. JSON is bounded to 1 MiB, including its stored canonical representation. The
response follows `api/schemas/job-admission.v1alpha1.schema.json` and includes `Location`.
A replay uses the original IDs, timestamp and `queued` receipt with
`idempotency_replay=true`; use GET for current state. Reserved events/attempts/artifact
links are contract locations, not implemented collection routes in this task.

## Acceptance transaction

```text
Strict parsing and request hash
  -> begin bounded local SQLite transaction
  -> recheck current token, expiry, revocation, workspace and scope
  -> replay matching workspace + job.create + key, or reject changed request
  -> resolve current allowed profile revision for a NEW job
  -> check/pin owned bundle and object-input metadata
  -> apply outstanding-job limits
  -> insert job + attempt 1 + random nonce + object references
  -> append job.accepted at sequence 1
  -> insert original receipt and idempotency identity
  -> commit
  -> return 202
```

Job, attempt, references, event and idempotency are all-or-nothing. An error or uncertain
commit acknowledgement returns no successful IDs and is not automatically retried. The
client can resubmit the same key/request to discover the committed result. This does not
make future provider submission exactly-once.

The idempotency primary key includes workspace and operation. Only a key digest is stored;
keys are not credentials and must not contain secrets. Records have no independent expiry
or deletion API; future retention must keep them at least as long as job metadata. Restoring
an older database also restores its older admission history, not receipts written later.

## Canonical request identity

The explicit format version is `compute-relay/job-request/v1`, not an RFC 8785 claim.
SHA-256 covers that version, a NUL separator and canonical JSON. Object-key order,
insignificant whitespace, equivalent string escapes and integer spellings such as `1`,
`1.0` and `1e0` do not change identity. Array order, string contents and optional-field
presence do: omitted `labels` and `labels:{}` are different requests.

All numeric fields in this job contract are integers. Parsing avoids float rounding,
bounds exponent and depth, rejects duplicate decoded keys, invalid UTF-8/unpaired Unicode
surrogates, trailing JSON, unknown fields and oversized canonical expansion. The existing
pinned JSON Schema validator loads only embedded repository schemas with external loading
disabled. No dependency version or public job schema is replaced.

Semantic checks also reject overlapping input/output paths, duplicate input names, NUL
command arguments, reserved control/environment variables, and phase budgets leaving no
payload time. Plain environment values remain explicitly non-secret; validation is not a
universal secret scanner or a sandbox for hostile workload code.

## Frozen resolution and inputs

Local operator `Store.PutProfile` creates immutable non-secret revisions containing the
provider binding, account scope, optional credential reference, cost class and policy
bounds. Changing the contents of an existing revision fails; a new revision affects only
future admissions. No credential reference is dereferenced during this task.

A matching replay is checked before current profile remapping, enablement and queue limits.
It retains the originally accepted binding even after a profile is disabled. It still
requires current token/workspace authority: revocation is not bypassed by an existing key.

Object sources are workspace-scoped and pinned by foreign keys with recorded sizes and
digests. This is metadata admission, not proof that bundle framing, every referenced byte
or the remote environment has been verified. Preparation must inspect/revalidate those
bytes before dispatch. SQL guards reject changes to referenced object metadata.

Direct HTTPS sources remain in the immutable canonical request and are explicitly pending;
admission performs syntax/policy checks only, not DNS or downloading. Future preparation
must freeze the first permitted snapshot once and use it for all attempts. Until that path
is composed, callers may use the existing explicit ingestion endpoint and submit its object
ID. Neither validation nor `202` claims `inputs.ready`. Source URLs, commands, nonce and
credential references are not exposed in the current status response or ordinary errors.

Validation reports `before_dispatch` checks and GPU `verify_after_start` requirements. It
is not a provider capability/eligibility result. Default bounds are 100 outstanding jobs per
workspace and 1,000 globally, conservatively counting every nonterminal active attempt.
This prevents unbounded acceptance; it is not M3-03 scheduling or account-capacity policy.

## State/event persistence and failures

The existing `ports.Store` now has a SQLite implementation. `CommitAttempt` validates the
transition, compares the stored revision/identity, and commits the updated state with the
next per-job event sequence. A stale revision, duplicate event or failed insert rolls back
both sides. Operation-linked events await durable operations in M3-05.

Tests cover concurrent replay, workspace isolation, missing/foreign objects, profile
remapping, revocation, limits, failure at each insert, database-full rollback, real process
kill before/after commit and HTTP response-writer failure. A lost response never turns into
a second locally accepted job or attempt. Migration 3 appends to M3-01; earlier migration
bytes/checksums remain unchanged. Older binaries must reject the newer schema.

## Developer verification

With the pinned repository toolchain:

```text
go test -race ./internal/admission ./internal/store/sqlite ./internal/api
go test -count=10 -run='TestAdmission(Replay|Concurrent|Atomic|Pins|Limits)' ./internal/store/sqlite
go run ./cmd/admissionsmoke
```

The finite smoke uses temporary SQLite and synthetic metadata. It checks eight concurrent
replays, a changed-request conflict, restart replay and one queued attempt. Its report
states `metadata_only=true`, `blob_bytes_checked=false` and zero provider/payload calls.
It is a developer command, not a production service.

Local engineering evidence was collected on Go 1.23.2 in an out-of-tree isolation harness
because exact toolchain/module downloads were unavailable. That harness used system SQLite
through a non-shipped CGo adapter, a Python Draft 2020-12 bridge and reduced supporting
fixtures. Its vet/race/repeat/HTTP/smoke results are not production-modernc or full-repository
evidence. The standalone standard-library canonicalizer also passed race tests and 37,096
fuzz executions. An earlier broad 25-repeat run exceeded the local budget and is not claimed
as passing. Full integration and native builds are checked separately by the existing
Go 1.27.1 offline CI; PR #12 records final-head results. No harness or replacement directive
is shipped.

See [ADR-0009](decisions/0009-durable-idempotent-admission.md), [storage](storage.md) and
[API contracts](../api/README.md). Scheduler, input-preparation orchestration, submission
intents, control operations and production CLI composition retain their own task gates.
