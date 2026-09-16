# Durable preparation, dispatch and recovery

> **Task:** M3-04, implemented offline; PR #14 merged. M3-05 controls are merged in PR #15.
> M3-06 [verified collection](collection.md) and M3-07 [retention](retention.md) are merged
> in PRs #16/#17. M3-08 [fault qualification](fault-matrix.md) is in review in PR #18.
>
> This is an explicitly composed orchestration component, not a production `serve`
> command or live Kaggle adapter. No workload command runs on the control-plane host.

## From admission to collection

```text
Fenced local claim
  -> freeze pending HTTPS roles; verify bundle and input bytes
  -> resolve the accepted provider/configuration/account snapshot
  -> validate required capabilities without changing the job
  -> commit staging identity + ownership ledger + state/events
  -> one Prepare call; observe private staging readiness
  -> commit submission intent + execution resource + possible activity
  -> one Submit call
  -> persist accepted / rejected / unknown
  -> read-only reconciliation and observation of the same attempt
  -> collecting (not yet succeeded)
```

`internal/dispatch` owns the application state machine and bounded workers. SQLite
migration 5 stores the journal, preparation/execution resource ledger and submission
intents. Journal version and the complete scheduler claim fence are checked in the same
transaction as state and sequenced events. No transaction spans network or file I/O.

`GET` job status returns the cached structured `problem` when present. It never polls a
provider or retries an execution. The response excludes arbitrary diagnostic details,
internal causes, source URLs and credential references. Existing workspace authorization
and the existing job-status schema still apply. M3-04 introduced no new HTTP route;
M3-05 separately adds explicit controls and current-operation reads.

## Inputs and immutable resolution

A pending HTTPS role is downloaded through the existing SSRF-protected ingestion client,
with bounded streaming and optional expected digest. A nil fetcher disables this path.
The verified object metadata and job role are committed together; a failed or uncertain
acknowledgement does not delete the completed blob. Reloading an already frozen role uses
its stored bytes rather than fetching the URL again. Before the first successful role
commit, a retry can still fetch the same source under the original ingestion policy.

Preparation inspects the M2-07 bundle, verifies input lengths/digests and checks the M2-09
runner's supported interpreter, paths, environment and required files. It does not extract
the archive, install dependencies or execute uploaded commands locally. The original
canonical admission request stays unchanged. The provider-facing resolved specification
replaces HTTPS sources with immutable object IDs; no source URL is sent to the adapter.

The input manifest digest uses the runner's sorted ASCII JSON representation of name,
target, byte count and SHA-256. Object IDs and provider staging paths do not alter that
content digest. A golden test covers ordering, empty inputs and HTML-sensitive characters.

`provider.SnapshotRegistry` retains exact profile/revision/instance/account/credential-
reference bindings. It neither overwrites a revision nor falls back to a current alias.
Adapters must implement `BindingVerifier` to verify the effective account before use.
Credential rotation behind the same reference is permissible only for the same account;
a configured account label is not proof of live credential identity. The fake verifier
checks only its fixture binding. No credential value is stored in the journal or plan.

## One-shot mutation gates

| Durable phase | Permitted next behavior |
|---|---|
| `local` | Verify/freeze inputs and validate; commit the first preparation intent. |
| `staging` | Read `ReconcilePreparation`; never call `Prepare` again for that intent. |
| `ready` | Recheck current pause/workspace/account/quota policy; commit the first submission intent. |
| `submitting` | Read `ReconcileSubmission`; never call `Submit` again for that intent. |
| `submitted` | Observe the exact persisted resource/version/identity. |
| `rejected` / `failed` | No automatic new mutation. |
| `attention` | Automatic recovery stopped; retain unresolved evidence for operator inspection. |
| `collectible` | Hand off to the separate artifact verifier; do not infer job success. |
| `prevented` | M3-05 cancelled before submission intent; do not reopen dispatch or infer remote termination. |

Only the invocation receiving a successful **new** begin-intent commit may perform that
mutation. A crash immediately after commit but before the actual provider call therefore
has a conservative outcome: recovery observes the identity and may require manual action,
even when the request never reached the provider. A not-found response is not evidence
that it is safe to repeat a potentially accepted request. Availability is intentionally
sacrificed rather than risking duplicate compute or untracked staging.

`PreparationObserver` must be read-only. Delayed readiness is distinct from upload success.
The adapter's `Prepared.Private` assertion must be backed by its tested private-staging
path; false/unknown stops before submission and preserves the resource for inspection.
Underlying adapters must not hide mutation retries. These contracts and fake tests are
not evidence of any real provider's identity, privacy or retry behavior.

A proven rejection records `ExecutionNotSubmitted` and a terminal local failure, not a
remote command failure. Another compute request requires explicit operator action. A quota
rejection latches account exhaustion without inventing numerical limit/used/remaining
values. A timeout, panic or invalid submission result becomes uncertainty, not rejection.

## Observation, capacity and shutdown

Recovery leases can observe existing work while new dispatch is paused or an account is
disabled. The worker checks generation, owner, random fence, expiry, queue identity,
account binding, attempt revision and journal version before each mutation of local state.
Lease expiry or local shutdown never clears a submission intent or possible remote activity.
Capacity remains reserved until evidence establishes that execution is inactive.

Observations must match the exact resource/version and cannot regress previously confirmed
state. Old/future timestamps are rejected. An unknown poll does not erase earlier running
or active evidence. Five consecutive unresolved recovery outcomes stop at
`needs_attention`; successful nonterminal observations can continue while the runtime runs.
Private staging reported as still processing may likewise continue to be observed.

Terminal execution evidence opens collection with `result=not_available`. The separately
composed M3-06 collector validates the result manifest and required outputs before publishing
verified availability and final orchestration state. Hardware-release precision remains
whatever the adapter can observe; it is not inferred from local time or transfer completion.

Defaults are four workers, one-minute control calls, five-minute preparation invocations
and a 15-second poll delay. Failure backoff doubles to a 120-second base ceiling, followed
by reproducible per-attempt jitter of up to 30 percent. Policy blocking uses a five-minute
base recheck. These are local invocation/poll budgets, not measured provider guarantees or
an overall provider-session deadline. Remote wall/setup/finalization budgets remain frozen
in the job and are enforced through the runner and verified provider timeout path.

Callbacks must cooperate with cancellation. The fixed worker pool does not spawn replacement
workers for callbacks that ignore cancellation, and does not claim shutdown completed before
they return. Stopping this engine does not call provider cancellation or cleanup.

## Explicit control integration

M3-05 stores cancellation intent separately from termination evidence. Before submission
intent it can commit `prevented` with state/events and retain any existing staging ledger.
After submission may have started, only a new committed cancellation intent authorizes one
capability-verified call against the exact frozen binding. Lost acknowledgement or restart
does not repeat cancellation, clear remote capacity or use cleanup instead.

Explicit reconcile requests only observation of the recorded identity; permanent
identity/privacy failures cannot be rearmed into new staging/submission. Compute retry
creates a distinct attempt with the same verified frozen inputs and binding, never resets
the existing journal. Collection acceptance creates a durable transfer-only ticket; it does
not start a verifier inside the request. See [operations](operations.md) for HTTP, receipt
and completion-race semantics, and [collection](collection.md) for the consumer and recovery.

M3-07 retention assesses all historical/shared references and the journal's recovery needs.
Active, unknown, incomplete or held work remains pinned even after ordinary retention windows
or nominal lease expiry. The local sweeper cannot clear an intent or repair a dispatch
journal. Remote cleanup preview checks exact ownership and terminal/publication evidence;
it never applies deletion or substitutes for cancellation. Staging preview is unavailable
through the execution-only cleanup port. See [retention](retention.md).

## Verification

With the pinned repository toolchain:

```text
go test -race ./internal/dispatch ./internal/provider/... ./internal/store/sqlite ./internal/api
go test -run=TestDispatch ./internal/store/sqlite
go run ./cmd/dispatchsmoke
```

The finite smoke uses actual auth/admission, SQLite and filesystem blobs with a fixture-only
provider. It verifies 983,040 input bytes, one preparation, one simulated submission with a
lost response, reopen/reconciliation of the same attempt/nonce, and a terminal observation
that stops at collection. The fake backend stays in memory across local database reopen;
this is not a live provider or a provider-process crash test. No payload is executed.

Separate fault tests kill actual Go child processes before/after staging and submission
SQLite commits. The child performs no provider call. Other tests inject lost commit
acknowledgements, genuine SQLite disk-full rollback, resource/event/journal insert failures,
twenty competing submit gates, stale ownership, mutable URLs, wrong ownership/privacy and
provider response loss/panic. Schema-4 upgrade preserves identity/queue/barriers and does not
invent remote evidence. API tests validate the actual condition response against the schema.

Local engineering used Go 1.23.2 and system SQLite 3.46.1 in a non-shipped isolation harness:
a database/sql adapter to a Python SQLite subprocess plus reduced supporting fixtures.
Targeted vet/race/repeat/fuzz and a compiled SQL fault-test executable passed. This is not
production modernc, full auth/domain/archive/schema integration, the real schema upgrade,
or local execution of `cmd/dispatchsmoke`. Exact Go 1.27.1/modernc integration, the smoke
component and native tests/build are checked by the existing offline CI. PR #14 records
exact commands, source identities and final-head check results; no workflow was added.
M3-05's additional control/receipt/race/HTTP evidence is recorded separately in PR #15 and
the operations guide; the historical M3-04 local harness is not relabeled as that evidence.
M3-06's real-blob collection, publication and recovery tests are recorded in PR #16 and the
collection guide; the original dispatch smoke still ends at its collection handoff.

M3-08 [qualification](fault-matrix.md) adds targeted regressions for unknown/modified state,
stale polls, active restart, a local observation deadline, frozen-account recovery, quota
rejection and a nonzero helper-process exit after synthetic acceptance. The fresh named-test
checker is part of `devtool check`. These tests preserve one-shot gates; they do not implement
an overall-job deadline service or establish official-CLI/live-provider compatibility.

## Remaining boundaries

The ledger is recovery/ownership evidence, not an unconditional cleanup authorization.
Durable cancellation, explicit compute retry/reconcile and collection tickets have M3-05
offline components. M3-06 adds verified collection/publication; M3-07 supplies conservative
retention, exact local byte deletion and remote dry-run previews only. Production CLI/config,
artifact HTTP, remote apply and live Kaggle integration retain their separate gates.
Do not manually reset journal phases, delete intent rows or clear the scheduler barrier to
resume an uncertain attempt. Use [recovery semantics](recovery.md) for the safe boundary-specific
actions and the [implementation plan](implementation-plan.md) for the current review gate.

See [ADR-0011](decisions/0011-one-shot-mutations-and-recovery.md),
[the scheduler guide](scheduler.md), [storage](storage.md), [operations](operations.md),
[collection](collection.md), [retention](retention.md) and
[the approved recovery requirements](proposal.md#13-persistence-idempotency-and-crash-recovery).
