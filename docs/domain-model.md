# Provider-neutral domain model

> **Task:** M2-02 — domain model and truthful state semantics.
>
> **Status:** implemented offline for review. Persistence, HTTP schemas, provider ports,
> and provider adapters are separate tasks.

The `internal/domain` package defines the provider-neutral language used by application,
orchestration, persistence, API, and provider-boundary work. It imports no Kaggle code and
contains no transport, database, filesystem, subprocess, or credential behavior.

## Boundary

The package owns:

- opaque typed identifiers and canonical SHA-256 digests;
- immutable job identity and resolved provider-instance/profile snapshots;
- explicit attempts, operations, and sequenced events;
- capability support and evidence levels;
- stable error categories/codes and recovery recommendations; and
- validation and monotonic transition rules for independent attempt-state dimensions.

Infrastructure packages will translate provider responses and persisted rows into these
values. They must not bypass the validation rules by assigning optimistic terminal states.

## Independent attempt-state dimensions

One scalar status cannot describe a distributed execution truthfully. `AttemptState` keeps
these dimensions separate:

| Dimension | Question answered |
|---|---|
| Orchestration | What is the local runtime doing? |
| Execution | What has provider or runner evidence established? |
| Result | Are verified local results available, incomplete, invalid, or expired? |
| Cancellation | Was cancellation requested, accepted, confirmed, too late, or manual? |
| Remote activity | Is remote compute not started, possible, active, inactive, or unknown? |
| Release evidence | What is known about accelerator release/accounting? |
| Deadline | Has a relevant deadline been exceeded? |

This separation preserves cases such as:

- remote execution succeeded while artifact collection failed;
- cancellation was requested but completion won the race;
- a submission response was lost and compute may still exist;
- a successful result later expired under retention policy; and
- provider terminal execution is known while exact hardware release is not observable.

## Transition invariants

Transitions are centralized and value-based. They reject:

- moving a terminal orchestration or execution state backward;
- clearing deadline evidence after it has been recorded;
- reporting success without successful execution and verified results;
- reporting cancellation without prevented dispatch or confirmed remote cancellation;
- treating active/possibly active remote work as a proven terminal local failure;
- collecting results before terminal execution evidence; and
- rewriting the outcome or details of a terminal operation.

Remote observations may skip intermediate states. A short attempt may move from an
ambiguous/queued observation directly to a terminal state; observing `running` is not a
precondition for accepting a validated terminal result.

`Attempt` and `Operation` carry monotonically increasing revisions and update timestamps.
A true no-op preserves revision and timestamp. Persistence in M3 must write a state revision
and its sequenced event atomically; this package defines the rules but does not pretend an
in-memory transition is durable.

## Capability and evidence semantics

A capability conclusion is one of:

```text
supported | unsupported | unknown
```

Its evidence level is recorded independently:

```text
planned
implemented_offline
documented_upstream
passed_live
not_tested
blocked_environment
```

A capability cannot be marked supported or unsupported solely from planned, not-tested, or
blocked-environment evidence. Live evidence requires an account-scoped check and evidence
timestamp. This prevents an upstream feature description or fake-provider test from being
reported as account-level support.

## Error and recovery semantics

`Problem` uses stable error codes grouped by validation, authentication/authorization,
input preparation, provider preparation, submission, execution, observation, results,
operations, and local runtime.

It records independently:

- the failure stage;
- whether retrying the same control operation is safe;
- whether compute may already have started;
- a recommended next action such as `reconcile`, `collect`, or an explicit new compute
  attempt; and
- bounded provider-neutral diagnostic details.

There is intentionally no bare `retryable` flag. An ambiguous compute submission cannot be
converted into permission for automatic compute retry.

## Entities introduced

- `Job`: immutable specification identity, workspace, active attempt, and frozen
  provider-instance/profile/configuration revision.
- `Attempt`: one explicit execution attempt, independent state dimensions, revision, and
  timestamps.
- `Operation`: durable cancel, retry-compute, reconcile, collect, or cleanup action with its
  own lifecycle and structured failure.
- `Event`: one positive, monotonically sequenced durable fact associated with a job and
  optionally an attempt/operation.

The package defines validation for these entities. Actual ID generation, idempotent
admission, transactions, and storage belong to later application and persistence tasks.

## Offline verification

The M2-02 test suite covers:

- every declared error-code category;
- malformed typed IDs and digests;
- optimistic capability/evidence contradictions;
- normal attempt lifecycle and direct terminal observation;
- stale terminal rollback;
- cancellation prevention, confirmation, and completion races;
- artifact expiry without rewriting successful execution;
- contradictory cross-dimension states;
- immutable/value-based attempt transitions and revision overflow;
- operation terminal immutability and idempotent repeated outcomes; and
- entity timestamp, identity, sequence, and scope validation.

The package uses only the Go standard library. No credential, provider API, remote resource,
or compute is involved in these tests.

## Next consumers

M2-03 should map these types into strict JSON Schema/OpenAPI representations without
weakening unknown-state or error semantics. M2-04 should define provider ports and a
deterministic fake provider that produce normalized observations against this model without
adding provider-name branches to the domain.
