# ADR-0011: Commit one-shot mutation intents and recover by observation

- **Status:** accepted
- **Date:** 2026-09-15
- **Scope:** M3-04; DUR-03, PRV-02, VER-02

## Context

M3-02 provides durable admission and immutable profile/object references. M3-03 provides
fenced local ownership and a conservative dispatch barrier, but neither a lease nor that
barrier records which provider side effect may exist. Pending URL inputs also need a first
immutable snapshot before staging. A local database transaction cannot atomically include a
remote service request, and a lost response cannot establish non-acceptance.

The approved proposal requires private staging, prewritten identities, no automatic
ambiguous compute replay, same-attempt recovery and separate execution/result evidence.
Operator credentials belong to the runtime; Actions remain offline quality/build/test.

## Decision

Add a bounded per-attempt journal, exact resource ownership ledger and separate submission
intent in SQLite migration 5. Each meaningful journal transition commits with state and
sequenced events, using both the scheduler fence/revision and a journal version. Preserve
all existing migrations. Do not migrate an old dispatch barrier into invented provider facts.

Freeze HTTPS roles through the existing protected ingestion/blob path. Commit ownership and
job role together and never replace a frozen role. Inspect bundles and revalidate bytes
outside SQL. Retain the original request while producing a resolved object-only provider
specification and runner-compatible content digest. No workload is executed on the host.

Keep an explicit registry of immutable provider binding snapshots. Require a binding
verification contract and a read-only preparation reconciliation contract from adapters.
The effective account must remain the accepted one; credential values never enter jobs.
Private staging is a required adapter assertion, not a default inferred from upload success.

Persist a preparation identity and resource record before one Prepare invocation. Persist
the submission identity, execution resource record and possible activity before one Submit
invocation. Only the caller receiving the successful new intent commit gets that one call.
An already persisted intent is never rearmed automatically, including after a not-found
observation. Reconcile the same operation/resource identity instead.

Preserve accepted, proven rejected and unknown outcomes. Invalid responses, lost responses
and provider panics after a possible side effect cannot become optimistic rejection. A
proven quota rejection latches exhaustion without fabricating numerical quota values.

Use fenced recovery leases, bounded cooperative workers and separate call/preparation
budgets. Pausing new dispatch must leave safe observation possible. Local lease expiry,
shutdown and request deadlines do not release possible remote capacity. Stop repeated
unresolved automatic recovery at needs_attention with a sanitized structured condition.
Expose that cached condition through the existing authorized status response, not a new
HTTP operation. Never emit upstream exception strings or full internal plans there.

A terminal provider observation changes the phase to collecting; required artifacts and
runner-result verification remain necessary before success. Collection, control operations
and cleanup are intentionally not implemented in this task.

## Alternatives and consequences

Retrying Prepare or Submit with the same resource name is rejected: an adapter may create
another version/execution even under the same name. Treating not-found as permission to
retry is rejected because lookup may be delayed, incomplete or lack sufficient identity.

A distributed transaction or broker would not create transactional guarantees in the
external provider and would violate the small operator-hosted baseline. Holding SQLite
transactions over transfers would obstruct local state and still not solve ambiguity.

The conservative one-shot gate can strand work when the runtime dies after intent commit
but before sending the request. That is an intentional availability trade-off. A future
explicit resolution must prove its safety; this task does not add a manual rearm escape.
No exactly-once remote execution guarantee is made.

The journal and ledger duplicate bounded identity information so recovery can cross-check
it. They are internal state, not public wire contracts. Complete unreferenced local blobs
and unresolved remote resources remain for later ownership-based recovery/cleanup rather
than being deleted after an uncertain acknowledgement.

The component is not automatically wired into `serve`, and its only provided bound adapter
is a fixture. Real binding/privacy/staging/retry semantics remain adapter verification gates.
The future adapter must turn the resolved plan into the M2-09 remote runner manifest and
must not implement hidden transport-level mutation retries.

## Verification

Tests cover one-shot transitions, twenty competing submit gates, exact snapshot/runner
hashing, wrong privacy/identity, delayed staging, mutable URL freeze, restart, lost provider
responses, lost SQLite acknowledgements, event/ledger rollback, real SQLITE_FULL and actual
child-process kills around both intent commits. A component smoke uses actual admission,
blobs and SQLite with a nonexecuting fake provider and stops at collection.

Existing CI verifies the pinned Go/modernc stack and native Linux/macOS/Windows tests/build.
Local Linux evidence uses a disclosed non-shipped Go 1.23.2/system-SQLite isolation harness;
it is not equivalent to production integration. No credential, provider API or GPU is used.
See [the dispatch guide](../dispatch.md) and PR #14 for exact evidence and remaining gates.
