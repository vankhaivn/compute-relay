# ADR-0012: Attempt-scoped durable controls and immutable receipts

- **Status:** proposed; implemented offline in PR #15, pending owner review/merge
- **Date:** 2026-09-15
- **Task:** M3-05
- **Requirements:** DOM-02/03, API-03, OPS-04; preserves DUR-02/03 and VER-02

## Context

Admission and the fenced dispatch journal already survive restart without automatically
repeating ambiguous provider mutations. User controls must preserve those guarantees under
lost HTTP responses, concurrent completion/cancellation/retry, revocation and source expiry.
An implicit active-attempt target or a mutable idempotency receipt would make the same
request mean different work after a retry. Cancellation acknowledgement is not terminal
execution evidence, and missing artifacts do not justify repeating compute.

## Decision

Require an explicit source `attempt_id` on every cancel/retry/reconcile/collect request.
Require a scoped idempotency key on every POST and a nonblank non-secret reason for compute
retry. Parse bounded strict JSON and recheck current workspace/token authority inside the
store transaction, including replay. HTTP remains provider-neutral and performs no provider
call; the operation service and dispatch engine are separate composition boundaries.

Persist current operation state separately from its immutable acceptance receipt. Matching
POST replay returns that original receipt; GET returns the current revision. Key identity
covers workspace, kind and canonical job/attempt/reason. Operation/state/event changes and
new attempt/queue creation are atomic. Per-source-attempt uniqueness prevents duplicate
cancellation and multiple concurrent compute retries even with different keys.

Verify retry's original frozen bytes outside SQL, then recheck authority, target eligibility
and snapshot identity in the commit. Retry creates a new attempt and nonce without changing
job specification, profile/provider binding, inputs or historical attempts. Refuse unresolved
remote execution and direct successful execution with missing results to collect instead.
Never refetch a URL or silently remap a provider to make retry succeed.

Prevent dispatch atomically where possible. Otherwise commit a one-shot cancellation intent
before the exact-bound, capability-verified adapter call. Do not automatically repeat an
ambiguous cancellation or use cleanup as its substitute. Accepted/unknown cancellation
retains possible remote capacity; only matching terminal evidence confirms termination.
Late completion cannot be overwritten with a fabricated cancellation outcome.

Reconcile existing identity without repeating staging/submission mutations. Collect creates
a durable transfer-only ticket; its consumer, artifact verification and publication remain
M3-06. No accepted control receipt is proof of remote execution, verified results or hardware
release. Terminal operation outcomes remain historical facts rather than being rewritten
by later execution observations.

Keep the generic domain operation schema, including the reserved cleanup kind. Publish a
separate strict HTTP control view with receipt/current-state links, explicit effect and
termination fields, optional new attempt ID and sanitized problem fields. There is no
cleanup HTTP handler or production `serve` claim.

## Alternatives considered

- **Target the current active attempt implicitly:** rejected because a repeated request
  after retry could act on a different execution.
- **Return current state for every idempotency replay:** rejected because acknowledgement
  recovery must identify the original committed operation and effects.
- **Retry inputs or provider mutations automatically:** rejected because fresh URL bytes,
  remapped bindings or ambiguous submissions could silently change or duplicate compute.
- **Perform provider controls in HTTP transactions:** rejected because network duration and
  uncertain acknowledgement cannot safely extend short local metadata transactions.
- **Treat collection acceptance or cancellation acknowledgement as success evidence:**
  rejected because those facts do not establish verified artifacts or terminal execution.

## Consequences

Receipt storage and current operation records intentionally duplicate some immutable fields.
Conservative one-shot cancellation can require manual inspection even when a crash happened
before the remote call. Collection tickets may remain pending until M3-06 is implemented.
Operators must read operation, attempt, result and release evidence separately. None of
these trade-offs authorizes new provider side effects, cleanup or a broader milestone.

## Verification

Migration/rollback/restart tests preserve installation, journal and prior attempt history.
Concurrency, lost-acknowledgement, cancellation/completion, stale authority and frozen-input
tests establish local failure semantics. Real-loopback SQLite HTTP and strict published
schema tests cover actual receipts, replay and current reads. Exact-head CI and local
limitations are recorded in PR #15 and [the operations guide](../operations.md).

This ADR adds no live Kaggle capability evidence. It extends, rather than supersedes,
[ADR-0009](0009-durable-idempotent-admission.md) and
[ADR-0011](0011-one-shot-mutations-and-recovery.md).
