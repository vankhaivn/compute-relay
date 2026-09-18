# ADR-0012: Explicit attempt controls and original receipts

Status: accepted. Requirements: DOM-02/03, API-03, OPS-04, DUR-02/03.

## Decision

Require current operate authority, an explicit source attempt and idempotency key for cancel,
retry, reconcile and collect. Store immutable acceptance receipts separately from current operation
status. Commit related operation/state/event and retry-queue effects atomically; handlers perform
no provider or collection I/O.

Cancellation can prevent dispatch before submission intent. Otherwise, one newly committed,
capability-verified intent permits one exact-target cancel call; acknowledgement is not termination.
Unsupported/ambiguous cancellation remains manual/unresolved. Completion winning the race is not
rewritten as cancelled.

Compute retry creates a distinct attempt/nonce using verified original unexpired inputs and frozen
binding, never resets the source journal. Reconcile only observes; collect requests transfer-only
work after terminal evidence. Existing receipts replay under current authority despite later state
changes; GET reports later facts without rewriting those receipts.

## Reason and consequences

Implicit latest-attempt controls, generic retry flags and mutable receipts confuse intent with
outcome and can duplicate work. Explicit targets and separate histories cost metadata but preserve
truth across restart, response loss, revocation and races. See [controls](../operations.md),
[collection](../collection.md) and [application CLI](../../application-cli.md).
