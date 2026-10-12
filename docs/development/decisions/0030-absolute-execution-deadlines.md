# ADR-0030: Immutable absolute execution deadlines

Status: accepted (2026-10-12). Requirements: JOB-03, JOB-05, PRD-09, DOM-02, DUR-03, OPS-01, OPS-03, OPS-04, SSH-03.

## Decision

Add an optional whole-second UTC `timeouts.execution_deadline` to the immutable job contract.
Keep the relative resource budget; the earlier work bound wins and finalization remains bounded
separately. An explicit retry keeps the original job deadline. Capability-gate the requirement,
with SSH implementing it initially and old jobs retaining their original relative-only behavior.

Check new admission, authorization and one-shot preparation/start barriers for expiry. Preserve
idempotent receipts and immutable snapshots after their deadline. A possible earlier start stays
in original-identity observation; local expiry cannot prove inactivity or release capacity.

Enforce remotely with UTC and a frozen Linux suspend-aware elapsed ceiling. Give each command
an independent process-group watchdog and the SSH container a PID 1 finalization supervisor.
Check the engine clock against a bounded controller interval before start. Publish the exact
clock and grace assumptions in the [execution deadline contract](../../execution-deadlines.md).

## Reason and alternatives

A relative runner timeout starts after queueing and staging, so it cannot preserve a time window
owned by an external application. A controller-only timer disappears with Relay or connectivity.
An absolute UTC check alone can be extended by a backward clock step; a monotonic clock that
excludes suspend can silently extend work after resume. Independent watchdogs keep a blocked
runner or output path from extending payload activity.

## Consequences

- The feature remains finite work, not scheduling, a recurring job, an interactive session or an
  external-service lifecycle controller.
- Expired prepared resources remain exact owned resources requiring explicit cleanup. No automatic
  restaging, restart or replacement is added.
- Clock synchronization, schedulable Linux execution and truthful host administration are required.
  Software cannot act while powered off; VM power behavior requires its own qualification.
- Hard termination may leave no complete artifact. Provider termination, result publication and
  hardware release keep their existing independent evidence requirements.
