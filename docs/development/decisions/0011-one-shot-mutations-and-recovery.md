# ADR-0011: Write intent once, recover through observation

Status: accepted. Requirements: DUR-03, PRV-02, DOM-02/03, VER-02.

## Decision

Freeze and verify original inputs, resolve the exact accepted provider/account/configuration,
and commit preparation/submission identity plus ownership/state/events before each permitted
external mutation. Only the invocation receiving a successful new commit may call Prepare/Submit.
Keep journal revision and scheduler fence checks in the same local transaction.

After committed intent or uncertain acknowledgement, observe the prewritten identity rather than
repeat the mutation. Not found is not proof of non-acceptance. Require private ready staging,
exact provider reference and monotonic confirmed evidence. Terminal observation opens collection,
not verified success. Repeated unresolved recovery stops for attention without erasing capacity.

## Reason and consequences

An at-least-once mutation wrapper can duplicate compute or orphan staging. This design sacrifices
availability when a process dies after intent but before a call, rather than guessing replay is
safe. No transaction spans provider/file I/O. Stored ownership is not a deletion permit; shutdown
and local deadlines do not terminate remote work. See [dispatch](../dispatch.md),
[recovery](../../recovery.md) and [controls](../operations.md).
