# ADR-0026: Explicit result collection and observed output progress

Status: accepted. Requirements: DOM-01/02, DUR-01/03, API-03, PRV-02, VER-02.

## Decision

Add optional immutable JobSpec `result_collection=manual`; omission keeps automatic collection
and historical canonical bytes. Manual policy gates initial automatic tickets for every result,
including failure diagnostics. Reuse the existing attempt-scoped collect control, original
receipts, coalescing, fenced recovery and transfer-only failure retry. Collection grants no
compute and preserves original provider/account/version identity.

Expose additive collection status separately from remote execution and local publication. Persist
bounded generation-scoped selected-output samples: completed work includes rehashed cache and
the current unverified stream; received bytes include only new observed selected output streams.
Controls/protocol overhead are excluded, totals are unknown until pinning, and providers without
stream observation retain unknown received bytes. Sample no more than once per second while
streaming, plus durable boundary updates; expiry projects pending recovery.

Keep immutable result pins, independent byte/receipt verification, and atomic publication. Full
progress counts are not availability. Local artifact delivery remains a separate verified read.

## Reason and consequences

Finite remote work can complete while the application/runtime is closed. Owners can postpone
large result transfer until explicitly needed without another compute permit or host callback.
Existing automatic consumers remain compatible. Provider retention and partial-byte Range resume
are not promised. Failed executions may publish diagnostics without becoming business success.

See [collection](../collection.md), [controls](../operations.md) and
[artifact delivery](../../artifact-delivery.md).
