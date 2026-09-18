# ADR-0014: Commit expiry before exact byte cleanup

Status: accepted. Requirements: OPS-05, DUR-01, VER-02; preserves DUR-02/03 and DOM-02/03.

## Decision

Separate retention assessment, irreversible expiry, local byte removal and remote dry-run
preview. Age is not enough: all historical/shared references, manual holds, held callbacks,
active/ambiguous execution and incomplete recovery must be safe. Default minimum windows are
24 hours for unreferenced inputs and seven days for safely completed references/results.

Expire a whole verified result with state CAS, tombstones, audit and one `result.expired` event;
preserve execution/cancellation/release and immutable receipts/history. Reject new input use after
expiry even when bytes remain. Bind input/result inventory to separate persistent blob-root IDs.
Only exact metadata/digest-matching tombstones permit quarantine/removal; acknowledge deletion
separately and recover already-absent exact targets safely.

## Reason and consequences

Deleting files first loses evidence; deleting metadata destroys replay/ownership. Path-only deletion
can target a replaced store. Pins favor recovery over space, and metadata grows without a pruner.
No automatic root rebind, secure erasure or revocation of already-read bytes is promised.

Remote preview requires exact owned frozen binding, terminal/publication/pin evidence and renewed
authority. Dry-run is never an apply permit. Staging preview is unavailable through the execution-only
cleanup port. See [retention](../retention.md) and [coordinated recovery](../storage.md).
