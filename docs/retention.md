# Retention, byte cleanup and remote previews

Expiry, local deletion and remote cleanup are different operations. This reference describes
explicitly composed services; the normal server runs no automatic sweeper. Remote apply and
staging cleanup are not implemented by the current preview path.

## Eligibility and holds

Default minimum windows are 24 hours for unreferenced inputs and seven days for safely completed
references/verified results. These are eligibility windows, not deletion deadlines; every relevant
reference must be safe. Configured byte-retention windows are bounded from one hour to 365 days.

Active/unknown/ambiguous execution, unresolved preparation/submission, incomplete collection,
held worker ownership, pending operations and named holds override age. A nominally expired
lease does not prove its callback stopped. Evaluate all historical/shared attempts and references,
not only the current attempt. Incomplete/oversized evidence fails closed.

Local operator composition can set/release named input/job/attempt holds. Repeating a name is
idempotent; releasing one does not release others, and no hold resurrects expired data. Terminal
execution without verified collection remains recovery material, not an orphan-cleanup permit.

## Expiry is a transaction

Inventory tombstones are irreversible. For results, expire the entire publication together with
result-state CAS, audit and one sequenced `result.expired` event. Preserve execution outcome,
cancellation/release evidence, original receipts and metadata. Repeated scans do not emit another
expiry event. Failed/uncertain expiry acknowledgement permits no deletion in that invocation.

New admission/retry rejects expired input inventory before byte reads and again at commit, even
when physical bytes remain. Matching original receipt replay still checks current authority first.
Current result reads report expired, not never-existent; HTTP delivery returns 410 under authority.
Metadata/event history is retained indefinitely by this component; no metadata pruner exists.

## Exact local sweep

`retention.NewSweeper` / `SweepOnce` operate only on committed tombstones with bounded pages.
Input/result roots have separate persistent `.retention-id` bindings. Swapped/replaced roots or
one root supplied for both roles fail; paths alone are not authority and automatic rebind is absent.

Verify exact workspace/object metadata and actual size/digest before same-root quarantine rename
and removal. Unexpected children/bytes/copies preserve evidence. Deletion acknowledgement is a
separate transaction; lost acknowledgement or already-absent exact owned content can recover from
the tombstone. Never use prefix deletion, arbitrary SQL or manual filesystem cleanup to bypass it.

Open handles can delay deletion; POSIX readers can outlive unlink. Expiry prevents new authorized
opens, not bytes already delivered. This is not secure physical erasure or a hostile-host sandbox.
Keep root identity files with whole-store backups/moves; see [storage](storage.md).

## Remote preview only

The operate-authorized preview service selects an exact owned execution ledger entry and verifies
frozen account/binding, identity, terminal/publication evidence and all pins. It calls only dry-run
cleanup and rechecks current authority/pins/plan before recording the observation. Contradictory
or deletion-claiming responses fail. `would_delete` is not a reusable apply permit;
`already_absent` is an observation, not a fabricated deletion event.

The execution cleanup port cannot establish safe dataset staging cleanup. Staging entries remain
retained with `staging_preview_unavailable`. Prefix matching never proves ownership; cleanup never
substitutes for cancellation. Exact-target live cleanup requires a separately reviewed procedure
and explicit authorization in the [validation checklist](development/validation-checklist.md).
