# Retention, byte cleanup and remote previews

Expiry, local deletion and remote cleanup are different operations. Serve modes with provider
workers run the local sweep below with the default policy; admission-only serve does not. Remote
deletion is never authorized by the preview path; explicit attempt-scoped apply uses the separate
[cleanup control](operations.md#remote-cleanup).

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
Provider-mode serve runs a pass at startup and then hourly: it follows the durable inventory scan
until it wraps and deletes committed tombstones page by page. A failed expiry, listing or deletion
never stops dispatch, collection or HTTP serving; the next pass resumes from the tombstones.
Input/result roots have separate persistent `.retention-id` bindings. Swapped/replaced roots or
one root supplied for both roles fail; paths alone are not authority and automatic rebind is absent.

Verify exact workspace/object metadata and actual size/digest before same-root quarantine rename
and removal. Unexpected children/bytes/copies preserve evidence. Deletion acknowledgement is a
separate transaction; lost acknowledgement or already-absent exact owned content can recover from
the tombstone. Never use prefix deletion, arbitrary SQL or manual filesystem cleanup to bypass it.

Open handles can delay deletion; POSIX readers can outlive unlink. Expiry prevents new authorized
opens, not bytes already delivered. This is not secure physical erasure or a hostile-host sandbox.
Keep root identity files with whole-store backups/moves; see [storage](storage.md).

## Remote preview and explicit cleanup

The public [attempt preview](../remote-cleanup.md) lists original staging and execution resources
with bounded typed targets, support and exclusion reasons. It may perform dry-run checks and
rechecks current authority and the complete inventory afterward. It never records deletion intent.
The older retention component's execution-only preview remains read-only and cannot prove staging
cleanup eligibility; it is not the public attempt inventory.

The [cleanup control](operations.md#remote-cleanup) is the only remote apply path. It is an
explicit request for one attempt, never a sweep: retention neither creates nor consumes it,
and a preview result is not an input to it. Pending controls, shared references and named
input/job/attempt holds block apply. A succeeded cleanup does not expire local results or change
their retention windows. Exact-target live cleanup requires the separate authorization in the
[validation checklist](validation-checklist.md).
