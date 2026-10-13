# ADR-0027: Explicit attempt-scoped remote cleanup

Status: accepted. Related requirements: OPS-05, PRV-01, API-03, DUR-01/02, DOM-01, VER-02.

## Decision

Use an explicit `cleanup` control for remote resources created for one named attempt. It requires
operate scope, an idempotency key, original creation receipts and current eligibility. Matching
keys replay their original receipt; distinct pending keys coalesce. Admission and operation GET
never call a provider. An operate-authorized GET preview may perform bounded read-only checks,
then revalidates current authority and the entire inventory before releasing its observation.

Snapshot all eligible staging and execution ledger targets at admission. Refuse active/unknown
execution, unresolved creation/submission, held worker leases, pending controls/collection, named
retention holds and shared references. A prepared never-started attempt remains eligible once
settled. Uncollected execution outputs require explicit `discard_uncollected=true`.

A worker claims under a fenced lease. It processes execution before staging, verifying the frozen
binding and rechecking eligibility immediately before committing each resource's deletion intent.
The target fingerprint and creation evidence cannot change. Each outcome is independently fenced
and durable: deleted, absent, retained, failed or unknown. Whole-attempt success requires every
owned target to be deleted or absent. Otherwise the operation fails without discarding earlier
outcomes. Potentially destructive or uncertain outcomes keep collection blocked.

Provider adapters must bind deletion to immutable identities. SSH records the original Docker
engine and full container ID with its preparation and ownership evidence; it never forces
removal or removes volumes. Missing historic engine evidence remains unsupported. Kaggle's
pinned owner/name deletion RPCs cannot condition deletion on the recorded ID/version, so both
resource types stay retained. A preceding GET cannot establish atomic compare-and-delete safety.

Cleanup metadata lives in separate tables. Original operation, execution, publication, resource
and idempotency history remains intact. See [controls](../operations.md) and
[provider limits](../../remote-cleanup.md#provider-limits).

## Reason and alternatives

Owners need to remove settled remote data without losing recovery evidence. An attempt-wide
success bit cannot represent partial deletion. Independent target snapshots and outcomes preserve
both confirmed effects and uncertainty across process loss. The existing fenced operation worker
provides serialization without adding a second deletion scheduler.

[ADR-0014](0014-retention-tombstones-and-cleanup-preview.md) separates observation from authority.
A preview cannot be an apply permit because identity, holds and activity can change afterward.
Automatic deletion by age or prefix, arbitrary host-file deletion and name-based provider deletion
remain excluded: none proves original ownership at mutation time.

## Consequences

Lease-expiry recovery may repeat a running deletion only against its immutable original target;
absence must be confirmed independently. A later access or binding refusal cannot convert an
unacknowledged deletion into a known retained outcome. Terminal failure requires a new explicit
request, preserving old history and targets. Local verified results retain their own retention
policy. Admission-only serve accepts intent but runs no worker.

Provider support and live qualification remain separate. Kaggle requires a reviewed supported
immutable-target deletion mechanism. Exact-target live cleanup still requires the authorization
in the [validation checklist](../validation-checklist.md).
