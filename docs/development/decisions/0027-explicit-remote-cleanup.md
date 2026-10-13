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

SSH binds deletion to the original Docker engine and full immutable container ID with preparation
and ownership evidence; it never forces removal or removes volumes. Missing historic engine
evidence remains unsupported. Kaggle's pinned RPCs delete by owner/name. The 2026-10-13 decision
permits this weaker boundary only with a clear preview warning and an explicit, durable
`accept_name_based_delete=true` in the cleanup request. Relay verifies the account, recorded ID,
version, privacy and ownership bytes immediately before deletion. If someone replaces the object
between verification and deletion, the replacement may be deleted, including versions and outputs.
Renaming can also leave the original object at a new name after the recorded name becomes absent.
Acceptance covers these name-based limits; an observed mismatch still refuses deletion.

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
Automatic deletion by age or prefix and arbitrary host-file deletion remain excluded. Name-based
provider deletion cannot prove atomic ownership at mutation time; explicit acceptance makes this
limitation visible instead of presenting a preceding GET as compare-and-delete protection.

## Consequences

Lease-expiry recovery may repeat a running deletion only against its immutable original target;
absence must be confirmed independently. Name-based deletion recovery only reads the original
target and never reissues a delete RPC. A later access or binding refusal cannot convert an
unacknowledged deletion into a known retained outcome. Terminal failure requires a new explicit
request, preserving old history and targets. Local verified results retain their own retention
policy. Admission-only serve accepts intent but runs no worker.

Provider support and live qualification remain separate. Kaggle mutation acknowledgements require
a subsequent explicit absence read; HTTP 403 or an ambiguous response stays unknown. Whole-object
scope and live fault behavior remain unqualified. Live cleanup still requires the authorization
in the [validation checklist](../validation-checklist.md).
