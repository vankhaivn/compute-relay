# ADR-0014: Retention tombstones, bound stores and cleanup previews

- Status: accepted; PR #17 merged on 2026-09-15
- Date: 2026-09-15
- Task: M3-07
- Requirements: OPS-05, DUR-01, VER-02; preserves DUR-02/03 and DOM-02/03
- Builds on: ADR-0008, ADR-0011, ADR-0012 and ADR-0013

## Context

The durable core has immutable input references, one-shot provider ownership ledgers,
operation receipts and verified collection publications. Their existence does not authorize
deleting bytes while compute, collection, callbacks or manual recovery still depend on them.
Filesystem deletion and SQL acknowledgement cannot share one atomic transaction. A path can
also be replaced with a different blob store after a database backup is restored.

Remote execution cleanup and staging cleanup are different provider operations. The current
port addresses executions only, and offline fixture behavior is not live-provider evidence.

## Decision

Separate policy assessment, committed expiry, exact local byte removal and remote preview.
Use 24 hours for unreferenced inputs and seven days for safely completed referenced inputs
and verified results by default. Evaluate all references; active, unknown, recovery-required,
held-worker, pending-operation and named manual pins override age. Fail closed on incomplete
or oversized evidence, including an expired lease whose held ownership has not been resolved.

Migration 8 adds immutable inventory identity, irreversible tombstones, named holds, result
expirations, append-only audit, a rotating expiry cursor and latest cleanup previews. Legacy
inventory receives a fresh observation time instead of guessed historical age. Retain object,
artifact, job, attempt, receipt, publication and provider-resource metadata; do not implement
metadata pruning. Indefinite preservation exceeds the proposal's 30-day minimum without
claiming a day-30 deletion policy.

Expire a whole verified result publication atomically with result-state CAS, all its byte
tombstones, one sequenced result.expired event and audit. Preserve execution/business outcome,
cancellation and hardware-release evidence. Read expired results as expired, not nonexistent.
Reject new admission/retry references to expired input inventory during preflight and commit,
while preserving receipt-first replay under current authority. Keep SQL guards as defense
against stale callers after the eligibility read.

Migration 9 binds input and result inventory to separate persistent blob-root identities.
Publish a private .retention-id atomically on first retention use and preserve it with a
whole-store backup or move. A replacement or swapped store cannot inherit the bound tombstones.
No automatic rebind or path-only deletion fallback is introduced.

The finite explicitly composed local sweeper reads only committed tombstones. Verify exact
workspace/object metadata and actual byte length/digest before same-root quarantine rename
and removal. Record deletion acknowledgement separately. Lost acknowledgements recover via
the durable tombstone; an already-absent exact local object is idempotent. Do not delete
unexpected content or broaden a target because verification failed. Existing OS locking and
the trusted local-operator boundary remain in effect.

For remote cleanup, provide only a dry-run preview of an exact ownership-ledger entry under
workspace operate authority. Resolve and verify the frozen account/configuration binding,
require terminal/publication/retention evidence and reject shared/unresolved references.
The wrapper forces CleanupDryRun. Recheck authority, pins and the complete plan digest before
recording the observation. Reject a response claiming deletion or contradictory outcomes.
Do not change the original ledger or interpret would_delete as a reusable apply permit.

Retain staging entries with staging_preview_unavailable because the execution cleanup port
cannot establish safe staging behavior. Prefix matching is never ownership authority. Remote
apply requires later explicit authorization, a suitable adapter path and fresh evidence.

## Alternatives rejected

- Delete files first and repair metadata later: loses data without a durable decision.
- Delete immutable metadata with bytes: destroys replay, historical verification and ownership.
- Use age or lease expiry alone: can erase active or unresolved work and stalled callbacks.
- Scan only the active attempt: can miss historical/shared recovery dependencies.
- Bind deletion to configured paths only: a restored database could target a replacement store.
- Hold SQLite across hashing or provider calls: extends a local transaction across external I/O.
- Treat cleanup as cancellation, or dry-run success as deletion: invents remote effects.
- Reuse execution cleanup for staging datasets: the port does not prove that target semantics.

## Consequences and limits

Conservative pins can retain data beyond nominal windows and require operator resolution.
Metadata and audit grow until a separately reviewed pruning policy exists. Collection bytes
that lack publication remain recovery material, not sweep candidates. Byte deletion is not
secure physical erasure and cannot revoke bytes already read. Open handles may delay Windows
removal; context deadlines do not forcibly interrupt arbitrary filesystem/kernel work.

Database-only backup is insufficient for installation recovery. Whole input/result stores,
including their identity files, must be preserved separately; old snapshots may lack later
expiry or token-revocation facts. Never bypass root mismatch by deleting markers or rewriting
SQL. No production command, automatic sweep, remote apply or live-provider claim is added.

## Verification

Policy and SQLite/blob tests cover pins, independent holds, expiry/retry races, original
receipts, exact deletion, quarantine reopen, root replacement, upgrade, event/audit rollback,
actual SQLite disk-full and lost acknowledgements. Preview tests verify ownership, frozen
binding, repeated absence, staging refusal, current authority and in-flight hold/revocation.
Tests assert no additional compute submission or remote apply. Exact-head CI and local test
limitations are recorded in PR #17 and [the retention guide](../retention.md).

This decision extends rather than rewrites earlier accepted ADRs. Stop for owner review/merge
after PR #17; the M3-08 fault-matrix audit and production composition remain separate tasks.
