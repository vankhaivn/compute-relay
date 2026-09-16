# Retention, local byte sweep and remote cleanup preview

> **Task:** M3-07, implemented offline; PR #17 merged.
>
> **Scope:** durable retention decisions, named holds, exact local byte deletion and
> ownership-ledger-based remote dry runs. No remote apply, production cleanup command,
> deployment or live-provider verification is supplied by this task.

## Three distinct operations

Expiry is a metadata transaction. Local sweep removes only bytes already covered by a
committed irreversible tombstone. Remote preview is a read-only provider observation;
`would_delete` and `already_absent` do not mean the runtime deleted a resource. None of
these operations cancels compute, releases remote capacity or starts another attempt.

`internal/retention` contains the policy, finite sweeper and preview service. SQLite owns
the inventory, references, holds, tombstones, audit and root binding. `blobfs` owns exact
identity verification and same-root quarantine before removal. Composition is explicit:
opening/migrating a database, admission and HTTP GET never start a sweep or provider call.

## Policy and pins

`DefaultPolicy()` uses 24 hours for unreferenced input uploads and seven days for completed
referenced inputs and verified result sets. These are minimum eligibility windows under
that policy, not deletion deadlines. A later safe reference/publication time extends the
window. Configured byte-retention durations must be between one hour and 365 days.

An input is evaluated against **every attempt of every job that references it**, not just
the current active attempt. Result expiry covers an entire verified attempt publication,
not a partial selection of its files. Eligibility requires all relevant evidence to agree:

- no active, unknown, ambiguous or nonterminal work;
- no held scheduler/collection ownership or accepted/running operation;
- no unresolved preparation/submission or missing collection recovery evidence; and
- no active named input/job/attempt hold, with the retention window satisfied.

A terminal provider execution without a verified collection publication stays pinned.
Missing, invalid or incomplete results are recovery work, not permission to delete their
inputs or provider resources. Private staging with uncertain readiness is retained. A held
lease remains a pin even after its nominal expiry: time passing alone is not proof that a
callback stopped or that recovery evidence can be discarded.

`Store.SetRetentionHold` is a local operator composition API, not an application HTTP route.
It targets an input, job or attempt and requires a named hold ID. Repeating a hold/release is
idempotent; releasing one name does not release another. At most 100 hold names per target
are recorded. A new hold cannot resurrect expired data. Metadata changes are audited without
storing private payloads or credentials.

Reference enumeration has a 100,000-reference ceiling and fails closed rather than making a
decision from a truncated set. Ordinary SQLite operation deadlines still bound scans.

## Migrations and persistent history

Migration 8 adds `retention_inventory`, named holds, attempt expirations, an append-only
retention audit, a rotating scan cursor and latest remote preview records. Existing object
and artifact rows are inventoried with a fresh conservative observation time; upgrade does
not guess their historical byte age or immediately expire them.

Migration 9 binds the input and result inventory kinds to separate persistent blob-store
identities. Both migrations are additive. Migration 1 through 7 bytes, installation identity,
accepted job specifications, object references, original receipts, collection publications,
submission intents and provider-resource ownership remain unchanged.

This task does **not** prune job/attempt/operation/event metadata. It retains that history,
object/artifact metadata and tombstones indefinitely, exceeding the proposal's 30-day minimum
metadata retention. There is no promise that records are automatically removed on day 30.
A later metadata-pruning design must preserve replay, references and ownership evidence.

## Atomic expiry and observable results

`Store.ExpireRetention` scans a bounded rotating inventory page. Reference/pin checks, result
state CAS, all result tombstones, audit and cursor advancement share one short transaction.
Failure or an uncertain acknowledgement returns no successful expiry report and authorizes
no filesystem removal in that invocation. No transaction spans provider or blob I/O.

For a verified result set, expiry changes only `result` from `available` to `expired` and
advances the attempt revision. It appends exactly one sequenced **`result.expired`** job event
in the same transaction. Execution, orchestration outcome, cancellation and release evidence
are preserved. Event insertion failure rolls back the whole expiry page. Repeated scans or
database restart do not emit another event for the same irreversible expiry.

Current job status can therefore report succeeded execution/orchestration together with
expired results. Authenticated internal collection reads return `retention.ErrExpired`
instead of pretending the publication never existed. Artifact rows and their verified
identity remain available as historical metadata; content is no longer authorized by that
publication after expiry. This task adds no artifact download or event HTTP endpoint.

Input metadata likewise survives its bytes. New job validation/admission and explicit retry
resolve only exact **unexpired** inventory. Retry fails with the existing original-input
error before rereading physically present but expired bytes, and rechecks again during
commit. SQL guards independently reject new references, attempts or reactivation after a
tombstone. Existing job/control idempotency replay remains receipt-first and still checks
current authority; expiry cannot turn replay into a new execution or erase an old receipt.

## Finite local sweep

Compose `retention.NewSweeper` with SQLite, the input blob store, the **separate** result
blob store and a clock. `SweepOnce` accepts an explicit policy, deletion cursor and page
limit of 1 through 100. The caller carries the returned cursor until an empty page returns
zero, then starts a later bounded pass as needed. Per-file failures do not hide later
candidates; they remain pending for another pass. No background runtime is installed here.

The sweeper binds both store identities before expiry/removal. Each blob root obtains a
private, atomically published `.retention-id` on first retention use. SQLite persists the
kind-to-identity mapping. Swapping input/result roots, supplying the same root twice or
replacing a bound root with a new store fails closed. Paths alone are not deletion authority.

For each committed tombstone, the input or result store must match the exact workspace,
object ID, length and digest. Deletion validates private directories, expected metadata,
known object files and actual byte hash before renaming the complete object directory into
that root's temporary quarantine. Provider paths are never interpreted as local paths.
Unknown children, changed bytes, conflicting copies or invalid metadata preserve evidence
and return an error instead of broadening deletion.

The same-root rename precedes unlinking individual quarantine files. A restart can finish
an interrupted owned quarantine; an already-absent exact local object is an idempotent
outcome. The deletion acknowledgement is recorded separately in SQLite. Lost deletion or
completion acknowledgement leaves a durable tombstone from which another pass can recover,
without duplicate audit records or a new compute request.

A pass has a ten-minute context budget and each deletion a one-minute budget. Context-aware
reads cooperate with cancellation; these are not hard real-time filesystem deadlines. An
open reader can prevent rename on Windows; deletion stays pending. On POSIX an already-open
file descriptor can outlive unlink. Expiry prevents new authorized result opens, not the
revocation of bytes already delivered to a client. This is not secure physical erasure.

`blobfs.Delete` is a trusted internal primitive, not a public authorization service. Call it
only through correctly composed retention logic after a committed tombstone. Same-user
filesystem writers and trusted in-process code remain inside the existing operator trust
boundary. Never use ad-hoc SQL, `rm -rf`, a filename prefix or database deletion to bypass it.

## Exact-ledger remote dry run

The preview service requires current workspace `operate` authority and an exact persisted
provider-resource ID. It validates installation/job/attempt/submission identity, creation
operation, frozen profile/configuration/account binding, resource reference, plan digest,
terminal observation, publication and retention pins. Shared or unresolved references are
retained; an arbitrary prefix cannot discover or authorize a cleanup target.

An eligible execution preview resolves the original binding, verifies that binding and
invokes only a source `Preview` method. The adapter wrapper always forces
`provider.CleanupDryRun`; it exposes no apply method. The response cannot claim `Deleted`,
or simultaneously `WouldDelete` and `AlreadyAbsent`. Current authority, pins and plan digest
are checked again in the transaction recording the observation, closing hold/revocation
races while the read-only provider call was in flight.

Repeated exact owned absence is safe and updates the latest preview record, not the original
ownership ledger or a fabricated remote-deletion event. `would_delete` is a plan observation,
not a reusable deletion permit. A future apply path requires its own explicit authorization,
fresh checks and provider-specific evidence.

**Staging preview remains unavailable.** The existing provider cleanup port identifies
executions, not staging datasets. Staging ledger records are retained with
`staging_preview_unavailable`; the execution cleanup port is never substituted. This limit
is explicit rather than a fake successful staging cleanup. No live Kaggle behavior is proven
by the fixture preview source.

## Recovery and backup boundaries

Keep the database, input root and result root together when planning installation recovery.
A database-only backup contains tombstones and root bindings, not blob bytes. Preserve each
root's `.retention-id` when backing up or moving the entire store. A new root identity must
not silently inherit an old database's deletion tickets. There is no automatic rebind command.

Quiesce writers and retention while taking a coordinated checkpoint. Restoring an older
database may restore older grants/receipts and lack later expiry/deletion facts, while bytes
may already be gone. Preserve the old state, verify all required stores and references and
review authorization before enabling runtime work. Never activate original and restored
installations against the same provider identity at once. A restore does not stop remote
compute, and missing bytes never justify refetching original URLs or rerunning compute.

Complete pre-publication collection blobs and resources still needed for recovery remain
pinned rather than being treated as orphan garbage. General orphan discovery, metadata
pruning, remote apply, production CLI/configuration and installation-wide automated backup
remain outside this component.

## Verification and evidence

Reproduce repository checks with the pinned toolchain and locked dependencies:

```text
go run ./cmd/devtool check
go run ./cmd/devtool test-race
go test -run='TestRetention|TestCleanupPreview|TestDelete' ./internal/store/sqlite ./internal/blobfs ./internal/retention
```

Tests cover active/unknown/recovery pins, shared references, independent named holds, expiry
windows, tombstone guards, expired input preflight/commit and immutable receipt replay.
SQLite/private-blob tests cover schema-7 upgrade, root swaps/replacement, exact deletion,
reopen/quarantine recovery, wrong bytes/children, lost expiry/delete/completion acknowledgements,
audit/event rollback and actual `SQLITE_FULL`. Preview tests cover exact ownership/binding,
prefix and cross-workspace rejection, repeat absence, staging refusal, hold/revocation races
and contradictory provider responses.

M3-07 quarantine tests use explicit rename/reopen and fault injection, not a claim of a new
process-kill experiment at every deletion boundary. Earlier process-kill suites remain part
of the repository checks. PR #17 records the exact tested head and CI runs. Native
Linux/macOS/Windows tests and CGo-free builds use the pinned Go 1.27.1/modernc stack in CI.
Local Go 1.23.2 formatting and Python schema/hash checks are reported separately; no full
local modernc integration or retention smoke executable is claimed. No dependency, workflow,
provider adapter or runner asset was changed for this task.

M3-08's [fault matrix](fault-matrix.md) nominates repeated exact-owned absence as FM24 and
runs it through fresh named-test qualification. That remains **dry-run** evidence; neither
the matrix nor a passing preview implements remote apply or staging cleanup. The complete
retention suite still runs through the repository checks beyond that one nominated root.

See [ADR-0014](decisions/0014-retention-tombstones-and-cleanup-preview.md),
[storage](storage.md), [collection](collection.md), [operations](operations.md) and
[recovery semantics](recovery.md). The [implementation plan](implementation-plan.md) records
the current owner-review and next-task gate; production runtime composition remains separate.
