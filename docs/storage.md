# Durable metadata and database-only recovery

> **Task:** M3-01, implemented offline; PR #11 in review.
>
> **Scope:** SQLite workspace/token/object repositories, migrations, process ownership and
> backup/restore. This is not a complete durable job runtime or a production `serve` mode.

## Implemented boundary

`internal/store/sqlite.Store` implements the existing `auth.TokenRepository`,
`auth.WorkspaceRepository` and `objects.Repository` interfaces. It persists installation
identity, workspace enablement/profile grants, token digests/scopes/expiry/revocation, and
immutable object ID/owner/size/digest metadata. It never stores blob payloads or provider
credential values. The object service remains responsible for publishing verified bytes
before committing metadata; SQL does not span an upload, HTTPS request or provider call.

`PutWorkspace` is local configuration authority. Token issuance/revocation still goes through
`auth.Service`; newly generated secrets are returned only after the SQLite insert succeeds.
No application-facing administrative API or nondurable production fallback is introduced.

Job/attempt admission, idempotency, event/state transactions and scheduler/submission-intent
records belong to later M3 tasks. Existing developer upload/import/ingest fixtures remain
explicitly nondurable unless a caller deliberately composes these new repositories.

## State layout and process ownership

```text
state/
  runtime.lock             persistent lock inode; OS lock, not PID-file ownership
  .compute-relay-state     versioned root marker
  .installation            guard against accidental database deletion/replacement
  runtime.db               SQLite metadata
  runtime.db-wal           SQLite-owned while needed
  runtime.db-shm           SQLite-owned while needed
```

The parent of a new state directory must already exist. Supply a dedicated private path,
not an application repository or home directory. Root and database/sidecar symlinks and
nonregular files are rejected. Unix permissions exclude group/other access; new Windows
roots receive a protected DACL for the current user, SYSTEM and administrators. Existing
unsafe directories are rejected without rewriting their permissions.

Only one Store/process may own a state directory. A second opener returns
`statefs.ErrLocked`. Close waits for active store operations, closes SQLite and only then
releases the OS lock. Never delete `runtime.lock` to bypass a lock: another process may
still hold the old inode. Blob storage retains its independent root lock.

The local administrator and same-user filesystem writers are trusted; this is not a
filesystem sandbox. Network/shared filesystem locking and arbitrary storage hardware
power-loss behavior have not been established by the component tests.

## SQLite and transaction policy

| Setting | Value |
|---|---|
| Driver / required libc | `modernc.org/sqlite v1.58.0` / `modernc.org/libc v1.75.6` |
| Toolchain / production CGo | Go 1.27.1 / disabled build supported |
| Journal / synchronous | WAL / FULL (2) |
| Foreign keys / trusted schema | ON / OFF |
| Private connection pool | One open and one idle connection |
| Transaction lock | IMMEDIATE |
| Busy timeout / WAL auto-checkpoint | 1,000 milliseconds / 1,000 pages |
| Default ordinary operation / backup budget | Five seconds / one minute |
| Default maximum database snapshot | 256 MiB, configurable up to 1 TiB |

DSNs are constructed from escaped paths, not concatenated operator URI parameters.
Connection-scoped pragmas are configured through the driver for every physical connection.
Operations have context deadlines, including queueing for the connection. Filesystem
flushes still depend on the host OS; a context is not a hard real-time disk watchdog.

Transactions are short, internal and atomic. Failures roll back; commit errors are never
blindly replayed. Busy, full, corrupt, conflict and unsupported-schema conditions have
separate sentinel errors without exposing raw SQL or host paths. Metadata size can exceed
the default backup ceiling; increase that explicit ceiling only after reviewing disk and
time budgets rather than silently enlarging it.

## Migration rules

The embedded `migrations/0001_identity.sql` and `0002_workspace_objects.sql` are immutable
once released/applied. SQL bytes, version and name are checked against `schema_migrations`;
DDL, ledger insertion and `user_version` changes share one transaction. Never fix an applied
migration in place. Add a new version and regression test.

Open rejects a newer version, a foreign application ID, missing/gapped/tampered migration
history, invalid installation identity or failed integrity/foreign-key check. It does not
reset unknown databases. Initial migrations are additive. A future destructive migration
must define and verify a pre-change backup procedure; arbitrary downgrade is not supported.

Do not remove `.installation`, `.compute-relay-state` or a `runtime.db.restore` blocker to
make a damaged directory open. Preserve evidence and recover into a new directory.

## Backup and restore API

The Go library exposes `Open(ctx, stateDir, options)`, `Store.Backup(ctx, newBackupDir)` and
`Restore(ctx, backupDir, newStateDir, options)`. These are local composition APIs, not public
HTTP routes. Administrative command wiring is not shipped yet; do not assume a
`compute-relay backup` command exists.

Backup uses `VACUUM INTO`, preserving committed WAL contents in a consistent snapshot.
The private output directory contains:

```text
metadata.sqlite    standalone database, without required WAL/SHM sidecars
backup.json        format/scope/installation/schema/time/size/SHA-256 receipt
```

A receipt is written only after the snapshot is flushed and validated. A missing or
malformed receipt, sidecars, excessive size, changed bytes or unsupported schema makes the
bundle unusable for restore. A failed ordinary operation removes only its newly created
incomplete output; a crash may leave an incomplete directory. Preserve it for diagnosis
rather than treating it as a successful backup.

Restore refuses **every existing destination**, even an empty directory. It copies into
private staging, verifies the digest/schema/identity/integrity, and publishes the database
only after validation. An interrupted restore marker blocks accidental fresh initialization.
The resulting installation ID is unchanged, and supported older schemas upgrade on Open.

### Operator recovery procedure

1. Preserve the original state and matching blob store; do not copy only a live
   `runtime.db` and assume committed WAL data came with it.
2. Obtain a verified database-only snapshot through `Store.Backup`. For a whole-installation
   checkpoint, quiesce writes/retention and separately retain all referenced immutable blob
   bytes. This PR does not automate or verify a combined database/blob backup.
3. Stop the original runtime before activating a restored copy. Restoring local metadata
   does not stop remote compute and must not cause a new dispatch.
4. Restore into a new private directory with the matching binary/schema support. Open it,
   inspect installation identity/readiness, and verify referenced blob availability before
   exposing future job operations. Keep the old directory intact until verification ends.
5. Review workspace access and revoke/rotate tokens as needed: restoring a snapshot from
   before revocation can restore that old grant. Keep backups private and out of ordinary
   support bundles, user artifacts and source control.

The receipt detects accidental corruption, not a maliciously rewritten snapshot plus
receipt. Never run original/restored copies concurrently against the same provider identity.
Windows files are flushed; directory-sync and sudden-power-loss guarantees remain unclaimed.

## Developer checks and evidence

With the pinned toolchain:

```text
go test -race ./internal/statefs ./internal/store/sqlite
go test -count=25 ./internal/statefs ./internal/store/sqlite
go run ./cmd/storesmoke
```

`storesmoke` creates private temporary SQLite state, persists synthetic workspace/token and
object metadata, reopens it, checks isolation/revocation, makes a live consistent snapshot
and restores it under the same identity. Its report says `backup_scope=database-only` and
`blob_bytes_checked=false`; it is not an upload, workload or GPU test. It exits and cleans
up the temporary state.

Local engineering evidence on 2026-09-14 used Go 1.23.2 and system SQLite 3.46.1 through an
external temporary CGo `database/sql` adapter because module/toolchain downloads were
unavailable. The new Store/statefs code passed vet, race tests, 25 repeated tests and the
executable smoke. Neither the adapter nor its temporary modfile is committed, and that run
is not represented as testing modernc or a CGo-free production binary.

The existing offline CI checks the actual pinned modernc driver with Go 1.27.1, full-repo
contracts/vet/tests and Linux race checks, plus native Linux/macOS/Windows tests and
CGo-free builds. Its trigger now includes SQL migration assets. No provider credentials,
Kaggle API calls, GPU allocation or deployment participate.

## Dependency review

The driver is the only new direct dependency; its tagged source declares BSD-3-Clause and
requires the exact libc version pinned above. See the primary-source references in
[ADR-0008](decisions/0008-sqlite-durability-and-backup.md). Transitive module identities are
locked in `go.mod`/`go.sum`; keep original distribution notices when building release SBOMs.
The final release-artifact license inventory remains M6 rather than a claim that every
upstream test/tool module is linked into the runtime.
