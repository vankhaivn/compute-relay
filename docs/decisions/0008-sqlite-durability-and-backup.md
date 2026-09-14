# ADR-0008: SQLite durability, ordered migrations and database-only backup

- **Status:** accepted
- **Date:** 2026-09-14
- **Task:** M3-01
- **Requirements:** DUR-01, DUR-04, DX-01; reuse the M2-05/M2-06 auth/object contracts
- **Extends:** ADR-0003 and ADR-0004 without changing approved product boundaries

## Context

The portable core already separates byte publication from ownership metadata. It needs a
real local metadata store before durable job admission can be built. A database driver
alone is insufficient: process ownership, schema identity, transaction rollback and a
consistent recovery copy must be explicit.

## Decision

Use `database/sql` with `modernc.org/sqlite v1.58.0` and its matching
`modernc.org/libc v1.75.6`, selected from the driver's tagged `go.mod` [S1]. Keep Go 1.27.1
and the CGo-free production build. The driver license is BSD-3-Clause [S2]. There is no
alternate production driver, external database service or shell-based SQLite fallback.
Exact transitive versions/checksums are in `go.mod` and `go.sum`.

Configure each connection with foreign keys, synchronous FULL, private cache, a 1,000 ms
busy timeout, trusted schema disabled, and a 1,000-page WAL auto-checkpoint. Select WAL
only after inspecting database application/schema identity. Use one pooled connection and
`_txlock=immediate`; ordinary operations have a five-second default context budget,
including pool wait. Backups have a separate one-minute default. These are local policy
values, not a service-level guarantee. Do not retry a failed commit automatically.

`internal/statefs` holds a real OS lock from before opening SQLite until all database
handles close. It uses flock on Linux/macOS and LockFileEx on Windows, never a PID file.
The lock inode remains in place after release. Dedicated state directories and sidecars
must have private Unix modes or a conservative Windows DACL. Existing permissions are
validated rather than silently rewritten. This state lock is separate from blobfs's lock.

Embed ordered SQL migrations. Record each version, name, SHA-256 and application time;
update the ledger and `user_version` in the same transaction as DDL. Reject newer versions,
gaps, changed migration bytes and foreign databases. Initial migrations are additive:
installation identity, workspaces, token hashes and immutable object metadata. Do not add
empty job/attempt/queue tables before their admission semantics are implemented.

Persist a random installation identity in SQLite and a small private identity guard next
to it. Missing/empty/replaced databases in an initialized root must fail, not silently
become another installation. Startup checks integrity and foreign keys. Interrupted restore
staging is a blocker, not a reason to initialize fresh state.

For backups, use SQLite `VACUUM INTO` to produce a consistent snapshot [S3], not a copy of
a live `runtime.db` that ignores WAL. Precreate the target privately, check a configurable
size ceiling (256 MiB default), convert the snapshot to standalone DELETE mode, flush it,
verify schema/integrity, and publish a checksum-bearing receipt last. Release the source
connection before hashing the snapshot. The backup is explicitly **database-only**.

Restore is offline into a newly created directory, never over an existing destination.
Validate the receipt, bounds, hash, application/migration history, identity and integrity
before publishing `runtime.db`. Preserve the installation identity. A late publication
flush error is uncertain: retain the published data instead of deleting it speculatively.

## Alternatives and consequences

- CGo-based SQLite would add a native build dependency; the approved pure-Go family avoids
  that in production. A temporary local system-SQLite test adapter is not shipped.
- An external migration framework is unnecessary for two ordered embedded SQL migrations.
  Future migrations must not edit applied files; destructive changes require a reviewed
  backup/recovery procedure before execution.
- The SQLite online backup API supports incremental copying. `VACUUM INTO` is the smaller
  baseline here; it temporarily occupies the single connection and can make other bounded
  operations time out. Large/incremental backups are not claimed.
- FULL/WAL is selected for committed-state durability [S4], subject to actual OS/filesystem
  and hardware behavior. Windows file flushes are implemented, but portable directory-sync
  or arbitrary sudden-power-loss guarantees are not claimed. Use operator-owned local
  filesystems; network filesystems and hostile same-user mutation are outside this proof.
- A metadata snapshot does not include input/artifact bytes or provider credentials. It is
  sensitive because it contains workspace records and token digests. Restoring an old
  snapshot may undo later revocations. Review/rotate tokens before exposing restored state.
- Never run an original and restored installation concurrently against the same provider
  resources. Reopening a database does not submit, cancel or clean up remote work.

## Verification

Component tests cover real database reopen, token revocation, scoped metadata, uniqueness
and foreign keys, migration rollback/tampering/newer versions, disk-full rollback,
connection-pool deadlines, concurrency, and an actual child-process kill during an
uncommitted transaction. Backup tests cover committed WAL contents, corrupt/incomplete
snapshots, size limits, independent writers and refusal to overwrite live or empty targets.

Native CI exercises the pinned driver on Linux/macOS/Windows with CGo-free builds. Local
Linux validation additionally exercised the same storage code against system SQLite 3.46.1
through an external, non-shipped CGo adapter because Go 1.27.1/module downloads were blocked.
That local run is not labeled modernc evidence. See [`../storage.md`](../storage.md).

M3-02 owns job/attempt admission and idempotency. M3-03 onward owns scheduling, submission
intent and orchestration recovery. M5 owns production `serve` and administrative CLI wiring.

## Sources reviewed

[S1]: https://github.com/modernc-org/sqlite/blob/v1.58.0/go.mod
[S2]: https://github.com/modernc-org/sqlite/blob/v1.58.0/LICENSE
[S3]: https://www.sqlite.org/lang_vacuum.html#vacuum_with_an_into_clause
[S4]: https://www.sqlite.org/pragma.html#pragma_synchronous
