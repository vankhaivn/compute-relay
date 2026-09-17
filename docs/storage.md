# Durable storage and recovery

SQLite stores installation/workspace authority, object metadata, jobs/attempts, original receipts,
queue/intent/resource journals, collection publications and retention history. Blob payloads live
in separate private filesystem roots, not the database. Provider credential values are not stored.
The normal host binds these stores with its own [installation marker](local-runtime.md).

## Ownership and transactions

A state directory has one OS-lock owner. Do not remove `runtime.lock`, `.installation`,
`.compute-relay-state` or restore blockers to force an open. Root/database/sidecar symlinks and
unsafe file types/permissions are rejected. Original and restored copies must never run concurrently
against the same provider identity. Network/shared filesystem guarantees are not established.

The pinned CGo-free SQLite stack uses WAL/FULL, foreign keys, trusted-schema restrictions,
immediate short transactions and one private connection. Default ordinary operations have
five-second contexts, backup one minute and busy timeout one second. Filesystem/OS calls still
must cooperate; a deadline is not a hard real-time disk guarantee. Exact versions are in `go.mod`.

No transaction spans provider or blob I/O. State/events and related receipts/ownership updates
commit together at each local boundary. Busy/full/corrupt/conflict/incompatible errors remain
distinct. A commit error is not permission to replay a transaction blindly.

## Migrations and history

Embedded migrations 1–9 supply the current repositories. Applied version/name/bytes are checked
against their checksummed ledger. Add a new migration instead of changing an applied one.
Reject newer/foreign/tampered/gapped state; do not reset it. Restore/open may upgrade supported
older schemas; arbitrary downgrade is unsupported.

Job/attempt/operation/event history, object/artifact metadata, receipts, publications and resource
ledgers remain retained. Byte expiry does not prune these records. Input and result store identities
are bound separately; a replacement path does not inherit deletion authority.

## Database-only backup API

`Store.Backup(ctx, newBackupDir)` creates a consistent `VACUUM INTO` snapshot and writes a
receipt after flushing and validating it. `Restore(ctx, backupDir, newStateDir, options)` requires
a completely new destination and verifies checksum/schema/identity/integrity before publication.
The bundle contains `metadata.sqlite` and `backup.json`; missing receipts, sidecars, changed bytes,
excessive size or unsupported schemas fail. These are Go composition APIs, **not a backup CLI**.
Default snapshot ceiling is 256 MiB, explicitly configurable within implementation bounds.

A database snapshot does not contain input/result bytes, acceptance process markers or future
revocations/expiry facts. Checksums detect corruption, not malicious replacement of both data
and receipt. Preserve interrupted output for diagnosis; it is not a successful backup.

## Coordinated recovery

1. Preserve the original state, all matching blob roots and identity/process markers. A plain
   copy of a live database can omit committed WAL state.
2. Quiesce writers and retention. Take the database snapshot and separately preserve every
   referenced blob, including unpublished collection recovery files and root `.retention-id`.
3. Stop the original installation. Restore into a new private directory with compatible binary,
   migrations and complete matching stores; inspect identity, bytes and current authority before use.
4. Review grants/tokens/expiry restored from older snapshots. Do not rewrite markers or bind old
   tombstones to unrelated roots. Missing receipts/bytes do not prove remote work never started.

No library call here automates a whole-installation checkpoint or stops remote compute. Retain
original compatible binaries/configuration for source-bound provider recovery. See
[retention](retention.md), [collection](collection.md) and [recovery](recovery.md).
