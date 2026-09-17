# ADR-0008: SQLite identity, migrations and database-only backup

Status: accepted. Requirements: DUR-01/04, DX-01, VER-02.

## Decision

Use the pinned CGo-free driver with aligned dependencies, WAL/FULL, foreign keys and short
immediate transactions on a private connection. Own the state directory with an OS lock and
persistent installation guards. Reject incompatible/tampered/foreign state instead of resetting it.
Applied migrations are immutable and checksummed; schema/history updates commit together.

Backup produces a consistent database snapshot with a flushed validated receipt. Restore requires
a new private destination, checks bytes/schema/installation and preserves identity. An incomplete
restore must not accidentally initialize a new installation. Do not downgrade or replay uncertain
commits automatically.

## Reason and consequences

Local durability cannot be replaced by a memory fallback or a copy of a live database that ignores
WAL. A database-only snapshot lacks input/result bytes and later grants/revocation/expiry facts.
Whole-installation recovery must separately coordinate matching blob stores and identity markers,
quiesce writers and activate only one copy. Backup checksums are corruption checks, not hostile
operator attestation. Context limits do not establish arbitrary hardware power-loss behavior.

See [storage API and recovery](../storage.md), [retention](../retention.md) and the exact
[driver](https://pkg.go.dev/modernc.org/sqlite) dependencies in `go.mod`/`go.sum`.
