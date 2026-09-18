# ADR-0003: Go and CGo-free SQLite

Status: accepted. Requirements: PRD-05, DUR-01/04, DX-01, VER-01.

## Decision

Use Go for the control plane, standard-library HTTP/context/concurrency where adequate, and
`database/sql` with the `modernc.org/sqlite` driver family. Preserve the exact toolchain, driver
and matching libc dependency pins in `go.mod`/`go.sum`; do not select versions from memory.
Embed ordered migrations and use explicit short transactions with no provider or long-transfer I/O.

Direct builds must not require Docker or a C compiler solely for SQLite. Native tests are needed
before host claims; a cross-compile does not establish locking, permissions or durability.
The initial toolchain baseline is Go 1.27.1, not a statement about the latest upstream release.

## Reason and consequences

Mandatory CGo complicates the direct cross-platform workflow; an external database/process adds
infrastructure the local runtime does not need. The selected driver adds generated dependency
and binary weight and requires alignment discipline. SQLite remains single-owner local state,
not a distributed database. Driver changes require a reviewed superseding decision.

See [storage](../storage.md), [compatibility](../../compatibility.md),
[Go release policy](https://go.dev/doc/devel/release) and
[driver source](https://gitlab.com/cznic/sqlite).
