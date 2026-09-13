# ADR-0003: Use the current Go toolchain and a CGo-free SQLite driver

- **Status:** accepted
- **Date:** 2026-09-13
- **Decision owners:** repository owner and implementation maintainers
- **Related requirements:** PRD-05, DUR-01, DUR-04, DX-01, VER-01
- **Related sources:** [Go release history](https://go.dev/doc/devel/release),
  [`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite)

## Context

The approved architecture requires a Go control plane, SQLite metadata storage, native
direct-install builds for Linux/macOS/Windows, and Docker only as an option. A driver that
requires a platform C compiler would complicate the primary build path and cross-platform
contributor experience.

As of 2026-09-13, Go 1.27.1 is the current stable patch release. The
`modernc.org/sqlite` project provides a `database/sql` driver based on a CGo-free SQLite
port and documents the project's required target operating-system/architecture families.
Its documentation warns that the matching `modernc.org/libc` revision must be kept aligned.

## Decision drivers

- Current supported Go toolchain and standard compatibility policy.
- Native direct builds without requiring Docker or a local C compiler.
- One common SQLite behavior across claimed host targets.
- `database/sql` integration and explicit transaction ownership.
- Reproducible dependency versions and native platform validation.
- A small dependency surface with no external database service.

## Considered options

### Option A — `mattn/go-sqlite3`

Mature and widely used, but its normal build path requires CGo and a C toolchain. That
conflicts with the simplest direct-install/cross-platform baseline.

### Option B — `modernc.org/sqlite`

CGo-free `database/sql` driver, suitable for the target host families. It has a substantial
generated dependency graph and requires exact `modernc.org/libc` version alignment.

### Option C — External SQLite process or server database

Would add process/network/deployment complexity and violate the no-mandatory-service
baseline.

## Decision

Use:

- Go `1.27.1` as the initial module/toolchain baseline;
- standard-library `net/http`, `log/slog`, context, subprocess, and concurrency facilities
  where adequate;
- `database/sql` with `modernc.org/sqlite` as the initial SQLite driver family;
- an exact driver and matching transitive `modernc.org/libc` pin when `go.mod` is created;
- ordered SQL migrations embedded in the Go binary;
- explicit transaction helpers and no transaction held over network requests or long file
  transfers; and
- native tests before claiming Linux `amd64/arm64`, macOS `amd64/arm64`, or Windows
  `amd64` support.

The exact driver release is intentionally pinned by implementation task M2-01 after license,
API, platform, and current release checks are executed in the actual module. Selecting a
family here does not authorize an unreviewed version from memory.

## Consequences

### Positive

- Direct builds do not require CGo or a C compiler solely for SQLite.
- The local metadata store remains embedded and operator-controlled.
- Cross-platform build/release automation is simpler.
- The driver stays behind the store port and can be replaced by a superseding ADR.

### Negative or limiting

- The pure-Go SQLite port and generated libc layer add binary/dependency weight.
- Exact transitive alignment needs dependency tests and update discipline.
- A successful cross-compile does not establish native locking, durability, backup, or
  permission behavior.
- SQLite remains a single-state-directory/single-runtime-process design, not a distributed
  database.

### Follow-up

- M2-01 creates `go.mod`, pins exact dependencies, records licenses, and adds toolchain
  commands.
- M3-01 defines connection pragmas, migrations, busy/locking policy, durability mode, and
  backup strategy with tests.
- M5/M6 run native platform matrices before support claims.
- A superseding ADR is required if driver limitations block a required target.

## Verification

- `go mod verify`, dependency/license review, formatting, vet/static analysis, tests, and
  reproducible builds run in CI.
- Native component tests cover migrations, concurrent claims, restart, disk-full/failure
  injection where feasible, online-consistent backup, and state-directory locking.
- No required build command relies on Docker, GNU Make, CGo, or an external database.
- The exact `modernc.org/libc` revision matches the selected driver's module requirements.

## Rejected alternatives

Option A is not selected for the baseline because mandatory CGo increases local and release
toolchain requirements. Option C is rejected because it adds infrastructure the MVP does
not need. Both may be reconsidered through a superseding ADR if measured evidence shows the
selected driver cannot satisfy required correctness or platform behavior.
