# Go toolchain and developer commands

Build and check the repository with the committed toolchain and dependency locks. For
application usage, start with [the README](../../README.md). For operator/provider evidence,
use the [validation checklist](validation-checklist.md), not an old CI log.

## Build

The module is `github.com/vankhaivn/compute-relay`. `go.mod` and CI pin **Go 1.27.1**.
The runtime supports a CGo-free build; the race detector has its own host/compiler requirements.

```text
go version
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay --help
```

Use `compute-relay.exe` on Windows. Record a missing/unavailable pinned toolchain as an
environment blocker; do not change `go.mod` to make an operator validation run pass.
GNU Make and Docker are not prerequisites.

The main executable includes local administration, loopback serving and application commands.
Serving is still admission-only: it does not start provider or collection workers. See
[current status](../status.md) and [local runtime](../local-runtime.md).

## Checks

Run from the repository root. The shell-independent form is `go run ./cmd/devtool TASK`.
The wrappers `./scripts/dev.sh TASK`, `./scripts/dev.ps1 TASK` and `scripts\dev.cmd TASK`
forward the same tasks on POSIX, PowerShell and CMD.

| Task | Behavior |
|---|---|
| `fmt` / `fmt-check` | Apply Go formatting / reject formatting differences. |
| `mod-check` | Run `go mod tidy -diff` and `go mod verify`; do not silently update dependencies. |
| `contract-check` | Validate schemas, examples, OpenAPI, local references and the contract lock. |
| `contract-lock` | Rewrite the contract lock after an intentional reviewed contract change. |
| `vet` / `test` | Run repository vet / unit and component tests. |
| `test-race` | Run repository tests with the race detector on a supported host. |
| `fault-test` | Run the exact nominated fault tests uncached and reject missing/skipped evidence. |
| `build` | Build all packages with `CGO_ENABLED=0` and `-trimpath`. |
| `check` | Format, module, contract, vet, test, fault-test and build checks. Race tests remain separate. |

```text
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

The [fault matrix](../fault-matrix.md) owns scenario/test traceability and evidence limits.
Its result is not a coverage percentage or a live-provider qualification. Do not rewrite
locks, weaken tests or replace the approved scenario list to obtain a green run.

Finite component smoke commands include `go run ./cmd/storesmoke`, `./cmd/admissionsmoke`,
`./cmd/schedulersmoke` and `./cmd/dispatchsmoke` (each path is an argument to `go run`).
They use disposable local state and synthetic provider behavior. They do not execute a
submitted user workload, allocate GPU or qualify a production service.

## Dependency changes and CI

`go.mod`/`go.sum` are authoritative. Direct dependencies are kin-openapi `v0.149.0`,
jsonschema/v6 `v6.0.3` and modernc SQLite `v1.58.0`; the driver requires the pinned libc
`v1.75.6`. Keep driver/libc compatible and preserve distribution licenses. See
[ADR-0008](../decisions/0008-sqlite-durability-and-backup.md) for source references.
Review migrations, locking, backup and native tests when changing the storage stack.

Existing Go CI runs checks/race on Linux and native tests/CGo-free builds on Linux, macOS
and Windows. Separate workflows check the locked [Kaggle client](../../tools/kaggle-client/README.md)
and [runner](../../runner/README.md). CI uses no live account credential or GPU. A pass
applies to the actual runner/toolchain tested, not every architecture or filesystem.
Put exact-head run URLs and temporary engineering-harness limitations in the PR, not here.

## Build metadata

Build automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults are `dev`, `unknown`, `unknown`; they are not release provenance. For the multi-process
GPU acceptance procedure, build its separate executable once and retain its exact bytes as
specified in [the acceptance guide](../providers/kaggle-acceptance.md).
