# Go toolchain and developer commands

Build and check the repository with the committed toolchain and dependency locks. For
application usage, start with [the README](../../README.md).

## Build

The module is `github.com/vankhaivn/compute-relay`. `go.mod` and CI pin **Go 1.27.1**.
The runtime supports a CGo-free build; the race detector has its own host/compiler requirements.

```text
go version
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay --help
```

Use `compute-relay.exe` on Windows. Do not change `go.mod` to make a validation run pass.
GNU Make and Docker are not prerequisites.

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

The [fault matrix](fault-matrix.md) owns scenario/test traceability and evidence limits.
Its result is not a coverage percentage or a live-provider qualification. Do not rewrite
locks, weaken tests or replace the approved scenario list to obtain a green run.

## Dependency changes and CI

`go.mod`/`go.sum` are authoritative. Direct dependencies are kin-openapi `v0.149.0`,
jsonschema/v6 `v6.0.3` and modernc SQLite `v1.58.0`; the driver requires the pinned libc
`v1.75.6`. Keep driver/libc compatible and preserve distribution licenses. See
[ADR-0008](decisions/0008-sqlite-durability-and-backup.md) for source references.
Review migrations, locking, backup and native tests when changing the storage stack.

Go CI always runs the full ordinary checks and CGo-free build on Linux when relevant source,
embedded assets or CI configuration change. PRs also run the full race suite for runtime,
provider, storage and shared-core changes; presentation/CLI, packaging and contract-only changes
use ordinary tests. Native macOS tests/builds run for platform, filesystem, credential, storage,
packaging, CLI and Kaggle-process changes. Dependencies and CI selection changes exercise both
extra checks. Relevant pushes to `main` run both regardless of the PR classification. The exact
path policy and its regression tests live in [ci_scope.py](../../.github/scripts/ci_scope.py).

Go dependency/build caches are enabled; unchanged tests can reuse Go's valid result cache.
The named fault matrix and actual runner-result contract checks still run uncached. New PR
commits cancel superseded workflow runs. Separate workflows check the locked
[Kaggle client](../../tools/kaggle-client/README.md) on Linux/macOS and the
[runner](../../runner/README.md) on Linux. Windows CI is disabled; Windows compatibility changes
require explicit native validation. CI uses no live account credential or GPU.

## Build metadata

Build automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults are `dev`, `unknown`, `unknown`; they are not release provenance.
