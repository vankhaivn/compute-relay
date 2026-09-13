# Go toolchain and developer commands

> **Task:** M2-01 — Go module and offline CI foundation.
>
> **Status:** implemented for review; this is not a runtime/API implementation.

## Baseline

- Module: `github.com/vankhaivn/compute-relay`
- Go directive and CI toolchain: `go1.27.1`
- Runtime dependencies: none at M2-01; production code uses only the Go standard library.
- CGo: disabled for the baseline build check.
- Executable: `cmd/compute-relay`, currently exposing only help and build-version output.

The SQLite driver, HTTP contract, domain model, and provider interfaces belong to later
M2/M3 tasks. They are not represented by optimistic placeholder packages in this change.

## Commands

The repository-owned Go task runner keeps behavior identical across shells:

| Task | POSIX | PowerShell | CMD | Behavior |
|---|---|---|---|---|
| Format | `./scripts/dev.sh fmt` | `./scripts/dev.ps1 fmt` | `scripts\dev.cmd fmt` | Apply `gofmt` to repository Go files. |
| Format check | `./scripts/dev.sh fmt-check` | `./scripts/dev.ps1 fmt-check` | `scripts\dev.cmd fmt-check` | Fail and list unformatted files. |
| Module check | `./scripts/dev.sh mod-check` | `./scripts/dev.ps1 mod-check` | `scripts\dev.cmd mod-check` | Require `go mod tidy -diff` and `go mod verify` to pass. |
| Vet | `./scripts/dev.sh vet` | `./scripts/dev.ps1 vet` | `scripts\dev.cmd vet` | Run standard-library static analysis with `go vet`. |
| Unit tests | `./scripts/dev.sh test` | `./scripts/dev.ps1 test` | `scripts\dev.cmd test` | Run `go test ./...`. |
| Race tests | `./scripts/dev.sh test-race` | `./scripts/dev.ps1 test-race` | `scripts\dev.cmd test-race` | Run the race detector where supported. |
| Build | `./scripts/dev.sh build` | `./scripts/dev.ps1 build` | `scripts\dev.cmd build` | Build all packages with `CGO_ENABLED=0` and `-trimpath`. |
| Required checks | `./scripts/dev.sh check` | `./scripts/dev.ps1 check` | `scripts\dev.cmd check` | Format, module, vet, unit-test, and build checks. |

GNU Make and Docker are not prerequisites.

## CI evidence

`.github/workflows/go.yml` runs:

1. format/module/vet/unit/build checks and the race detector on Linux;
2. native unit tests and CGo-free builds on GitHub-hosted Linux, macOS, and Windows; and
3. an exact Go toolchain assertion before checks.

A native CI pass establishes the module/tooling baseline on the hosted runner image. It does
not yet establish SQLite locking, credential permissions, provider-client installation, or
full release support on that OS.

## Dependency and license inventory

| Component | Version | Purpose | License/source |
|---|---|---|---|
| Go toolchain | `1.27.1` | Compiler, standard library, formatter, vet, test, race, build. | BSD-style Go license; official Go distribution. |
| `actions/checkout` | commit `3d3c42e5...` (`v7.0.1`) | CI source checkout. | MIT; official GitHub action. |
| `actions/setup-go` | commit `b7ad1dad...` (`v7.0.0`) | Install exact Go toolchain in CI. | MIT; official GitHub action. |

There are no entries in `require` and therefore no `go.sum` at M2-01. The first external Go
module must be version-pinned, license-reviewed, and justified in the change that introduces
it. ADR-0003 already requires exact `modernc.org/sqlite`/`modernc.org/libc` alignment when
the storage task begins.

## Build metadata

Release automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults remain explicit (`dev`, `unknown`, `unknown`) so a local build never invents release
provenance.
