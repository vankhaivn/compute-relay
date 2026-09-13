# Go toolchain and developer commands

> **Origin:** M2-01 Go module/offline CI foundation; updated by M2-03 contract validation.
>
> **Status:** implemented offline. Runtime/API/provider behavior remains scoped to its own
> implementation tasks.

## Baseline

- Module: `github.com/vankhaivn/compute-relay`
- Go directive and CI toolchain: `go1.27.1`
- CGo: disabled for the baseline build check.
- Executable: `cmd/compute-relay`, currently exposing only help and build-version output.
- Contract tool: `cmd/contractcheck`, for repository-local JSON Schema/OpenAPI validation.

The SQLite driver, HTTP server, application services, and provider interfaces belong to
later M2/M3 tasks. A contract file marked `planned` is not endpoint implementation.

## Commands

The repository-owned Go task runner keeps behavior identical across shells:

| Task | POSIX | PowerShell | CMD | Behavior |
|---|---|---|---|---|
| Format | `./scripts/dev.sh fmt` | `./scripts/dev.ps1 fmt` | `scripts\dev.cmd fmt` | Apply `gofmt` to repository Go files. |
| Format check | `./scripts/dev.sh fmt-check` | `./scripts/dev.ps1 fmt-check` | `scripts\dev.cmd fmt-check` | Fail with a formatting diff. |
| Module check | `./scripts/dev.sh mod-check` | `./scripts/dev.ps1 mod-check` | `scripts\dev.cmd mod-check` | Require `go mod tidy -diff` and `go mod verify` to pass. |
| Contract check | `./scripts/dev.sh contract-check` | `./scripts/dev.ps1 contract-check` | `scripts\dev.cmd contract-check` | Compile schemas, validate fixtures/OpenAPI/domain enums/local refs, and verify the contract lock. |
| Contract lock | `./scripts/dev.sh contract-lock` | `./scripts/dev.ps1 contract-lock` | `scripts\dev.cmd contract-lock` | Atomically rewrite the reviewed JSON contract lock. |
| Vet | `./scripts/dev.sh vet` | `./scripts/dev.ps1 vet` | `scripts\dev.cmd vet` | Run `go vet`. |
| Unit tests | `./scripts/dev.sh test` | `./scripts/dev.ps1 test` | `scripts\dev.cmd test` | Run `go test ./...`. |
| Race tests | `./scripts/dev.sh test-race` | `./scripts/dev.ps1 test-race` | `scripts\dev.cmd test-race` | Run the race detector where supported. |
| Build | `./scripts/dev.sh build` | `./scripts/dev.ps1 build` | `scripts\dev.cmd build` | Build all packages with `CGO_ENABLED=0` and `-trimpath`. |
| Required checks | `./scripts/dev.sh check` | `./scripts/dev.ps1 check` | `scripts\dev.cmd check` | Format, module, contract, vet, unit-test, and build checks. |

GNU Make and Docker are not prerequisites.

## CI evidence

`.github/workflows/go.yml` runs only offline code-quality/build/test work:

1. format/module/contract/vet/unit/build checks and the race detector on Linux;
2. native unit tests and CGo-free builds on GitHub-hosted Linux, macOS, and Windows; and
3. an exact Go toolchain assertion before checks.

The workflow watches `api/**` so a schema-only change cannot bypass contract validation. It
does not authenticate a provider, create remote resources, deploy the runtime, or allocate
GPU compute.

A native CI pass establishes the checked module/tooling behavior on the hosted runner image.
It does not yet establish SQLite locking, credential permissions, provider-client
installation, or full release support on that OS.

## Dependency and license inventory

| Component | Version | Purpose | License/source |
|---|---|---|---|
| Go toolchain | `1.27.1` | Compiler, standard library, formatter, vet, test, race, build. | BSD-style Go license; official Go distribution. |
| `github.com/getkin/kin-openapi` | `v0.149.0` | Parse/resolve/validate the committed OpenAPI 3.1 document offline. | MIT; pinned upstream release. |
| `github.com/santhosh-tekuri/jsonschema/v6` | `v6.0.3` | Compile Draft 2020-12 schemas and validate examples with format assertions. | Apache-2.0; pinned upstream release. |
| `actions/checkout` | commit `3d3c42e5...` (`v7.0.1`) | CI source checkout. | MIT; official GitHub action. |
| `actions/setup-go` | commit `b7ad1dad...` (`v7.0.0`) | Install exact Go toolchain in CI. | MIT; official GitHub action. |

Contract-validation dependencies are development/build dependencies imported only by
`internal/contracts` and `cmd/contractcheck`; the `compute-relay` executable does not import
them. Exact transitive module identities are recorded in `go.sum` and verified by
`mod-check`.

ADR-0003 still requires exact `modernc.org/sqlite`/`modernc.org/libc` alignment when the
storage task begins.

## Build metadata

Release automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults remain explicit (`dev`, `unknown`, `unknown`) so a local build never invents release
provenance.
