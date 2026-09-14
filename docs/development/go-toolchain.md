# Go toolchain and developer commands

> **Origin:** M2-01 Go module/offline CI foundation; updated through M3-01 storage.
>
> **Status:** implemented offline. Runtime/API/provider behavior remains scoped to its own
> implementation tasks.

## Baseline

- Module: `github.com/vankhaivn/compute-relay`
- Go directive and CI toolchain: `go1.27.1`
- CGo: disabled for the baseline build check, including the SQLite store and smoke command.
- Executable: `cmd/compute-relay`, with help/version and explicit bundle commands.
- Contract tool: `cmd/contractcheck`, for repository-local JSON Schema/OpenAPI validation.
- Metadata smoke: `cmd/storesmoke`, using temporary SQLite, synthetic tokens and database-only backup/restore.

HTTP, input and provider interfaces now have offline components. Production `serve`,
durable job admission and orchestration remain later tasks. A contract marked `planned`
is not endpoint implementation.

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

GNU Make and Docker are not prerequisites. The finite metadata smoke is shell-independent:

```text
go run ./cmd/storesmoke
```

## CI evidence

`.github/workflows/go.yml` runs only offline code-quality/build/test work:

1. format/module/contract/vet/unit/build checks and the race detector on Linux;
2. native unit tests and CGo-free builds on GitHub-hosted Linux, macOS, and Windows; and
3. an exact Go toolchain assertion before checks.

The workflow watches `api/**` and embedded SQLite migration paths, so schema-only changes
cannot bypass their checks. It does not authenticate a provider, create remote resources,
deploy the runtime, or allocate GPU compute.

M3-01 adds native SQLite/state-lock and private-permission component tests. A CI pass is
still scoped to the hosted runner image and tested operations, not every filesystem,
provider-client installation, power-loss scenario or full release support claim.

## Dependency and license inventory

| Component | Version | Purpose | License/source |
|---|---|---|---|
| Go toolchain | `1.27.1` | Compiler, standard library, formatter, vet, test, race, build. | BSD-style Go license; official Go distribution. |
| `github.com/getkin/kin-openapi` | `v0.149.0` | Parse/resolve/validate the committed OpenAPI 3.1 document offline. | MIT; pinned upstream release. |
| `github.com/santhosh-tekuri/jsonschema/v6` | `v6.0.3` | Compile Draft 2020-12 schemas and validate examples with format assertions. | Apache-2.0; pinned upstream release. |
| `modernc.org/sqlite` | `v1.58.0` | CGo-free SQLite metadata store. | BSD-3-Clause; tagged source/license reviewed in ADR-0008. |
| `modernc.org/libc` | `v1.75.6` | Exact driver-required runtime dependency. | Version follows the driver's tagged `go.mod`; preserve upstream distribution notices. |
| `actions/checkout` | commit `3d3c42e5...` (`v7.0.1`) | CI source checkout. | MIT; official GitHub action. |
| `actions/setup-go` | commit `b7ad1dad...` (`v7.0.0`) | Install exact Go toolchain in CI. | MIT; official GitHub action. |

Contract validators remain development/build dependencies. SQLite is imported by the store
and metadata smoke command, not a production `serve` implementation yet. Exact transitive
module identities/checksums are recorded in `go.mod` and `go.sum` and verified by `mod-check`.
Do not change SQLite/libc independently; rerun migration, locking, backup and native tests.
See [storage](../storage.md) and [ADR-0008](../decisions/0008-sqlite-durability-and-backup.md).

## Build metadata

Release automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults remain explicit (`dev`, `unknown`, `unknown`) so a local build never invents release
provenance.
