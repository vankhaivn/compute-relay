# Go toolchain and developer commands

> **Origin:** M2-01 Go module/offline CI foundation; updated through M3-08 qualification.
>
> **Status:** implemented offline. See the [implementation plan](../implementation-plan.md)
> for the current owner-review boundary. Production composition and live provider evidence
> remain separate tasks.

## Baseline

- Module: `github.com/vankhaivn/compute-relay`
- Go directive and CI toolchain: `go1.27.1`
- CGo: disabled for the baseline build check, including SQLite and orchestration components.
- Executable: `cmd/compute-relay`, with help/version and explicit bundle commands.
- Contract tool: `cmd/contractcheck`, for repository-local JSON Schema/OpenAPI validation.
- Metadata smoke: `cmd/storesmoke`, using temporary SQLite, synthetic tokens and database-only backup/restore.
- Admission smoke: `cmd/admissionsmoke`, using temporary SQLite for concurrent/restarted receipt replay and conflict checks.
- Scheduler/dispatch smokes: `cmd/schedulersmoke` and `cmd/dispatchsmoke`, using temporary state and nonexecuting synthetic provider evidence.
- Fault qualification: `cmd/devtool fault-test`, requiring fresh named-test evidence for all 25 proposal scenarios.

HTTP, input, durable admission, scheduling, one-shot dispatch/recovery, controls, verified
collection and pin-aware retention have offline components. Production `serve`, complete
artifact HTTP/CLI and live provider integration remain separate gates. A contract marked
`planned` is not endpoint implementation.

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
| Fault qualification | `./scripts/dev.sh fault-test` | `./scripts/dev.ps1 fault-test` | `scripts\dev.cmd fault-test` | Validate the 25-case catalog and run its 34 named tests uncached; missing/skipped/failed evidence is an error. |
| Build | `./scripts/dev.sh build` | `./scripts/dev.ps1 build` | `scripts\dev.cmd build` | Build all packages with `CGO_ENABLED=0` and `-trimpath`. |
| Required checks | `./scripts/dev.sh check` | `./scripts/dev.ps1 check` | `scripts\dev.cmd check` | Format, module, contract, vet, unit-test, fault-qualification and build checks. |

GNU Make and Docker are not prerequisites. The finite developer checks are shell-independent:

```text
go run ./cmd/storesmoke
go run ./cmd/admissionsmoke
go run ./cmd/schedulersmoke
go run ./cmd/dispatchsmoke
go run ./cmd/devtool fault-test
```

`fault-test` checks exact complete scenario text and numbering in proposal section 23.2,
then validates source/test identities and consumes fresh `go test -json -count=1` events.
It requires every nominated root to run/pass and every required package to finish. Skipped
roots or descendants, malformed/incomplete output and exceeded budgets fail qualification.
An ordinary green package or a saved test log is insufficient. The invocation is bounded
by 15 minutes, each package by five minutes, each JSON event by 1 MiB and total stdout by
64 MiB. This is developer qualification, not a remote workload timeout policy.

See the [fault matrix](../fault-matrix.md) for exact tests, requirements and evidence limits,
and [recovery semantics](../recovery.md) for operational interpretation. The qualification
runner does not replace the full unit/race suites or prove that assertions cover every
possible failure. Catalog edits still require semantic review.

## CI evidence

`.github/workflows/go.yml` runs only offline code-quality/build/test work:

1. format/module/contract/vet/unit/fault-qualification/build checks and the race detector on Linux;
2. native unit tests and CGo-free builds on GitHub-hosted Linux, macOS, and Windows; and
3. an exact Go toolchain assertion before checks.

The workflow watches `api/**` and embedded SQLite migration paths, so schema-only changes
cannot bypass their checks. It does not authenticate a provider, create remote resources,
deploy the runtime, or allocate GPU compute. M3-08 adds qualification to the existing
`check` command rather than adding a provider-enabled workflow.

M3 includes native SQLite/state-lock, permissions, admission, dispatch, collection, retention,
crash and HTTP component tests. A CI pass is scoped to the hosted runner image and tested
operations, not every filesystem, provider-client installation, power-loss scenario or full
release support claim. Exact final-head CI evidence belongs in the task PR. Local isolated
checker/parser tests on Go 1.23.2 are not full Go 1.27.1/modernc integration or local fault
qualification; the matrix guide and PR distinguish those evidence tiers.

## Dependency and license inventory

| Component | Version | Purpose | License/source |
|---|---|---|---|
| Go toolchain | `1.27.1` | Compiler, standard library, formatter, vet, test, race, build. | BSD-style Go license; official Go distribution. |
| `github.com/getkin/kin-openapi` | `v0.149.0` | Parse/resolve/validate the committed OpenAPI 3.1 document offline. | MIT; pinned upstream release. |
| `github.com/santhosh-tekuri/jsonschema/v6` | `v6.0.3` | Draft 2020-12 contract checks and embedded runtime validation. | Apache-2.0; existing pinned release. |
| `modernc.org/sqlite` | `v1.58.0` | CGo-free SQLite metadata store. | BSD-3-Clause; tagged source/license reviewed in ADR-0008. |
| `modernc.org/libc` | `v1.75.6` | Exact driver-required runtime dependency. | Version follows the driver's tagged `go.mod`; preserve upstream distribution notices. |
| `actions/checkout` | commit `3d3c42e5...` (`v7.0.1`) | CI source checkout. | MIT; official GitHub action. |
| `actions/setup-go` | commit `b7ad1dad...` (`v7.0.0`) | Install exact Go toolchain in CI. | MIT; official GitHub action. |

OpenAPI validation remains a development/build dependency. Admission and collection use the
existing JSON Schema validator with embedded schemas, not remote resources. SQLite supplies
the durable components and developer fixtures. None of these components invents a production
`serve` implementation. M3-08 adds no dependency or toolchain change.

Exact transitive identities/checksums remain in `go.mod`/`go.sum` and are verified by
`mod-check`. Do not change SQLite/libc independently; rerun migration, locking, backup and
native tests. See [storage](../storage.md), [admission](../admission.md),
[ADR-0008](../decisions/0008-sqlite-durability-and-backup.md) and
[ADR-0009](../decisions/0009-durable-idempotent-admission.md).

## Build metadata

Release automation may inject these variables with `-ldflags -X`:

```text
github.com/vankhaivn/compute-relay/internal/buildinfo.Version
github.com/vankhaivn/compute-relay/internal/buildinfo.Commit
github.com/vankhaivn/compute-relay/internal/buildinfo.BuiltAt
```

Defaults remain explicit (`dev`, `unknown`, `unknown`) so a local build never invents release
provenance.
