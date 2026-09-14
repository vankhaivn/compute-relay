# Compute Relay

> **Project status:** portable-core components and the first SQLite metadata foundation
> are implemented offline. Durable job admission, orchestration, production `serve`
> composition and the live Kaggle path remain separate implementation gates.

Compute Relay is the repository for an open-source, self-hosted **compute connector
runtime**. The intended runtime sits beside an application, accepts finite jobs through a
provider-neutral HTTP/JSON boundary, and coordinates execution on external compute
providers. Kaggle is the first provider to investigate and implement; it is not the identity
of the core product.

The approved product direction is captured in [`docs/proposal.md`](docs/proposal.md). That
proposal uses **Compute Connector** as a working name. This repository uses **Compute
Relay**; public executable, package, and API naming remain provisional until recorded in an
architecture decision.

## Product direction

The intended product will:

- run on infrastructure controlled by the operator;
- use the operator's own provider account and credentials;
- execute bounded Python or shell jobs through an explicit job contract;
- preserve durable local job and attempt state;
- stage inputs, observe execution, and retrieve verified artifacts;
- expose unsupported, unknown, or unverified provider capabilities honestly; and
- keep provider-specific behavior behind adapters so applications integrate once.

It is **not** a hosted GPU service, transparent remote VRAM, a CUDA proxy, an always-on
model server, a remote desktop, or a guarantee of immediate GPU allocation. The reference
workflow must not require a maintainer-operated service or mandatory paid infrastructure.

## Repository state

The repository currently establishes:

- approved proposal, provider-neutral architecture baseline, and stable requirement IDs;
- Apache-2.0 licensing and OSS governance/security/contribution policies;
- mandatory instructions for human and automated contributors;
- Conventional Commits documentation, local hooks, and CI validation;
- an acceptance-based roadmap and dependency-aware M-0 through M-6 implementation plan;
- a dated review of Kaggle CLI `v2.2.4` and K-01 through K-16 feasibility ledger;
- initial ADRs for the official-client boundary, attempt-scoped Kaggle identity, and
  Go/SQLite baseline;
- a risk register, compatibility target matrix, and live-verification checklist;
- Go tooling, a pre-release help/version/bundle executable, strict API schemas, domain
  state semantics, provider ports and a deterministic fake provider;
- workspace token/authorization services, guarded loopback HTTP, immutable upload/import/
  HTTPS snapshots, finite runner assets and offline smoke/fault tests; and
- SQLite installation/workspace/token-digest/object metadata, embedded migrations,
  OS state locking and consistent database-only backup/restore.

SQLite-backed workspace, token and object repositories now exist as components. Durable
job admission, scheduling and production `serve` composition are still pending. The upload
smoke retains its explicitly nondurable metadata fixture; the separate store smoke exercises
SQLite. A database-only backup does not include blob bytes or prove full runtime recovery.

Implementation claims must be backed by code, tests, and—where provider behavior is
involved—dated evidence. A passing fake-provider test will not be described as proof of
live Kaggle support.

## Documentation map

| Document | Purpose |
|---|---|
| [`docs/proposal.md`](docs/proposal.md) | Approved product direction, defaults, feasibility gates, and boundaries. |
| [`AGENTS.md`](AGENTS.md) | Mandatory repository-wide instructions for humans and coding agents. |
| [`docs/scope-and-requirements.md`](docs/scope-and-requirements.md) | Requirement-to-component-to-test traceability. |
| [`docs/architecture.md`](docs/architecture.md) | Provider-neutral architecture baseline and invariants. |
| [`docs/auth-and-objects.md`](docs/auth-and-objects.md) | Implemented workspace auth, HTTP/upload boundary, blob recovery and smoke checks. |
| [`docs/storage.md`](docs/storage.md) | SQLite repositories, locking, migrations, database-only backup/restore and evidence limits. |
| [`api/README.md`](api/README.md) | Versioned schemas and operation-level implementation status. |
| [`docs/roadmap.md`](docs/roadmap.md) | Acceptance-based M-0 through M-6 outcomes and status. |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | Dependency-aware executable task plan and authorization boundaries. |
| [`docs/research/kaggle-interface-review.md`](docs/research/kaggle-interface-review.md) | Pinned official-client source review and M-1 probe sequence. |
| [`docs/research/kaggle-feasibility.md`](docs/research/kaggle-feasibility.md) | K-01 through K-16 evidence ledger and go/no-go rule. |
| [`docs/risk-register.md`](docs/risk-register.md) | Ranked risks, predetermined responses, and evidence gates. |
| [`docs/compatibility.md`](docs/compatibility.md) | Toolchain/provider/host targets and live-verification checklist. |
| [`docs/decisions/`](docs/decisions/README.md) | Accepted and proposed architecture decisions. |
| [`docs/development/commit-convention.md`](docs/development/commit-convention.md) | Canonical commit-message rules. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contributor workflow and quality expectations. |
| [`SECURITY.md`](SECURITY.md) | Vulnerability and credential-disclosure reporting. |

See [`docs/README.md`](docs/README.md) for the complete documentation system.

## Local developer smoke

With the pinned Go toolchain, from the repository root:

```text
go run ./cmd/uploadsmoke
go run ./cmd/storesmoke
```

The upload check exercises loopback HTTP, synthetic workspace tokens, streamed upload,
digest verification, isolation, revocation and reopened temporary blobs with fixture
metadata. The store check separately exercises actual SQLite persistence, revocation and
database-only backup/restore, explicitly reporting `blob_bytes_checked=false`.

Both checks are finite, clean up their temporary files, and never call Kaggle. Neither is
a production runtime. See [`docs/auth-and-objects.md`](docs/auth-and-objects.md) and
[`docs/storage.md`](docs/storage.md) for their distinct evidence limits.

## Current execution boundary

Provider credentials and live probes belong to each operator's own runtime. GitHub Actions
are offline code-quality/build/test checks only, not deployment or GPU execution workflows.
The local Linux engineering runtime is used for executable smoke checks where available.

Live Kaggle tasks require explicit authorization for the exact credential use and finite
side effect:

- read-only authentication/quota probes;
- private synthetic staging resources;
- bounded GPU smoke/timeout/log/restart tests; and
- identity-safe test-resource cleanup.

Credentials must not be pasted into chat, committed, passed to remote jobs, or captured in
fixtures.

## Contributing

Read [`AGENTS.md`](AGENTS.md), [`docs/proposal.md`](docs/proposal.md), and
[`CONTRIBUTING.md`](CONTRIBUTING.md) before changing the repository.

Enable the versioned local Git hook and commit template with one of:

```bash
./scripts/setup-git-hooks.sh
```

```powershell
./scripts/setup-git-hooks.ps1
```

```cmd
scripts\setup-git-hooks.cmd
```

Every commit subject must follow Conventional Commits, for example:

```text
docs(repo): bootstrap open-source project
feat(provider): add deterministic fake adapter
fix(store): preserve ambiguous submission state
```

The default collaboration path is a focused branch and pull request. Direct pushes to
`main` require explicit owner authorization for the specific task.

## Security and provider responsibility

Never commit provider credentials, runtime API tokens, account exports, real job inputs, or
private artifacts. Do not paste secrets into issues or chat. Provider availability,
eligibility, quota, terms, and pricing can change; operators remain responsible for their
accounts, workloads, data rights, and provider compliance.

See [`SECURITY.md`](SECURITY.md) for private reporting guidance.

## License

Compute Relay is licensed under the [Apache License 2.0](LICENSE). Third-party services,
uploaded data, models, dependencies, and generated artifacts retain their own terms and
licenses.
