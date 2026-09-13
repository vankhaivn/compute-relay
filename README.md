# Compute Relay

> **Project status:** repository bootstrap. No runtime binary, public API, or Kaggle execution path has been implemented or live-verified yet.

Compute Relay is the repository for an open-source, self-hosted **compute connector runtime**. The intended runtime sits beside an application, accepts finite jobs through a provider-neutral HTTP/JSON boundary, and coordinates execution on external compute providers. Kaggle is the first provider to investigate and implement; it is not the identity of the core product.

The approved product direction is captured in [`docs/proposal.md`](docs/proposal.md). That proposal uses **Compute Connector** as a working name. This repository uses **Compute Relay**; public executable, package, and API naming remain provisional until recorded in an architecture decision.

## Product direction

The intended product will:

- run on infrastructure controlled by the operator;
- use the operator's own provider account and credentials;
- execute bounded Python or shell jobs through an explicit job contract;
- preserve durable local job and attempt state;
- stage inputs, observe execution, and retrieve verified artifacts;
- expose unsupported, unknown, or unverified provider capabilities honestly; and
- keep provider-specific behavior behind adapters so applications integrate once.

It is **not** a hosted GPU service, transparent remote VRAM, a CUDA proxy, an always-on model server, a remote desktop, or a guarantee of immediate GPU allocation. The reference workflow must not require a maintainer-operated service or mandatory paid infrastructure.

## Repository state

This initial commit establishes the project governance and documentation system only:

- approved proposal and architecture baseline;
- Apache-2.0 licensing and OSS policies;
- contribution, security, support, and governance guidance;
- mandatory instructions for human and automated contributors;
- Conventional Commits documentation, local hooks, and CI validation; and
- planning, ADR, provider-research, and feasibility-document scaffolding.

Implementation claims must be backed by code, tests, and—where provider behavior is involved—dated evidence. A passing fake-provider test will not be described as proof of live Kaggle support.

## Documentation map

| Document | Purpose |
|---|---|
| [`docs/proposal.md`](docs/proposal.md) | Approved product direction, defaults, feasibility gates, and boundaries. |
| [`AGENTS.md`](AGENTS.md) | Mandatory repository-wide instructions for humans and coding agents. |
| [`docs/architecture.md`](docs/architecture.md) | Provider-neutral architecture baseline and invariants. |
| [`docs/roadmap.md`](docs/roadmap.md) | Acceptance-based outcome sequence; no invented dates. |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | Required format for dependency-aware implementation tasks. |
| [`docs/research/kaggle-feasibility.md`](docs/research/kaggle-feasibility.md) | Evidence ledger for Kaggle feasibility gates. |
| [`docs/development/commit-convention.md`](docs/development/commit-convention.md) | Canonical commit-message rules. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contributor workflow and quality expectations. |
| [`SECURITY.md`](SECURITY.md) | Vulnerability and credential-disclosure reporting. |

See [`docs/README.md`](docs/README.md) for the complete documentation system.

## Contributing

Read [`AGENTS.md`](AGENTS.md), [`docs/proposal.md`](docs/proposal.md), and [`CONTRIBUTING.md`](CONTRIBUTING.md) before changing the repository.

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

The default collaboration path is a focused branch and pull request. Direct pushes to `main` require explicit owner authorization for the specific task.

## Security and provider responsibility

Never commit provider credentials, runtime API tokens, account exports, real job inputs, or private artifacts. Do not paste secrets into issues or chat. Provider availability, eligibility, quota, terms, and pricing can change; operators remain responsible for their accounts, workloads, data rights, and provider compliance.

See [`SECURITY.md`](SECURITY.md) for private reporting guidance.

## License

Compute Relay is licensed under the [Apache License 2.0](LICENSE). Third-party services, uploaded data, models, dependencies, and generated artifacts retain their own terms and licenses.
