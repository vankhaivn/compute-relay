# Compatibility and support boundaries

This page describes the pinned environments and constraints to check on a new host. The
project is unreleased. Native CI is component evidence, not a complete installation or
live-provider support promise. See [current status](status.md) for implemented features.

## Pinned local environments

| Component | Repository baseline | Authority |
|---|---|---|
| Go | 1.27.1 | `go.mod`, Go workflow |
| SQLite / matching libc | modernc SQLite 1.58.0 / libc 1.75.6 | `go.mod`, `go.sum` |
| Python client | 3.11.16 | `tools/kaggle-client/.python-version`, client workflow |
| uv | 0.12.13 | Client `pyproject.toml` and wrappers |
| Kaggle CLI / SDK | 2.2.4 / 0.1.35 | Client `pyproject.toml`, `uv.lock` |
| Remote workload environment | Linux; actual Python/PyTorch/CUDA recorded by the run | Runner manifest and live acceptance report |

These are project pins, not claims about the latest upstream release. The local client
virtual environment does not determine the managed provider image or install remote PyTorch.
Do not assume a successful local import proves GPU availability or compatible remote packages.

## Host and filesystem requirements

Go CI runs native tests/builds on Linux, macOS and Windows; the client workflow separately
checks its locked environment on those operating systems. Check the actual job log for runner
architecture. Cross-compilation or one OS job does not qualify every amd64/arm64 combination.
Record the exact OS/architecture, filesystem and tool versions in
[validation results](development/validation-results.md).

| Boundary | Requirement / limitation |
|---|---|
| Runtime state | Dedicated private directories, valid original markers and exclusive process locks; stop `serve` before local administration. |
| Application transport | Explicit loopback address, application token file, no redirect or ambient proxy. Not a public multi-tenant endpoint. |
| Artifact HTTP | HTTP/1.1 chunked transfer with the final verification trailer preserved. Generic proxy/browser/HTTP/2 compatibility is not established. |
| Artifact destination | Private same-filesystem temporary file and create-only hard link; unsupported hard links fail rather than overwrite. |
| Permissions | Native private Unix permissions or protected Windows ACLs. The same-user operator remains trusted. |
| Remote runner | Linux process-group and signal semantics. Windows/macOS support refers to the local host, not remote workload execution. |
| Storage durability | Database-only backup excludes blobs. Shared/network filesystems and arbitrary power-loss guarantees are unverified. |

See [local runtime](local-runtime.md), [artifact delivery](artifact-delivery.md),
[storage](storage.md) and [runner](../runner/README.md) for the corresponding procedures.

## Provider support is evidence-scoped

Preflight, private staging, one-shot execution, quota/log snapshots and selected artifact
retrieval have offline implementations. The fixed GPU harness composes them for an explicit
operator experiment; the general server still starts no provider workers. No current live
qualification is recorded in the repository's validation-results template.

Cancellation remains manual-required without a verified session target. Provider timeout
enforcement, same-version rerun identity and exact hardware release cannot be inferred from
local clocks or successful output. Signed downloads accept only the reviewed storage host;
other CDNs fail closed. SDK private transport-layout dependencies and source reconstruction
across binary changes remain explicit limits in the [provider guides](providers/README.md).

## Qualification and upgrades

Use the [validation checklist](development/validation-checklist.md) for clean-host tests,
authorized account reads, the bounded GPU/restart experiment and remaining blocked checks.
Retain the exact source/executable identity and sanitized evidence. A scoped live pass does
not automatically close every M1 gate or qualify arbitrary jobs/accounts.

Toolchain, SDK or managed-environment changes require source/license review, updated fixtures,
offline regressions and live re-verification of affected claims. Do not silently relax identity,
TLS, privacy, timeout or retry constraints to accommodate a changed response. Record failures
and blockers in the results ledger; keep credentials and private state out of Git.
