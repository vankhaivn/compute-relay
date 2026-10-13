# Compatibility and support boundaries

This page describes the pinned environments and constraints to check on a new host. The
project is unreleased. Native CI is component evidence, not a complete installation or
live-provider support promise. See [current status](status.md) for implemented features.

## Pinned local environments

| Component | Repository baseline | Authority |
|---|---|---|
| Go | 1.27.1 | `go.mod`, Go workflow |
| SQLite / matching libc | modernc SQLite 1.58.0 / libc 1.75.6 | `go.mod`, `go.sum` |
| SSH client | golang.org/x/crypto 0.55.0 | `go.mod`, `go.sum` |
| Native Keychain FFI | purego 0.11.1; macOS Security.framework | `go.mod`, `go.sum` |
| SSH host standard image | Repository Dockerfile, `python:3.11-slim-bookworm` base pinned by index digest | `internal/provider/sshhost/image/Dockerfile`; installed image ID frozen per selection |
| Python client | 3.11.16 | `tools/kaggle-client/.python-version`, client workflow |
| uv | 0.12.13 | Client `pyproject.toml` and wrappers |
| Kaggle CLI / SDK | 2.2.4 / 0.1.35 | Client `pyproject.toml`, `uv.lock` |
| Remote workload environment | Linux; actual Python/PyTorch/CUDA recorded by the run | Runner manifest and live run record |

These are project pins, not claims about the latest upstream release. The local client
virtual environment does not determine the managed provider image or install remote PyTorch.
Do not assume a successful local import proves GPU availability or compatible remote packages.

## Host and filesystem requirements

Routine Go and client CI run on Linux. Manually starting either workflow adds its macOS checks;
the Go workflow also adds full race tests. Windows CI is disabled, and a green routine PR does
not provide current macOS or Windows validation. Platform implementations and local commands
remain available. See the [CI policy](development/go-toolchain.md#dependency-changes-and-ci).
Check the actual job log for runner architecture. Cross-compilation or one OS job does not
qualify every amd64/arm64 combination.
Record the exact OS/architecture, filesystem and tool versions in the private operator record;
share only the minimal sanitized values needed by a focused issue/PR.

| Boundary | Requirement / limitation |
|---|---|
| Runtime state | Dedicated private directories, valid original markers and exclusive process locks; stop `serve` before local administration. |
| Application transport | Explicit loopback address, application token file, no redirect or ambient proxy. Not a public multi-tenant endpoint. |
| Artifact HTTP | HTTP/1.1 chunked transfer with the final verification trailer preserved. Generic proxy/browser/HTTP/2 compatibility is not established. |
| Artifact destination | Private same-filesystem temporary file and create-only hard link; unsupported hard links fail rather than overwrite. |
| Permissions | Native private Unix permissions or protected Windows ACLs. The same-user operator remains trusted. |
| Managed credentials | New installations use an unencrypted private `credentials/secrets.env` file on Linux, macOS and Windows. Existing Keychain installations retain their backend; Keychain is optional on macOS. See [storage selection](managed-connections.md#choose-credential-storage). |
| Remote runner | Linux process-group and signal semantics. Windows/macOS support refers to the local host, not remote workload execution. |
| SSH host | SSH server with password, keyboard-interactive or key login; a Docker engine reporting `linux/amd64` or `linux/arm64` that the login can use; ED25519 host key preferred for pinning. See [SSH hosts](ssh-hosts.md). |
| SSH-only managed mode | `--managed` needs no Python or CGo and uses the selected credential backend. Optional Keychain uses the native macOS API; existing item access lists can require explicit per-binary trust when switching from the Python bridge or updating the binary. |
| Storage durability | Database-only backup excludes blobs. Shared/network filesystems and arbitrary power-loss guarantees are unverified. |

Credential-file updates flush a private temporary file before replacing the original. Unix also
syncs the directory; Windows does not claim a portable directory-sync or power-loss guarantee.
The file backend is bounded to 8 MiB and 4096 entries and refuses corruption, unsafe permissions
and links. It does not provide encryption or protection from same-user processes.

See [local runtime](local-runtime.md), [artifact delivery](artifact-delivery.md),
[storage](development/storage.md) and [runner](../runner/README.md) for the corresponding procedures.

## Provider support is evidence-scoped

The account check, private staging, one-shot execution, quota/log snapshots and selected
artifact retrieval have offline implementations; the live evidence so far is listed in
[current status](status.md). Another account or workload does not inherit that support.

Cancellation remains manual-required without a verified session target. Provider timeout
enforcement, same-version rerun identity and exact hardware release cannot be inferred from
local clocks or successful output. Signed kernel-output downloads accept only the reviewed
`storage.googleapis.com` and `www.kaggleusercontent.com` hosts (optional 443); other
destinations fail closed. SDK private transport-layout dependencies and source reconstruction
across binary changes remain explicit limits in the [provider guides](development/providers/README.md).

## Qualification and upgrades

Use the [validation checklist](development/validation-checklist.md) for clean-host tests,
authorized account reads, the bounded GPU/restart experiment and remaining blocked checks.
Retain the exact source/executable identity and sanitized evidence. A scoped live pass does
not automatically close every M1 gate or qualify arbitrary jobs/accounts.

Toolchain, SDK or managed-environment changes require source/license review, updated fixtures,
offline regressions and live re-verification of affected claims. Do not silently relax identity,
TLS, privacy, timeout or retry constraints to accommodate a changed response. Put reproducible
failures or changed support conclusions in a focused issue/PR; keep credentials and private state
out of Git.
