# Compatibility and qualification

Compute Relay is unreleased. Implemented contracts, offline fixtures, current-host checks and live
provider observations are separate evidence. A test pass is not a complete support promise.

## Pinned environments

| Component | Baseline / authority |
|---|---|
| Control plane | Go 1.27.1; [go.mod](../go.mod), [go.sum](../go.sum). |
| State | CGo-free modernc SQLite with matching libc pinned in Go modules. |
| Native Keychain | macOS Security.framework through pinned purego. |
| Python development / remote runner | Python 3.11.16; the control plane needs no interpreter. |
| SSH image | [Dockerfile](../internal/provider/sshhost/image/Dockerfile), digest-pinned Python 3.11 base; selected image ID is frozen. |
| Remote workloads | Actual Linux/Python/framework/CUDA/hardware recorded in runner results. |

A local interpreter or successful import does not determine a provider image or prove GPU support.
Routine CI is Linux offline validation; race and native macOS jobs are manual.
See [development](development.md). Cross-compilation does not qualify every OS/architecture pair.

## Host and filesystem boundaries

| Boundary | Requirement / limit |
|---|---|
| Relay host | Linux or macOS; native private Unix permissions. Same-user operator/processes remain trusted. |
| Runtime storage | Dedicated private directories, one top-level `runtime.lock`, matching `.compute-relay-runtime`/database installation identity and original blob format/retention markers. |
| HTTP | Literal loopback listener, scoped bearer tokens, Host/Origin checks; no public multi-tenant endpoint. |
| Artifact transport | HTTP/1.1 chunked content with final verification trailer preserved. Generic proxies/browser/HTTP/2 are unqualified. |
| File vault | Unencrypted private `credentials/secrets.env`; 8 MiB / 4096-entry bounds, corruption/unsafe permissions/links refused. |
| Keychain | Optional macOS vault; existing identity retained, changed executable may require macOS access permission. |
| Remote runner | Linux process-group/signal/clock semantics; Python executes remotely. |
| SSH host | Pinned SSH key and native machine identity; local Unix-socket Docker engine reporting `linux/amd64` or `linux/arm64`. |
| Durability | Whole-store recovery needs database/WAL, both blob roots, markers and credentials. Network filesystems and arbitrary power loss are unqualified. |

File-vault updates flush a private temporary file, replace the original and sync its directory.
Encoding is not encryption, and logical memory clearing is not secure erasure. Vault access denial
never selects another backend. See [managed connections](managed-connections.md).

## Provider evidence

Native Kaggle read-only account/quota access and original-binary Keychain continuity were observed
on 2026-10-14. Keychain access required owner-approved macOS permission; no token re-entry was
needed. Native deletion was denied, so exact creator-helper cleanup was required for that local
test item. This does not prove transparent migration or permission-free future upgrades.

Final native Kaggle CPU/GPU execution, logs, verified artifacts, restart reconciliation and exact
cleanup qualification remain pending. Final live SSH qualification also remains pending. Existing
offline adapter/contract tests and historical transport runs do not close those checks.

Kaggle cancellation may remain manual-required without a verified session target. Provider timeout
enforcement, same-version rerun count and hardware release require evidence independent of local
clocks and results. Absolute-deadline SSH sleep/resume, engine restart and reboot need qualification
on the actual host/VM. Read [Kaggle](kaggle-runtime.md), [SSH](ssh-hosts.md),
[execution bounds](execution-gates.md) and [cleanup](remote-cleanup.md) before relying on them.

For upgrades, retain the original executable and state, run offline regressions, then requalify
changed host/provider boundaries with exact finite authorization. Record binary revision, actual
OS/architecture, policy and sanitized outcomes in a focused issue/PR. Keep credentials, private
paths, account exports, databases and raw logs outside Git.
