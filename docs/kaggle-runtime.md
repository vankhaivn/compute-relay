# Kaggle runtime

Kaggle is a managed provider adapter implemented in Go. Relay uses authenticated HTTPS directly;
it needs no Kaggle CLI, Python SDK, companion package or local interpreter. The embedded Python
runner is executed by Kaggle's remote environment. Public jobs and provider-neutral state remain
separate from Kaggle-specific resource identities.

## Connect and select resources

Start [the local runtime](local-runtime.md), discover `/providers`, then create a Kaggle connection
with its write-only token field using [managed connections](managed-connections.md). The adapter
discovers the account itself; the application does not supply a username. Verification reads
account/quota information without starting compute. A successful connection publishes an immutable
selection. A new account requires a new connection; credential replacement must keep the account.

The default machine policy is CPU. `--managed-machine-shape NvidiaTeslaT4` or
`NvidiaTeslaP100` requests that GPU policy explicitly. CPU never means fallback from an unavailable
GPU. Use the exact returned profile and accelerator in jobs. Changes require a new checked
selection for future jobs; old attempts retain their binding.

Fresh sufficient cached GPU quota is required before a finite GPU authorization. A missing, stale
or exhausted allowance does not become unlimited. CPU does not reserve GPU seconds; unknown CPU
allowance remains a warning, and known exhaustion blocks work. Connections sharing one account
share capacity and reservations. Quota observations do not authorize compute or prove a device
has been allocated. The adapter supplies no authoritative quota-reset timestamp.

## Stage and execute

Upload a bounded [bundle and input objects](packaging-and-import.md), admit the job and authorize
its exact attempt and wall limit. The adapter stages private attempt-owned data with a bound marker,
checks readiness and original bytes, then submits the private kernel once after the durable
intent barrier. Finite preparation and execution budgets apply. Remote internet is disabled unless
both the selected policy permits it and the job requests it.

Staging identity, account, numeric resource identity, version, source hash, nonce and privacy must
match the frozen plan. Upload success is not readiness. A lost staging or submission response
leaves the original intent for observation; not-found alone never permits another create or submit.
There is no automatic account/provider rotation, resubmission, CPU fallback or paid capacity.

Kaggle uses the combined `/authorize` flow. Separate preparation/start, absolute execution deadlines
and ephemeral workload secrets are unsupported requirements and are rejected before preparation.
Read logs and job status for the original attempt. GPU success requires runner hardware evidence;
a requested GPU, quota deduction or provider terminal state alone is insufficient.

## Results, cancellation and cleanup

The collector uses a complete bounded listing, exact version and original result manifest, then
verifies bytes and publishes the whole artifact set atomically. Signed downloads accept only the
reviewed `storage.googleapis.com` and `www.kaggleusercontent.com` hosts over HTTPS (optional 443).
Unexpected redirects, identities, paths, sizes, digests or completion acknowledgements fail closed.
See [artifact delivery](artifact-delivery.md).

Cancellation can remain manual-required when a verified usable session target is absent.
A local timeout or cancellation receipt does not prove remote termination or hardware release.
Use [recovery](recovery.md) for uncertainty. Resources persist until explicit
[attempt cleanup](remote-cleanup.md); Kaggle name-based deletion retains a replacement/rename race despite
matching original ownership/version checks.

## Qualification boundary

The native candidate completed finite CPU execution, live logs, a restart while running without
resubmission, verified artifacts and exact cleanup on 2026-10-14. Cleanup acknowledgements plus
complete authenticated owner inventories confirmed the original dataset and kernel absent. A
separately bound finite GPU job completed with two Tesla T4 devices, CUDA 12.8 and Torch
2.11.0+cu128. Runner hardware evidence and all five artifacts passed verification; exact cleanup
and complete authenticated inventories confirmed its dataset and kernel absent. Test credentials,
installation fingerprints and temporary roots were removed after preserving sanitized evidence.

A credential created by the original Python-backed binary remained usable across a native restart
without token re-entry after explicit macOS item-access and code-hash permission. The original
helper removed those legacy test items. This establishes format continuity; changed executable
identity may still require owner permission. See [compatibility](compatibility.md).

Provider timeout enforcement, exact same-version execution count and hardware release cannot be
inferred from local clocks, a successful output or an offline fixture. Record the exact binary,
account scope, selected hardware and finite authorized live behavior when extending support.
Keep private resource details and raw logs outside Git. See [compatibility](compatibility.md).
