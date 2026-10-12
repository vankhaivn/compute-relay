# Current project status

The current implementation boundary. No public release is available.

## Serve modes

| Mode | Started with | Behavior |
|---|---|---|
| Local admission | `serve` with no provider flags | Authenticated loopback uploads, admission, status and control receipts, and reads of already published artifacts. Reports `local-admission-only`, `dispatch_enabled=false`; no provider effects. |
| Explicit Kaggle | complete `--provider-*` flags, an enabled `free_allowance` profile and a finite attempt budget | Verifies the account read-only at startup, then runs one dispatch worker and one collector. Reports `kaggle-workers`, `dispatch_enabled=true`. Once the budget is consumed, new jobs stay queued while started attempts are still observed and collected. |
| Managed | `--managed` (SSH only) or `--managed-python` (Kaggle and SSH) | Connection API with Relay-owned macOS Keychain credentials for Kaggle accounts and [SSH hosts](ssh-hosts.md), explicit CPU or GPU selections and multiple-account workers. Startup performs no account check; each new attempt needs a durable authorization through the API. See [managed connections](managed-connections.md). |

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Installation, workspace and token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| SSH host provider | CPU attempts in hardened per-attempt Docker containers on an operator-owned host, with visible pinned host keys, verified engine platforms, explicit standard-image installation, configurable concurrency/process limits, confirmed cancellation and live logs. |
| Native companion | Reproducible macOS arm64 candidate bundle with its own locked Python client, checksum-verified create-new installation and relocation discovery. |
| Durable work | Fair scheduling, fenced claims, one-shot preparation and submission intents, restart reconciliation, explicit controls and verified collection. |
| Preparation progress | While `preparing`, status reports input bytes handed to the provider staging upload. Observational only. |
| Result collection | Automatic by default; an immutable manual policy waits for `/collect` and reports selected-output byte progress. |
| Attempt logs | Bounded pages for an explicit original attempt with current read authority. Best effort: transient failures and long replay latency remain possible. |
| Artifact delivery | Attempt-scoped listing, metadata and create-only downloads verified to the end of the stream. |
| Local retention | Provider-mode serve expires and deletes eligible input and result bytes at startup and hourly (24 hours unreferenced, 7 days after safe completion); holds and unresolved work keep them. |

`202 Accepted` means local metadata committed, not remote success. A provider mutation needs a
durable one-shot intent plus a consumed managed authorization or the explicit-mode attempt budget.
There is no automatic CPU, account, provider or paid-capacity fallback.

## Evidence

Offline tests are not live-provider evidence. Live evidence so far:

- A fixed 2026-09 Kaggle experiment, using the same staging, execution and artifact components,
  qualified private staging, Tesla T4 execution, separate-process reconciliation and complete
  artifact collection. Per-question results are in
  [Kaggle feasibility](development/research/kaggle-feasibility.md).
- A finite CPU probe on the native companion qualified running log delivery, original-cursor
  reconnect and complete ordered replay.
- A 2026-10-12 password-based SSH run on a macOS Intel host over LAN, using Docker 29.6.1
  with Linux amd64 containers, qualified CPU execution, recovery after a Relay restart,
  bounded logs, verified artifacts, confirmed cancellation and explicit container cleanup.

Another account, environment or workload needs its own scoped qualification before support is
claimed. Use the [validation checklist](development/validation-checklist.md).

## Known limits

- Provider timeout enforcement is separate from local and runner deadlines.
- Batch cancellation is manual-required without a verified session target; exact hardware release
  is not observable.
- Explicit attempt-scoped remote cleanup is admitted and applied by provider-mode workers. The SSH
  host adapter deletes the attempt's stopped container; Kaggle cleanup requests settle as failed
  and preserve resources. Nothing is cleaned up automatically.
- SSH host qualification covers password access on one macOS Intel host over LAN. Live key
  authentication, Tailscale access, host sleep, Docker-engine restarts and Linux VPS hosts remain
  unqualified.
- Direct HTTPS input is not composed into `serve`; upload immutable inputs first.

Remaining product work is in the [implementation plan](development/implementation-plan.md).
