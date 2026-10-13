# Current project status

The current implementation boundary. No public release is available.

## Serve modes

| Mode | Started with | Behavior |
|---|---|---|
| Local admission | `serve` with no provider flags | Authenticated loopback uploads, admission, status and control receipts, and reads of already published artifacts. Reports `local-admission-only`, `dispatch_enabled=false`; no provider effects. |
| Explicit Kaggle | complete `--provider-*` flags, an enabled `free_allowance` profile and a finite attempt budget | Verifies the account read-only at startup, then runs one dispatch worker and one collector. Reports `kaggle-workers`, `dispatch_enabled=true`. Once the budget is consumed, new jobs stay queued while started attempts are still observed and collected. |
| Managed | `--managed` (SSH only) or `--managed-python` (Kaggle and SSH) | Connection API with private file credentials or optional macOS Keychain for Kaggle accounts and [SSH hosts](ssh-hosts.md), explicit CPU or GPU selections and multiple-account workers. Startup performs no account check; each new attempt needs a durable authorization through the API. See [managed connections](managed-connections.md). |

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Installation, workspace and token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| SSH host provider | CPU attempts in hardened per-attempt Docker containers on an operator-owned host, with visible pinned host keys, verified engine platforms, explicit standard-image installation, configurable concurrency/process limits, confirmed cancellation and live logs. |
| Native companion | Reproducible macOS arm64 candidate bundle with its own locked Python client, checksum-verified create-new installation and relocation discovery. |
| Durable work | Fair scheduling, fenced claims, one-shot preparation and submission intents, restart reconciliation, explicit controls and verified collection. |
| Preparation progress | While `preparing`, status reports input bytes handed to the provider staging upload. Observational only. |
| Preparation gates | Managed SSH supports opt-in stopped-container preparation, readiness, one-shot start and ephemeral secret slots. See [preparation gates](preparation-gates.md) and [workload secrets](workload-secrets.md). |
| Result collection | Automatic by default; an immutable manual policy waits for `/collect` and reports selected-output byte progress. |
| Attempt logs | Bounded pages for an explicit original attempt with current read authority. Best effort: transient failures and long replay latency remain possible. |
| Execution deadlines | Optional immutable absolute UTC deadline on supported paths, with enforcement in the remote runner and SSH container supervisor. See [execution deadlines](execution-deadlines.md). |
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
- A 2026-10-12 SSH qualification on one macOS Intel host with Docker 29.6.1 and Linux amd64
  containers covers LAN password access and Tailscale password/unencrypted Ed25519-key access.
  The envelope includes file credentials without Python, explicit standard-image installation,
  prepared holds and running work across Relay restart, separate one-shot start, bounded logs,
  verified artifacts, cancellation and exact cleanup of completed and never-started containers.
  Absolute-deadline enforcement stopped a TERM-ignoring payload and child while Relay was offline,
  then recovered the original timeout result and cleaned up that container after restart.
- A 2026-10-13 isolated Docker 29.1.3 Linux-amd64 engine under emulation, accessed through
  loopback SSH, qualified read-only cleanup preview, active-container refusal and removal of the
  original stopped container. Bind files, named and anonymous volumes, images, unrelated
  containers and verified local inputs/results were preserved. This does not qualify a native
  remote host or cleanup recovery after a lost acknowledgement.

Another account, environment or workload needs its own scoped qualification before support is
claimed. Use the [validation checklist](development/validation-checklist.md).

## Known limits

- Provider timeout enforcement is separate from local and runner deadlines.
- Host sleep, VM clock behavior and reboot remain unqualified for execution deadlines. Software
  cannot act on a powered-off or suspended host; expiry is observed after it resumes.
- Batch cancellation is manual-required without a verified session target; exact hardware release
  is not observable.
- [Remote cleanup](remote-cleanup.md) has a bounded attempt preview and durable per-resource
  outcomes. SSH deletion requires original container and engine identity; older receipts without
  engine evidence are retained. Kaggle resources remain unsupported because the pinned deletion
  API lacks an immutable-target precondition. Expanded safety checks have offline coverage;
  live lost-acknowledgement/restart/repeat cleanup and private Kaggle cleanup qualification remain
  pending.
- SSH host qualification covers one macOS Intel host over LAN and Tailscale. Encrypted private
  keys, host sleep, Docker-engine restarts, other Tailscale network paths and Linux VPS hosts remain
  unqualified live.
- Direct HTTPS input is not composed into `serve`; upload immutable inputs first.

Remaining product work is in the [implementation plan](development/implementation-plan.md).
