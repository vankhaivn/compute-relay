# Current project status

The current implementation boundary. No public release is available.

## Serve modes

| Mode | Started with | Behavior |
|---|---|---|
| Local admission | `serve` with no provider flags | Authenticated loopback uploads, admission, status and control receipts, and reads of already published artifacts. Reports `local-admission-only`, `dispatch_enabled=false`; no provider effects. |
| Explicit Kaggle | complete `--provider-*` flags, an enabled `free_allowance` profile and a finite attempt budget | Verifies the account read-only at startup, then runs one dispatch worker and one collector. Reports `kaggle-workers`, `dispatch_enabled=true`. Once the budget is consumed, new jobs stay queued while started attempts are still observed and collected. |
| Managed | `--managed-python` | Connection API with Relay-owned macOS Keychain credentials, explicit CPU or GPU selections and multiple-account workers. Startup performs no account check; each new attempt needs a durable authorization through the API. See [managed connections](managed-connections.md). |

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Installation, workspace and token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| Native companion | Reproducible macOS arm64 candidate bundle with its own locked Python client, checksum-verified create-new installation and relocation discovery. |
| Durable work | Fair scheduling, fenced claims, one-shot preparation and submission intents, restart reconciliation, explicit controls and verified collection. |
| Preparation progress | While `preparing`, status reports input bytes handed to the provider staging upload. Observational only. |
| Result collection | Automatic by default; an immutable manual policy waits for `/collect` and reports selected-output byte progress. |
| Attempt logs | Bounded pages for an explicit original attempt with current read authority. Best effort: transient failures and long replay latency remain possible. |
| Artifact delivery | Attempt-scoped listing, metadata and create-only downloads verified to the end of the stream. |

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

Another account, environment or workload needs its own scoped qualification before support is
claimed. Use the [validation checklist](development/validation-checklist.md).

## Known limits

- Provider timeout enforcement is separate from local and runner deadlines.
- Batch cancellation is manual-required without a verified session target; exact hardware release
  is not observable.
- Remote cleanup apply is not shipped.
- Direct HTTPS input is not composed into `serve`; upload immutable inputs first.

Remaining product work is in the [implementation plan](development/implementation-plan.md).
