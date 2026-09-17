# Current project status

This page is the current implementation boundary, not a record of completed development work.
No public release is available. Passing offline tests is not evidence of live-provider support.

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Local installation, workspace/token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| Normal `serve` | Authenticated loopback uploads, admission/status/control receipts and reads of already published artifacts. It reports `local-admission-only`, `dispatch_enabled=false`. |
| Durable components | Scheduling, fenced dispatch, one-shot intents, explicit controls, verified collection and pin-aware retention exist as explicitly composed Go services. |
| Artifact delivery | Explicit-attempt listing/metadata/content and create-only CLI downloads, with current authority, expiry and end-of-stream verification. |
| Kaggle components | Credential-scoped preflight, private staging, exact-identity execution, quota/log snapshots and selected artifact transfer. Tested with synthetic transport. |
| Kaggle acceptance utility | A separate fixed CUDA experiment using real components, durable state and a new resume process. Requires operator credentials and separate live authorization. |

The ordinary server does not instantiate providers, scheduler/dispatch workers, a collector,
an input fetcher or a retention sweeper. An admission profile is not a complete provider
configuration. `202 Accepted` means local metadata committed, not that compute started.
Artifact routes cannot manufacture a publication for a queued job.

## What is not established

No operator-run live GPU/result/restart report is currently recorded. Provider privacy,
actual GPU allocation, timeout enforcement, account-specific quota/log availability and
live result recovery need the [validation checklist](development/validation-checklist.md).

The fixed experiment is not an arbitrary-job service. A scoped `passed-live` result does not
close the entire provider checklist or make the normal server dispatch jobs. In particular,
cancellation is manual without a verified session target; cleanup is not cancellation.

## Remaining product work

General provider registration and worker lifecycle, remaining log/cleanup surfaces, strict
runtime TOML configuration, doctor, client examples, installation packaging and release
hardening remain. These are development tasks, not gates that an operator can close merely
by rerunning tests. See the [implementation plan](implementation-plan.md).

## Record evidence without inflating readiness

Use [validation results](development/validation-results.md) for dated, versioned operator
runs and unresolved failures. Keep secrets and raw evidence outside Git; commit only reviewed,
sanitized summaries. Update a capability or gate only when its exact acceptance criteria
are evidenced. Historical development discussion remains in Git and pull requests.
