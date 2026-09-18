# Current project status

If you are evaluating or operating the repository, start with [Getting started](getting-started.md) and the [Operator runbook](runbook.md). This page is the implementation boundary those guides rely on.

This page is the current implementation boundary, not a record of completed development work.
No public release is available. Passing offline tests alone is not evidence of live-provider support.

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Local installation, workspace/token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| Normal `serve` | Authenticated loopback uploads, admission/status/control receipts and reads of already published artifacts. It reports `local-admission-only`, `dispatch_enabled=false`. |
| Durable components | Scheduling, fenced dispatch, one-shot intents, explicit controls, verified collection and pin-aware retention exist as explicitly composed Go services. |
| Artifact delivery | Explicit-attempt listing/metadata/content and create-only CLI downloads, with current authority, expiry and end-of-stream verification. |
| Kaggle components | Credential-scoped preflight, private staging, exact-identity execution, quota/log snapshots and selected artifact transfer. |
| Kaggle acceptance utility | A separate fixed CUDA experiment using real components, durable state and a new resume process. The scoped path has been live-qualified for private staging, Tesla T4 execution, separate-process reconciliation and complete six-file artifact collection/publication. |

The ordinary server does not instantiate providers, scheduler/dispatch workers, a collector,
an input fetcher or a retention sweeper. An admission profile is not a complete provider
configuration. `202 Accepted` means local metadata committed, not that compute started.
Artifact routes cannot manufacture a publication for a queued job.

## Scoped provider qualification

The fixed Kaggle acceptance path has demonstrated private input staging, actual CUDA work on a
Tesla T4, terminal observation after a separate-process restart, bounded logs and complete
artifact download/publication through the reviewed Kaggle output CDN. That evidence applies to
the fixed experiment and pinned client/provider contract; it does not turn the normal server into
an arbitrary-job provider runtime.

Provider timeout enforcement remains separate from local/runner deadlines. Batch cancellation is
manual-required without a verified session target, exact hardware release is not observable, and
remote cleanup apply is not shipped. Same-version external reruns and arbitrary account/workload
compatibility are not implied by the scoped qualification.

## Remaining product work

General provider registration and worker lifecycle, remaining log/cleanup surfaces, strict
runtime TOML configuration, doctor, client examples, installation packaging and release
hardening remain. These are development tasks, not gates that an operator can close merely
by rerunning tests. See the [implementation plan](development/implementation-plan.md).

## Evidence hygiene

Keep credentials, raw logs, state databases, signed URLs and full operator run reports outside
Git. When new evidence changes a support claim or exposes a reproducible failure, attach the
minimal sanitized facts to a focused issue/PR and update this page or the relevant component
reference. Git and PR history preserve completed qualification work without a living run ledger.
