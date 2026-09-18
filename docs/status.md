# Current project status

If you are evaluating or operating the repository, start with [Getting started](getting-started.md),
the [Kaggle runtime](kaggle-runtime.md) for GPU execution, and the [Operator runbook](runbook.md).

This page is the current implementation boundary, not a record of completed development work.
No public release is available. Passing offline tests alone is not evidence of live-provider support.

## Available now

| Surface | Current behavior |
|---|---|
| Main binary | Local installation, workspace/token administration, immutable admission profiles, bundle tools and application HTTP commands. |
| Default `serve` | Authenticated loopback uploads, admission/status/control receipts and reads of already published artifacts. It reports `local-admission-only`, `dispatch_enabled=false`. |
| Kaggle-enabled `serve` | With complete explicit provider flags, an exact enabled `free_allowance` profile, successful read-only account verification and a finite attempt budget, starts one durable dispatch worker and one collector and reports `kaggle-workers`, `dispatch_enabled=true`. |
| Durable work | Fair scheduling, fenced claims, one-shot preparation/submission intents, restart reconciliation, explicit controls and verified collection/publication. |
| Artifact delivery | Explicit-attempt listing/metadata/content and create-only CLI downloads, with current authority, expiry and end-of-stream verification. |
| Kaggle components | Credential-scoped preflight, private staging, exact-identity execution, quota/log snapshots and selected artifact transfer. |
| Kaggle acceptance utility | Separate fixed CUDA experiment live-qualified for private staging, Tesla T4 execution, separate-process reconciliation and complete six-file artifact collection/publication. |

Provider mode does not turn admission into success evidence. `202 Accepted` means local metadata
committed. A new provider mutation requires the durable one-shot intent plus remaining process
authorization budget. Once that budget is consumed, additional queued jobs remain queued while
already-started attempts can still be observed/recovered and collected.

## Integrated Kaggle boundary

The normal runtime now reuses the reviewed Kaggle staging/execution/artifact components behind an
exact immutable profile/provider/account binding. It requires known positive free GPU quota before
new GPU dispatch and has no automatic CPU, account, provider or paid-capacity fallback.

The integrated normal-server path is implemented and offline/native-tested. Live evidence currently
belongs to the narrower fixed acceptance experiment; do not silently generalize that evidence to
arbitrary accounts/workloads.

Provider timeout enforcement remains separate from local/runner deadlines. Batch cancellation is
manual-required without a verified session target, exact hardware release is not observable, and
remote cleanup apply is not shipped. Direct HTTPS input preparation is not enabled in the current
normal provider composition; upload immutable inputs first.

## Remaining product work

Public provider log/quota/cleanup surfaces, strict runtime TOML configuration, doctor, thin client
and LLM examples, installation packaging, backup/restore qualification and release hardening
remain. See the [implementation plan](development/implementation-plan.md).

## Evidence hygiene

Keep credentials, raw logs, state databases, signed URLs and full operator run reports outside Git.
When new evidence changes a support claim or exposes a reproducible failure, attach the minimal
sanitized facts to a focused issue/PR and update this page or the relevant component reference.
