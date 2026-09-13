# Kaggle feasibility report

> **Status:** M-0 upstream review complete; live verification is
> `blocked-environment` until an operator explicitly authorizes credentials and finite
> provider tests.
>
> **Evidence date:** 2026-09-13
>
> **Important:** no Kaggle account was authenticated, no remote resource was created, no
> GPU job was submitted, and no provider quota was consumed for this report.

This report applies the gates from the approved proposal to the currently selected official
client baseline. `documented-upstream` means a primary official source exposes a relevant
surface; it does not mean the configured account or end-to-end project path passed.

See [`kaggle-interface-review.md`](kaggle-interface-review.md) for source-level findings and
the planned M-1 probe sequence.

## Reviewed context

| Field | Value |
|---|---|
| Evidence date | 2026-09-13 |
| Kaggle CLI release | `kaggle==2.2.4` |
| Release/tag commit | `f0afa32699d28c97f82691728ada3ed8c16c5abf` |
| Release date | 2026-07-23 |
| Required local Python | `>=3.11` |
| Declared SDK range | `kagglesdk >=0.1.35,<1.0`; exact project lock not created yet |
| Managed-environment source reviewed | `Kaggle/docker-python@1ed8d43599a7faa1b630baeba82c57def3a8e33b` |
| Account scope | Not authenticated |
| Live compute authorized | No |
| Provider resources created | None |

## Capability summary

`Support` describes the product-facing conclusion today. `Evidence` describes what has
actually been checked.

| Gate | Capability/question | Support today | Evidence | M-0 conclusion and predetermined response |
|---|---|---|---|---|
| K-01 | Authentication | `unknown` | `documented-upstream` | Official OAuth, token env/file, and legacy file paths exist. A configured read-only probe is still required; missing credentials become actionable diagnostics. |
| K-02 | GPU eligibility | `unknown` | `documented-upstream` | Push supports accelerator selection and GPU metadata. Account eligibility and actual CUDA computation are not tested; never substitute CPU. |
| K-03 | Multi-file code and private inputs | `unknown` | `documented-upstream` | Private kernels and private-by-default datasets with attached sources are documented. Synthetic checksums and observed privacy are required before acceptance. |
| K-04 | Staging readiness | `unknown` | `documented-upstream` | Dataset status exists, but processing states/readiness semantics are not captured. Wait explicitly; never allocate GPU while inputs are known unready. |
| K-05 | Attempt identity/recovery | `unknown` | `documented-upstream` | Status, output, and logs resolve a kernel's latest run. Use one persisted slug per attempt plus manifest identity; run an ambiguous-response experiment before claiming safe recovery. |
| K-06 | Execution states | `unknown` | `documented-upstream` | A status command exists, but raw state vocabulary and failure semantics are not fixture-backed. Unknown values remain unknown. |
| K-07 | Artifact enumeration/retrieval | `unknown` | `documented-upstream` | File listing/download and pagination exist, but they refer to the latest run. Require all-page enumeration, matching manifest, byte counts, and digests. |
| K-08 | Logs | `unknown` | `documented-upstream` | Released source exposes one-shot logs and SSE `--follow`; timing, replay, and account behavior are untested. Return actual availability rather than promising live logs. |
| K-09 | Remote timeout | `unknown` | `documented-upstream` | Push accepts a timeout and client metadata carries session timeout. Exact enforcement and clock origin require a small controlled live test; retain a runner deadline. |
| K-10 | Terminal/release evidence | `unknown` | `documented-upstream` | Provider status can report a terminal run, but complete mappings and hardware release/accounting observability are untested. Never invent a release timestamp. |
| K-11 | Remote cancellation | `unknown` | `documented-upstream` | v2.2.4 exposes no kernel-cancel CLI. A public-client path and reliable session identity are not established. Advertise unsupported/manual-required unless both are proven; never delete as cancel. |
| K-12 | Accelerator quota | `unknown` | `documented-upstream` | Official quota returns GPU/TPU totals, used/remaining hours, and refresh time. Account values, missing fields, precision, and reset semantics require read-only testing. |
| K-13 | Runtime environment | `unknown` | `documented-upstream` | Official environment source exists and changes over time. Capture actual Python, shell, GPU, CUDA, framework, filesystem, and image information in a live manifest. |
| K-14 | Internet-disabled execution | `unknown` | `documented-upstream` | Kernel metadata supports disabled internet. A staged job must pass without remote downloads before this mode is advertised. |
| K-15 | Local restart/reconciliation | `unknown` | `not-tested` | Project orchestration does not exist yet. M-1 must restart the local probe/observer and recover the same unique attempt without another push. |
| K-16 | Ownership-safe cleanup | `unknown` | `documented-upstream` | Delete surfaces exist, but the project ownership ledger and identity-safe cleanup do not. Default to dry run; never delete unknown/active resources. |

## Detailed gate records

### K-01 — Authentication

- **Question:** Can the configured official credential access required private resources?
- **Evidence level:** `documented-upstream`; live account check `blocked-environment`.
- **Primary source/client:** Kaggle CLI v2.2.4 authentication documentation.
- **Observed result:** Four official credential paths are documented.
- **Capability conclusion:** No account-specific claim.
- **Fallback:** `doctor` reports missing/invalid references without printing secret values.
- **M-1 acceptance:** an explicitly configured read-only command succeeds and a negative
  test produces sanitized actionable output.

### K-02 — GPU eligibility

- **Question:** Can the account start a finite GPU-enabled run?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** v2.2.4 kernel push and metadata documentation.
- **Observed result:** accelerator/GPU selection exists.
- **Capability conclusion:** eligibility and allocation are unknown.
- **Fallback:** block and report; no CPU or paid-provider fallback.
- **M-1 acceptance:** a <=120-second budget performs a checked numerical GPU operation and
  records the actual device/runtime.

### K-03/K-04 — Private code, inputs, and readiness

- **Question:** Can a private immutable bundle and separate input arrive together, and how
  is readiness observed?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** v2.2.4 datasets, dataset metadata, kernels metadata.
- **Observed result:** private dataset creation/status and attached dataset sources exist.
- **Capability conclusion:** candidate transport only.
- **Fallback:** stop with `PRIVATE_STAGING_UNAVAILABLE` or `STAGING_NOT_READY`; never publish
  data or silently add paid storage.
- **M-1 acceptance:** synthetic remote checksums, confirmed private visibility, observed
  readiness, and test-owned cleanup identity.

### K-05/K-06 — Identity and status

- **Question:** Can one attempt be identified and safely rediscovered after uncertainty?
- **Evidence level:** `documented-upstream`; live/fault checks `blocked-environment`.
- **Primary source/client:** v2.2.4 kernel push/status/output/log source and docs.
- **Observed result:** observation surfaces resolve a kernel and generally its latest run;
  no project-safe attempt identity has been proven.
- **Capability conclusion:** use one persisted remote slug per attempt and verify a runner
  manifest; retain stronger version/session IDs when available.
- **Fallback:** `submission.unknown` → reconciliation/`needs_attention`; no repeated push.
- **M-1 acceptance:** capture accepted, rejected, and induced ambiguous outcomes; observe
  raw states; prove a restart finds the original resource.

### K-07/K-08 — Artifacts and logs

- **Question:** Can all correct-attempt outputs and available logs be retrieved?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** v2.2.4 output/file pagination, logs source, and changelog.
- **Observed result:** paginated output and one-shot/follow logs are exposed.
- **Capability conclusion:** availability and identity safety remain unknown.
- **Fallback:** artifact-only collection retry; logs report `live`, `delayed`,
  `after_completion`, `unavailable`, or `unknown` from evidence.
- **M-1 acceptance:** multiple files/pages, deliberate slow output, reconnect behavior,
  manifest identity, sizes, and SHA-256 digests.

### K-09/K-10 — Timeout and termination

- **Question:** Does a requested limit bound the run, and what terminal/release evidence is
  observable?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** v2.2.4 push timeout and status surface.
- **Observed result:** timeout and terminal observation surfaces exist.
- **Capability conclusion:** semantics are not established.
- **Fallback:** enforce runner deadline and preserve `may_be_active=unknown` when needed.
- **M-1 acceptance:** one intentionally small timeout case, terminal status capture, and a
  written account of unobservable release/accounting details.

### K-11 — Cancellation

- **Question:** Is supported cancellation available with a reliably obtainable target ID?
- **Evidence level:** public CLI absence `documented-upstream`; bridge/live behavior
  `not-tested`.
- **Primary source/client:** v2.2.4 CLI parser/source and official repository issue #1169 as
  a non-authoritative research lead.
- **Observed result:** no `kaggle kernels cancel` command in v2.2.4.
- **Capability conclusion:** `unknown`; do not promise remote cancellation.
- **Fallback:** persist intent, return `manual_required`/`needs_attention`, continue safe
  observation, and give manual provider guidance.
- **M-1 acceptance:** both a supported public operation and its stable target identity, or
  an explicit unsupported conclusion.

### K-12 — Quota

- **Question:** What account-scoped values, units, reset time, age, and precision exist?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** v2.2.4 `kaggle quota` and client source.
- **Observed result:** GPU/TPU total, used, remaining hours, and refresh time are exposed.
- **Capability conclusion:** account values and completeness unknown.
- **Fallback:** unknown quota warns and permits only bounded free-only work by default;
  strict mode blocks.
- **M-1 acceptance:** sanitized JSON/structured fixture and documented missing-field/error
  behavior.

### K-13/K-14 — Environment and offline operation

- **Question:** What environment exists, and can an attached job run without internet?
- **Evidence level:** `documented-upstream`; live check `blocked-environment`.
- **Primary source/client:** Kaggle docker-python source and kernel metadata.
- **Observed result:** environment provenance and internet flag exist.
- **Capability conclusion:** actual account/run image is unknown.
- **Fallback:** fail explicit resource/dependency checks; never overwrite managed GPU
  packages optimistically.
- **M-1 acceptance:** environment manifest plus a small attached-input run with internet
  disabled.

### K-15/K-16 — Restart and cleanup

- **Question:** Can observation survive local restart, and can only owned completed
  resources be removed?
- **Evidence level:** `not-tested`.
- **Observed result:** no project implementation or live resources exist.
- **Capability conclusion:** required project behavior, not an upstream capability claim.
- **Fallback:** preserve intent/resource records indefinitely enough for recovery; remote
  cleanup remains disabled except explicit ledger-backed dry run/apply.
- **M-1 acceptance:** restart without duplicate push; cleanup of a unique test-owned
  terminal resource is identity-checked and idempotent.

## Go/no-go rule for the Kaggle batch adapter

Proceed to production adapter integration only after an authorized private-input → bounded
execution → identity-matched terminal result → verified artifact path succeeds.

Missing cancellation, live logs, or exact quota may remain explicit limitations. Failure of
private staging, identity-safe collection, or bounded termination blocks the Kaggle batch
claim; it does not authorize public-data workarounds, undocumented browser APIs, paid
fallback, or blind resubmission.
