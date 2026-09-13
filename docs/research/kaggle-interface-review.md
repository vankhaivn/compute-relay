# Kaggle interface review

> **Status:** M-0 source review complete; no account-authenticated or compute-consuming
> test has been performed.
>
> **Checked:** 2026-09-13
>
> **Reviewed release:** `kaggle==2.2.4`, tag commit
> `f0afa32699d28c97f82691728ada3ed8c16c5abf`, published 2026-07-23.

This review records what the pinned official client surface documents or exposes in source.
It does not establish that a particular account can use a capability, that the provider will
retain the behavior indefinitely, or that the project has completed a live workflow.

## Source baseline

| Source | Version or revision | What it establishes | Evidence level |
|---|---|---|---|
| [Kaggle CLI release v2.2.4](https://github.com/Kaggle/kaggle-cli/releases/tag/v2.2.4) | tag `v2.2.4` | Current stable release selected for the first probe. | `documented-upstream` |
| [CLI installation/authentication](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/README.md) | `v2.2.4` | Python 3.11+; OAuth, `KAGGLE_API_TOKEN`, access-token file, and legacy credential-file paths. | `documented-upstream` |
| [Kernel commands](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/kernels.md) | `v2.2.4` | Push, accelerator selection, timeout, status, output, file listing, pagination, pull, and delete surfaces. | `documented-upstream` |
| [Kernel metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/kernels_metadata.md) | `v2.2.4` | Private/GPU/internet/machine-shape/source metadata fields; private defaults to true when omitted. | `documented-upstream` |
| [Dataset commands](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets.md) | `v2.2.4` | Private-by-default create, version, status, file listing/download, and delete surfaces. | `documented-upstream` |
| [Dataset metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets_metadata.md) | `v2.2.4` | Exactly one license label is required; `copyright-authors`, `other`, and `unknown` are available labels. | `documented-upstream` |
| [Output formatting](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/output_format.md) | `v2.2.4` | JSON formatting is command-specific; it is not a universal option for push/status/output. | `documented-upstream` |
| [CLI changelog](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/CHANGELOG.md) | through `2.2.4` | Kernel logs, SSE follow, accelerator quota, paginated outputs, and recent transport fixes exist in released history. | `documented-upstream` |
| [CLI package metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/pyproject.toml) | `v2.2.4` | Python `>=3.11`; official client depends on `kagglesdk >=0.1.35,<1.0`, which is not an exact transitive pin. | `documented-upstream` |
| [Official managed Python environment](https://github.com/Kaggle/docker-python/tree/1ed8d43599a7faa1b630baeba82c57def3a8e33b) | commit `1ed8d435...`, 2026-09-05 | Environment provenance exists and changes independently of the CLI; it is not an account capability guarantee. | `documented-upstream` |

## Observed official client surface

### Authentication

The selected CLI documents four credential paths: browser-based OAuth, the
`KAGGLE_API_TOKEN` environment variable, `~/.kaggle/access_token`, and the legacy
`~/.kaggle/kaggle.json` file. The first project probe will support only explicitly
configured modes; it will not scan arbitrary host locations or accept credentials through
job specifications.

K-01 remains unverified for an operator account. A read-only probe must report which
configured mechanism was attempted without printing the credential.

### Kernel submission

`kaggle kernels push` uploads code and metadata and starts a run. The documented command
accepts an accelerator selector and a maximum runtime in seconds. The metadata format
supports a private kernel, GPU, remote internet, machine shape, and attached data sources.

The v2.2.4 wrapper invokes the official client's save operation directly. No retry wrapper is
visible at that wrapper call site, but transitive SDK and HTTP behavior still requires audit.
The project therefore does not claim exactly-once submission or assume a timeout proves
rejection.

### Execution identity

The documented `kernels status` and `kernels output` commands operate on a kernel reference
and describe the **latest run**. Logs likewise resolve the current/latest session for the
kernel reference. That is not a sufficiently strong common contract for repeatedly updating
one shared slug.

The initial identity strategy is therefore one deterministic, provider-safe kernel slug per
attempt, persisted before submission. A runner manifest must contain the job ID, attempt ID,
attempt nonce, and input/bundle digests. Stronger provider version/session identifiers will
be retained as opaque adapter data when the official client exposes them.

This reduces stale-output confusion but does not create an exactly-once guarantee. A lost
submission response still enters reconciliation; it is never answered by blindly pushing
again.

### Status

The official status surface exists, but the complete raw state vocabulary and transition
semantics have not been established for the selected account. Adapter code must retain
unknown raw values and may observe a terminal state without first observing every
intermediate state.

M-1 must capture sanitized status fixtures from at least one complete lifecycle and one
controlled failure.

### Outputs and pagination

The official client can list/download kernel output files with pagination. The documentation
states that normal output download scans available pages, and the command also exposes
page size/token controls.

The surface still refers to the latest run of a slug. Collection is therefore accepted only
after the attempt manifest matches the frozen local identity. All pages, required files,
byte counts, and digests must be checked before publishing local artifacts.

### Logs

Released history and v2.2.4 source expose `kaggle kernels logs`, including a `--follow` mode
implemented with an SSE stream. The stable `docs/kernels.md` page does not currently provide
the same complete command documentation.

The project records the feature as `documented-upstream`, not `passed-live`. M-1 must
establish whether logs are available while queued/running, replayed on reconnect, delayed,
or only complete after termination for the tested account and run type.

### Quota

The released client exposes `kaggle quota`; source returns GPU and TPU total, used, and
remaining values in hours plus a refresh time. This proves an official query surface, not
that every field will be present or sufficiently precise for admission decisions.

Missing or failed quota remains `unknown`, never zero or unlimited. The owner's approximate
weekly allowance is motivation, not a hard-coded entitlement.

### Private input staging

Dataset creation is private unless `--public` is selected, and dataset status can be queried.
Kernel metadata can attach dataset sources. This makes a private dataset a candidate
transport for immutable bundle/input staging.

Dataset metadata requires one license label. `copyright-authors` is a candidate that avoids
silently applying a permissive license, but the operator remains responsible for rights and
the final label. M-1 must verify private visibility, uploaded checksums, processing readiness,
attachment behavior, and cleanup identity before this transport is accepted.

### Timeout and termination

The push command and client metadata carry a session timeout. This documents a provider-side
control but not its exact clock origin, enforcement latency, or relation to hardware release.

A bounded runner deadline remains mandatory. Success requires provider terminal evidence,
runner success, and verified required outputs. Exact accelerator release/accounting time is
reported only when observable.

### Cancellation

The v2.2.4 `kaggle kernels` command surface has no cancellation command. The official
repository has an open request discussing a generated SDK cancellation request and the lack
of a public session identifier in ordinary status responses, but that issue is a research
lead rather than a supported contract.

For the project, cancellation support is `unknown`. M-1 may test a narrow official-client
bridge only if a stable public API and reliably persisted target identity can be demonstrated.
Otherwise the adapter advertises cancellation as unsupported/manual-required. Deleting a
kernel or dataset is never cancellation.

## Transport decision gate

The first adapter boundary will use a pinned official-client Python environment controlled
by the Go runtime:

1. invoke commands with argument arrays, a controlled working directory, minimal
   environment, bounded stdout/stderr, deadlines, and process-tree cleanup;
2. use documented CLI operations where their output and identity are sufficient;
3. add a narrow Python bridge over public official-client APIs only where structured
   results or identity cannot be obtained safely from the CLI;
4. return machine-readable data on stdout and diagnostics on stderr;
5. prohibit undocumented browser/editor APIs and implicit mutation retries; and
6. lock the CLI and transitive SDK versions used by fixtures and live evidence.

The exact per-operation CLI/bridge mapping remains a decision gate until M-1 captures the
actual responses. This is deliberate: the provider-neutral contract is stable, while the
transport details remain evidence-driven.

## M-1 probe sequence

| Probe | External credentials | Compute/quota | Required output |
|---|---:|---:|---|
| Install/version/command inventory | No | No | Exact CLI, Python, SDK lock, and sanitized command fixtures. |
| Read-only authentication and quota | Yes | No GPU allocation | K-01/K-12 account-scoped result with redacted diagnostics. |
| Private synthetic dataset create/status | Yes | Storage/API side effect | K-03/K-04 privacy, checksums, readiness, identity, and cleanup record. |
| Unique private GPU smoke run | Yes | Finite GPU budget | K-02/K-05/K-06/K-09/K-10/K-13 evidence and result manifest. |
| Logs during a deliberately short slow run | Yes | Finite GPU budget | K-08 availability/replay/timing behavior. |
| Paginated multi-file output collection | Yes | Covered by smoke run where practical | K-07 pages, identity, sizes, and digests. |
| Restart and ambiguous-response experiment | Yes | Finite, explicitly bounded | K-05/K-15 proof that no automatic second run is created. |
| Cancellation investigation | Yes | Finite only if safe and authorized | K-11 supported operation plus target identity, or explicit unsupported/unknown result. |
| Offline-network run | Yes | Finite GPU budget | K-14 result using staged inputs with remote internet disabled. |
| Ownership-safe cleanup dry run/apply | Yes | No new compute | K-16 ledger, identity checks, and idempotent result. |

No live probe should combine unrelated unknowns merely to save one run. Each mutation uses a
unique test-owned resource and a finite cleanup plan.
