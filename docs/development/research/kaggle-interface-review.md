# Pinned Kaggle interface evidence

**Source review date: 2026-09-13.** This is a versioned upstream reference, not an operator
runbook or current-account compatibility claim. CLI baseline: **2.2.4**, tag commit
`f0afa32699d28c97f82691728ada3ed8c16c5abf`. Later component decisions use the project's
locked SDK **0.1.35** and are described in the linked provider guides. Editing this document
for clarity does not constitute a new upstream or live review.

## Source map

| Primary source | Relevant evidence |
|---|---|
| [CLI release](https://github.com/Kaggle/kaggle-cli/releases/tag/v2.2.4) and [package metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/pyproject.toml) | Selected release; Python 3.11+ and declared SDK range, not the project's exact dependency lock. |
| [Installation/authentication](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/README.md) | OAuth, token environment/file and legacy credential paths exposed upstream. The project supports only explicitly configured sources. |
| [Kernel commands](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/kernels.md) and [metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/kernels_metadata.md) | Submission, accelerator/time settings, private/internet flags and data sources; ordinary CLI observations describe latest-run behavior. |
| [Dataset commands](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets.md) and [metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets_metadata.md) | Private creation, readiness/status and file operations; one license label, including ownership-preserving choices. |
| [Output formatting](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/output_format.md) and [changelog](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/CHANGELOG.md) | JSON availability is command-specific; logs/follow and quota surfaces exist but do not prove live timing or account allowance. |
| [SDK kernel service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py) and [types](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py) | Structured save/read, status/output and cancellation-target types used in component reviews; live requests are validated against the narrower project contract. |
| [SDK transport](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_http_client.py) and [decoding](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_object.py) | Private session-layout dependency, ambient defaults and raw-field checks that must be reviewed on upgrade. |
| [Managed environment source](https://github.com/Kaggle/docker-python/tree/1ed8d43599a7faa1b630baeba82c57def3a8e33b) | Environment provenance reviewed at this commit, not the image/device actually assigned to an operator run. |

## Consequences for the implementation

**Credentials and transport.** The locked environment isolates official SDK calls from the
application. Local package/help inventory does not authenticate. Explicit account reads use
an allowlisted environment reference and verified server identity. A private, version-bound
session hook enforces fixed destinations, bounded data, TLS and no hidden retry/redirect.
It is not a stable public SDK injection API. See [preflight](../providers/kaggle-preflight.md)
and [ADR-0001](../decisions/0001-official-kaggle-client-boundary.md).

**Identity and submission.** A shared/latest slug is insufficient. Use a prewritten per-attempt
name, exact numeric ID/version/source checks and runner nonce/digests. SaveKernel is upsert,
not atomic create-only; response loss cannot authorize another save. An external same-version
rerun is not ruled out by those observations. See [execution](../providers/kaggle-execution.md).

**Staging.** A private create receipt is not verified readiness. Require observed private
metadata, complete original bytes and readiness; no public fallback or automatic relicensing.
Attachment by owner/slug is not a guaranteed immutable mount. See
[staging](../providers/kaggle-staging.md).

**Status, timeout and cancellation.** Inspect raw fields before SDK defaults. Unknown or
missing status stays uncertain. A requested provider timeout is not proof of enforcement or
hardware release. The current batch reference lacks a verified cancellation session target,
so cancellation is manual-required without substituting deletion or a kernel ID.

**Logs and quota.** Upstream follow/SSE does not make the project a live-log implementation.
The project uses bounded versioned snapshots and exact raw durations/reservations for
conservative quota. Missing values/reset times are not invented. See
[operational mappings](../providers/kaggle-operations.md).

**Artifacts.** Use complete current-session output listing and explicit version-number file downloads rather than arbitrary
listing URLs or bulk ZIP extraction. The manifest selects candidates; independently verified
bytes and final acknowledgement must precede local publication. See
[artifact retrieval](../providers/kaggle-artifacts.md).

## Evidence boundaries

Source and mocked-HTTP tests alone do not prove account eligibility, private mounting, actual GPU
use, provider timeout behavior, live log availability or ownership-safe remote cleanup. The
[K-01–K-16 support matrix](kaggle-feasibility.md) separates the fixed path that has been qualified
live from provider guarantees that remain unsupported or unobservable. Use the
[validation checklist](../validation-checklist.md) for re-qualification after relevant
provider/toolchain changes; keep complete run records private and do not promote source review
alone into a live-support claim.
