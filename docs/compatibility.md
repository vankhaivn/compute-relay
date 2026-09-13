# Compatibility and live-verification baseline

> **Status:** M-0 target matrix. No runtime binary or live Kaggle path exists yet.
>
> **Reviewed:** 2026-09-13

Compatibility claims require native evidence. A successful cross-compile, parser fixture, or
upstream feature description is not enough to claim that an operator can run the complete
workflow.

## Toolchain baseline

| Component | M-0 selection | Evidence/status | Pinning rule |
|---|---|---|---|
| Go | `go1.27.1` | Current supported stable toolchain reviewed from official Go release history. | Pin in `go.mod`/CI/tooling when the module is created; review patch upgrades deliberately. |
| Kaggle CLI | `kaggle==2.2.4` | Current stable release, tag commit `f0afa326...`; no live account test. | Exact direct pin in the adapter environment. |
| Python for provider client | `>=3.11`; initial target 3.11 | Required by Kaggle CLI v2.2.4. | Pin a concrete supported interpreter in bootstrap/container assets. |
| Kaggle SDK | CLI declares `>=0.1.35,<1.0` | Exact resolved version not yet locked. | Generate a reproducible lock and record the resolved SDK before fixtures/live tests. |
| SQLite driver | `modernc.org/sqlite` family | ADR-0003 selects the CGo-free `database/sql` driver; no project build exists. | Pin exact driver and matching `modernc.org/libc` revisions in `go.mod`. |
| Remote Kaggle environment | official `Kaggle/docker-python` provenance | Moving upstream source reviewed at `1ed8d435...`; actual run image unknown. | Record image/environment facts from each live evidence run; never assume parity from source alone. |

## Control-plane target matrix

| Host target | Build status | Native test status | Provider-client status | Claim today |
|---|---|---|---|---|
| Linux `amd64` | Not implemented | Not tested | Not tested | Target only |
| Linux `arm64` | Not implemented | Not tested | Not tested | Target only |
| macOS `amd64` | Not implemented | Not tested | Not tested | Target only |
| macOS `arm64` | Not implemented | Not tested | Not tested | Target only |
| Windows `amd64` | Not implemented | Not tested | Not tested | Target only |

A platform becomes supported only after native evidence covers:

- build, unit/component tests, race checks where supported, and startup/shutdown;
- SQLite open/migrate/transaction/recovery/backup behavior;
- exclusive state-directory locking;
- restrictive token/state/temp-file permissions appropriate to the OS;
- path normalization, archive containment, and atomic publication;
- subprocess timeout, signal/process-tree cleanup, bounded output, and environment handling;
- official-client installation/version/auth diagnostics; and
- direct-install documentation executed from a clean environment.

Windows remote jobs are not part of this matrix. Kaggle execution is a separate Linux
provider environment; Windows support refers to the local control plane and CLI.

## Kaggle client capability matrix

| Surface in v2.2.4 | Upstream evidence | Live evidence | Product conclusion |
|---|---|---|---|
| OAuth/token authentication | Documented | None | Probe available only after explicit credential configuration. |
| Private kernel metadata | Documented | None | Candidate; privacy must be observed. |
| Private dataset creation/status | Documented | None | Candidate staging transport. |
| Accelerator selection | Documented | None | Account eligibility and actual GPU use unknown. |
| Session timeout | Documented | None | Semantics unknown; runner deadline still required. |
| Kernel status | Documented | None | Raw states and latest-run behavior require fixtures/live capture. |
| Output listing/download/pagination | Documented | None | Candidate; manifest identity and digest verification required. |
| Kernel logs and `--follow` | Released source/changelog | None | Availability/timing/replay unknown. |
| GPU/TPU quota | Released source/command | None | Values/precision/account scope unknown. |
| Kernel cancellation | No v2.2.4 CLI command | None | Unknown; advertise unsupported/manual-required until proven. |
| Exact hardware release time | No established surface | None | Not observable unless later evidence proves otherwise. |

## Live-verification checklist

Every live evidence record must include:

- [ ] explicit owner/operator authorization for the exact probe;
- [ ] finite API/storage/compute budget and abort condition;
- [ ] client version, Python version, exact resolved SDK lock, and source tag/commit;
- [ ] sanitized account/provider-instance scope without credentials;
- [ ] generated resource IDs/slugs and proof they are test-owned;
- [ ] command/bridge operation and bounded timeout values;
- [ ] raw response fixtures sanitized for secrets/private account data;
- [ ] timestamps in UTC and local runtime version/commit;
- [ ] runner, bundle, input, and output identity digests;
- [ ] actual accelerator/environment manifest where compute is used;
- [ ] provider terminal observation and explicit release-observability statement;
- [ ] quota observation before/after when available, without treating elapsed time as billing;
- [ ] cleanup dry run, apply result, and retained ownership record;
- [ ] final evidence level (`passed-live`, `unsupported`, or `blocked-environment`);
- [ ] known limitations and next safe operator action.

## Evidence-to-claim rules

| Evidence | Permitted claim |
|---|---|
| Proposal requirement | “The project requires/plans this behavior.” |
| Official docs/source | “The reviewed upstream version exposes this surface.” |
| Offline unit/component/contract test | “The local implementation passed this deterministic behavior.” |
| Adapter fixture | “The adapter parses/maps this captured shape.” |
| Authorized live read-only probe | “The tested account/client returned this observation.” |
| Authorized live compute test | “The tested account/client completed this bounded path.” |

No lower evidence tier may be summarized as a higher one. In particular:

- fake-provider success is not Kaggle support;
- `kaggle --help` is not account eligibility;
- a successful cross-compile is not native platform support;
- device listing is not proof of GPU computation;
- provider terminal status is not exact hardware release time; and
- one account observation is not a universal quota or availability guarantee.

## Upgrade policy

A provider-client or toolchain upgrade requires:

1. source/changelog review and dependency/license check;
2. refreshed command/response fixtures;
3. offline adapter and failure-semantic tests;
4. explicit live re-verification for affected capability claims;
5. compatibility/evidence date updates; and
6. an ADR only when the change alters a material boundary or behavior.

Security patch upgrades may be expedited, but they do not bypass evidence disclosure.
