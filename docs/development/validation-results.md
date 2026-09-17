# Operator validation results

This is the editable handoff for the [validation checklist](validation-checklist.md).
It contains current operator evidence and unresolved failures, not development commit diaries.
**No operator run has been recorded yet.** Historical offline CI is not a pass for a new local
installation or a real Kaggle account. Do not prefill success from documentation or test names.

## Status vocabulary

`not-run`, `pass-offline`, `pass-live`, `fail`, `blocked-environment`, `blocked-authorization`,
`blocked-procedure`, `blocked-implementation`, `unsupported-reviewed`.
Every pass needs a run ID, exact command/check and evidence. `unsupported-reviewed` needs the
capability scope and reviewer; it must not replace a required safety check. Fixed code awaiting
retest is still an open failure with disposition `fix-pending-verification`.

## Current check register

Update this table to point to the latest relevant run, retaining older records below.

| Check | Status | Run / evidence or blocker |
|---|---|---|
| V01 source/toolchain/build | not-run | — |
| V02 offline suites | not-run | — |
| V03 local installation/authority | not-run | — |
| V04 profile/upload/admission | not-run | — |
| V05 reopen/conflict/control/authorization | not-run | — |
| V06 artifact delivery on operator host | not-run | — |
| V07 local Kaggle preflight | not-run | — |
| V08 authorized account reads | not-run | Approval and local credentials required. |
| V09 fixed experiment preparation | not-run | — |
| V10 private staging/GPU submission | not-run | Separate effect/budget approval required. |
| V11 separate-process live results | not-run | — |
| V12 collection failure recovery | not-run | Applicable only to an actual failed/interrupted transfer. |
| V13 live optional/multi-page probes | blocked-procedure | Fixed six-file smoke does not force these cases; bounded probe required. |
| V14 timeout/cancellation qualification | blocked-procedure | No timeout-fault CLI; cancellation target is unverified/manual. |
| V15 induced provider-response loss | blocked-procedure | Reviewed injection/recovery/evidence plan required. |
| V16 cleanup/coordinated restore | blocked-procedure | No acceptance cleanup apply/general backup CLI; exact-target authorization required. |
| V17 general runtime/release acceptance | blocked-implementation | Remaining provider/worker/product/release integration. |
| V18 gate review | not-run | Depends on actual evidence and bug dispositions. |

## Gate decisions

A check can contribute evidence without closing an entire gate. Update rows only after review.

| Gate | Decision | Evidence / reviewer / UTC date |
|---|---|---|
| M1-02 account/quota | unverified | — |
| M1-03 private staging | unverified | — |
| M1-04 actual GPU | unverified | — |
| M1-05 lifecycle/logs/all-page results | unverified | — |
| M1-06 timeout/cancellation conclusion | unverified | — |
| M1-07 induced ambiguity/recovery | unverified | — |
| M1-08 provider go/blocked decision | blocked-evidence | — |
| M4-06 scoped live experiment | unverified | Harness implementation is not a live pass. |
| M5/M6 product/release acceptance | blocked-implementation | Tests cannot replace missing code. |

## Run template

Copy this section for each run. Replace placeholders only with reviewed non-secret values.

### RUN-YYYYMMDD-NN

| Field | Value |
|---|---|
| Operator / agent | UNSET |
| UTC start / finish | UNSET |
| Source commit / branch | UNSET |
| Dirty code or config changes | UNSET; attach a sanitized patch/hash or explicitly `none`. |
| OS / architecture / filesystem | UNSET |
| Go / Python / uv / Kaggle / SDK versions | UNSET; report actual versions, including unavailable tools. |
| Main / preflight / acceptance executable SHA-256 | UNSET |
| Account alias / shape | UNSET; no actual account identifier or token value. |
| Private state/evidence location alias | UNSET; raw paths stay on operator host. |
| Non-secret configuration summary / private original hash | UNSET |
| Read-only approval | UNSET; who/when/scope, or not authorized. |
| Staging/GPU approval | UNSET; who/when, one attempt and explicit finite budget, or not authorized. |
| Fault/cleanup approval | UNSET; separate exact scope, or not authorized. |

| Check | UTC time | Exact command/check, private values aliased | Exit / reported status | Expected vs actual | Evidence ID/hash | Result / bug ID |
|---|---|---|---|---|---|---|
| Vxx | UNSET | UNSET | UNSET | UNSET | UNSET | not-run |

**Evidence summary:** Record sanitized reports and the decisive assertions, not only log counts.
Include skipped tests, scope limits, bytes/digests, original identity equality and observed process
separation where relevant. Raw log/database/configuration locations remain private aliases.

**Effects:** Account calls / uploads / submissions / collection / local publication actually
attempted: UNSET. For each effect, distinguish confirmed, possible/unknown and not attempted.
Do not infer remote execution count or termination from local call count or an exit code.

**Open failures / dependent checks:** UNSET.

**Gate decisions proposed:** UNSET. Review is separate from a test's status.

**Reviewer / decision / UTC date:** UNSET.

## Bug register

No reported bugs yet. Copy [the bug template](bug-report-template.md) below for every unexpected
failure. Use a stable ID in the check table. Do not paste secrets or full raw responses.

| Bug ID | Affected check / source SHA | Status | Fix SHA / retest run |
|---|---|---|---|
| — | — | No report recorded | — |

## Retest and handoff rules

Keep the failed run and append the retest. Include fix SHA, source/binary changes, exact original
case, negative regression results and unresolved effects. Never mark a bug resolved solely because
code was merged. For an interrupted acceptance run, preserve its original binary and state;
resolve remote activity before any separately authorized new GPU experiment.

Before sharing, inspect the diff for token literals, signed URLs, account names, raw private
paths, environment dumps and embedded inputs. Push only these sanitized documents on a report
branch. Do not commit raw evidence, token files, binaries or runtime/acceptance directories.
