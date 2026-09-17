# Remaining implementation and acceptance plan

Use [current status](status.md) for available behavior and the
[operator checklist](development/validation-checklist.md) for executable validation.
This plan retains task IDs for future work; completed implementation history lives in Git.

## Current boundary

Portable core and durable components are implemented offline. Kaggle components and the fixed
acceptance harness are implemented, but live acceptance is unverified. The main binary supplies
local operator commands, admission profiles, application requests and delivery of published
artifacts. Normal `serve` remains admission-only with no provider or collection workers.

## Operator evidence still required

| Task | Required evidence | Current state |
|---|---|---|
| M1-02 | Authorized authentication and account/quota reads, including missing/invalid credential behavior. | Not run live. |
| M1-03 | Private staging, original bytes, readiness and exact resource ownership. | Not run live. |
| M1-04 | Bounded GPU computation, not device listing alone. | Not run live. |
| M1-05 | Lifecycle, logs and complete identified output retrieval with verified hashes. | Not run live; pagination needs a case large enough to exercise multiple pages. |
| M1-06 | Controlled timeout and cancellation capability/target conclusion. | Separate procedure/authorization required; current batch cancellation is manual. |
| M1-07 | Deliberately lost/ambiguous provider response followed by safe recovery with no new submission. | Separate fault procedure required; ordinary restart is insufficient. |
| M1-08 | Review evidence and record a provider go/blocked decision and capability mapping. | Blocked on evidence or precisely documented unsupported capabilities. |
| M4-06 live | Fixed private GPU experiment, verified results and distinct-process resume. | Harness available; no live qualifying report recorded. |

Record outcomes and evidence IDs in [validation results](development/validation-results.md),
not in a growing narrative here. A fixed GPU pass may supply evidence to several rows, but
cannot automatically close all of them. Credential authorization never implies authorization
for GPU use, destructive cleanup, public data or paid capacity.

## M5 — Product integration

| Task | Work remaining / acceptance |
|---|---|
| M5-01 | Complete general provider registration, bounded worker lifecycle and remaining log/cleanup surfaces. Preserve frozen configuration, one-shot intents, authority and shutdown ownership. |
| M5-02 | Strict TOML runtime configuration, precedence, paths, safe defaults and validated examples; exclude secret values. Depends on M4-01/M5-01 interfaces. |
| M5-03 | Doctor with separate local/read-only/explicit-compute modes. Depends on M4-01/M5-02. |
| M5-04 | Verify and document GPU smoke plus a small pinned open-access LLM batch example. Depends on M4-06 and verified environment/model terms. |
| M5-05 | Thin Node.js, Python and Go HTTP clients handling receipts, unknown states, explicit controls and checked downloads. Depends on M5-01/M5-04. |
| M5-06 | Clean-install/build guide, optional unprivileged container and platform packaging. Depends on M5-01–M5-05. |

M5-01a local lifecycle, M5-01b profile/application commands and M5-01c artifact delivery are
implemented. They do not satisfy the remaining parent M5-01 worker/runtime requirements.
Do not invent an enable-dispatch flag, map an acceptance directory into a normal installation,
seed production SQL, or advertise a queued job as running to bridge that gap.

## M6 — Release acceptance

| Task | Work remaining / acceptance |
|---|---|
| M6-01 | Security review, authorization/archive/SSRF/secret/limit corpus and redacted diagnostics. |
| M6-02 | Coordinated database/blob backup/restore, migration and disk-failure qualification; ownership-safe cleanup and recovery guidance. |
| M6-03 | Native clean-install/runtime evidence for every claimed host and current provider version. |
| M6-04 | Reproducible release artifacts, checksums/signing, SBOM/notices and support/security policy. |
| M6-05 | Final requirement-to-implementation-to-evidence audit; unresolved items stay explicit. |

M6 follows usable M5 integration; component tests do not replace full product acceptance.
Unsupported optional provider features may remain honestly unsupported. Required private
execution/result safety failures block provider go rather than trigger an unapproved workaround.

## Delivery rule

Implement one focused slice per PR, push reviewable checkpoints, keep code and documentation
commits separate, record checks in the PR and stop for owner merge. Do not append finished
slice narratives here. A validation-only PR updates the results/bug ledger, not implementation
status by inference. No automatic compute retry, provider fallback or live side effect is
permitted merely because a plan row exists.
