# Remaining implementation and acceptance plan

Use [current status](status.md) for available behavior and the
[operator checklist](development/validation-checklist.md) for executable validation.
This plan retains task IDs for future work; completed implementation history lives in Git.

## Current boundary

Portable core and durable components are implemented offline. Kaggle components and the fixed
acceptance harness are implemented, and the scoped private-staging/Tesla-T4/restart/artifact path
has been qualified live. The main binary supplies local operator commands, admission profiles,
application requests and delivery of published artifacts. Normal `serve` remains admission-only
with no provider or collection workers.

## Remaining provider capability boundaries

The fixed live experiment has closed the scoped private-staging, actual-GPU, lifecycle/result and
separate-process acceptance path. The rows below remain product/capability limits rather than open
bugs in that qualified path.

| Task | Remaining work / conclusion |
|---|---|
| M1-02 | Authentication and quota reads work for the qualified account; quota remains an observation rather than a reservation or universal entitlement. |
| M1-06 | Provider timeout enforcement and a verified cancellation target need a separate supported capability/procedure; current batch cancellation remains manual-required. |
| M1-07 | Deliberately lost provider acknowledgement is a distinct fault qualification; normal restart/reconciliation does not claim to induce that ambiguity. |
| M1-08 | Record the provider support/blocked capability mapping when general worker integration is ready for a release decision. |

Large multi-page output, exact hardware release and remote cleanup apply remain explicit limits in
the provider guides. Credential authorization never implies authorization for GPU use, destructive
cleanup, public data or paid capacity.

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
slice narratives here. A validation-only PR records only the focused evidence needed for its task; it does not change
implementation status by inference. No automatic compute retry, provider fallback or live side effect is
permitted merely because a plan row exists.
