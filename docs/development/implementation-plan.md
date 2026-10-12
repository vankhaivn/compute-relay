# Remaining implementation plan

The path to a usable release, with task IDs for remaining work. [Current status](../status.md)
describes available behavior and the [validation checklist](validation-checklist.md) the executable
checks. No release dates are promised; completed history lives in Git.

## Outcomes

| Outcome | Current boundary | Exit condition |
|---|---|---|
| M0 — Scope and evidence | Approved scope, requirements and provider unknowns recorded. | Preserve them. |
| M1 — Real-provider proof | Scoped private staging, Tesla T4 execution, restart and artifact path qualified live; M1-02/06/07/08 below remain. | Timeout, fault and capability conclusions plus a provider go decision. |
| M2 — Portable core | Implemented offline. | Contracts, access controls, input handling and the finite runner stay regression-tested. |
| M3 — Durable orchestration | Implemented offline. | Receipts, one-shot intents, uncertainty, collection pins and recovery hold under fault tests. |
| M4 — Integrated Kaggle batch | Explicit and managed serve modes compose bounded workers. | Exact-account, private, finite execution and verified results survive a separate process. |
| M5 — Usable developer product | Local runtime, CLI, artifact delivery, managed connections and logs available. | M5 tasks below. |
| M6 — Release hardening | Outstanding. | M6 tasks below. |

If private staging, identity-safe results or bounded execution fails, retain the exact blocker. Do
not substitute public resources, browser automation, an always-on worker, another account or a paid
fallback. Missing optional capabilities stay unsupported or unknown rather than simulated.

## Provider capability boundaries

| Task | Remaining work / conclusion |
|---|---|
| M1-02 | Authentication and quota reads work for the qualified account; quota remains an observation rather than a reservation or universal entitlement. |
| M1-06 | Provider timeout enforcement and a verified cancellation target need a separate supported capability/procedure; current batch cancellation remains manual-required. |
| M1-07 | Deliberately lost provider acknowledgement is a distinct fault qualification; normal restart/reconciliation does not claim to induce that ambiguity. |
| M1-08 | Record the provider support/blocked capability mapping for a release decision. |

Large multi-page output, exact hardware release and Kaggle remote cleanup apply remain explicit
limits in the provider guides. Credential authorization never implies authorization for GPU use, destructive
cleanup, public data or paid capacity.

## M5 — Product integration

| Task | Work remaining / acceptance |
|---|---|
| M5-01 | Public provider quota and cleanup surfaces and broader product hardening; preserve frozen configuration, one-shot intents, authority and shutdown ownership. |
| M5-02 | Strict TOML runtime configuration, precedence, paths, safe defaults and validated examples; exclude secret values. |
| M5-03 | Doctor with separate local/read-only/explicit-compute modes. Depends on M5-02. |
| M5-04 | Verify and document a GPU smoke job plus a small pinned open-access LLM batch example. Depends on verified environment/model terms. |
| M5-05 | Thin Node.js, Python and Go HTTP clients handling receipts, unknown states, explicit controls and checked downloads. Depends on M5-04. |
| M5-06 | Clean-install/build guide, optional unprivileged container and platform packaging. Depends on M5-01–M5-05. |
| M5-07 | Qualify the SSH host provider (SSH-01–SSH-04) on the operator's Mac Intel host: connection verification, one CPU attempt, a cancelled attempt and requested cleanup, using [V09](validation-checklist.md). |

## M6 — Release acceptance

| Task | Work remaining / acceptance |
|---|---|
| M6-01 | Security review, authorization/archive/SSRF/secret/limit corpus and redacted diagnostics. |
| M6-02 | Coordinated database/blob backup/restore, migration and disk-failure qualification; ownership-safe cleanup and recovery guidance. |
| M6-03 | Native clean-install/runtime evidence for every claimed host and current provider version. |
| M6-04 | Reproducible release artifacts, checksums/signing, SBOM/notices and support/security policy. |
| M6-05 | Final requirement-to-implementation-to-evidence audit; unresolved items stay explicit. |

M6 follows usable M5 integration; component tests do not replace full product acceptance.
Required private execution/result safety failures block provider go rather than trigger an
unapproved workaround. A plan row never permits compute retry, provider fallback or a live effect.
