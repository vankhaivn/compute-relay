# Documentation system

This directory separates approved product direction, living architecture, implementation
planning, provider evidence, decisions, compatibility, and operational guidance.
Documentation must make clear whether behavior is proposed, implemented, tested offline,
documented upstream, or verified live.

## Authority and status

1. [`proposal.md`](proposal.md) is the approved product brief and default-answer register.
2. [`../AGENTS.md`](../AGENTS.md) defines mandatory repository workflow and contributor
   behavior.
3. Accepted ADRs under [`decisions/`](decisions/README.md) record material implementation
   choices that preserve the approved scope.
4. Research notes under [`research/`](research/README.md) record dated evidence; they do
   not silently change product requirements.
5. Living architecture, traceability, roadmap, compatibility, risk, and implementation
   documents describe current project knowledge and must be updated with code/evidence.

When documents conflict, do not silently reconcile them. Identify the conflict, preserve
owner-approved requirements, and record the technical resolution in an ADR or
owner-approved proposal update.

## Current map

| Path | Status | Purpose |
|---|---|---|
| [`proposal.md`](proposal.md) | Approved brief | Product decisions, defaults, boundaries, risks, and feasibility gates. |
| [`scope-and-requirements.md`](scope-and-requirements.md) | M-0 requirements baseline | Stable requirement IDs and minimum acceptance evidence; the fault matrix links the implemented offline evidence without changing requirements. |
| [`architecture.md`](architecture.md) | Active baseline | Provider-neutral layers, responsibilities, entities, and invariants. |
| [`domain-model.md`](domain-model.md) | M2-02 implemented offline | Typed IDs, entities, capability evidence, structured errors, independent state dimensions, and transition invariants. |
| [`../api/README.md`](../api/README.md) | Contracts and fifteen composable handlers implemented offline | Strict JSON Schema/OpenAPI contracts, fixtures, lock, operation status and validation workflow; production server remains separate. |
| [`providers/contract.md`](providers/contract.md) | M2-04 implemented offline | Provider contracts, deterministic fake, infrastructure ports and local smoke. |
| [`auth-and-objects.md`](auth-and-objects.md) | M2-05/M2-06 implemented offline; merged | Workspace authorization, HTTP guards, streaming upload, atomic blobs and recovery boundaries. |
| [`packaging-and-import.md`](packaging-and-import.md) | M2-07 implemented offline; merged | Explicit bundle commands, strict archive validation, rooted snapshots and opt-in local import. |
| [`https-ingestion.md`](https-ingestion.md) | M2-08 implemented offline; merged | Public HTTPS snapshots, DNS/peer/TLS and redirect defenses, bounded transfers and no source refresh. |
| [`../runner/README.md`](../runner/README.md) | M2-09 implemented offline; merged | Finite Linux runner, frozen-input manifest, setup/process/log/output bounds, CPU fixtures and provider-evidence limitations. |
| [`storage.md`](storage.md) | M3-01 implemented offline; merged | SQLite repositories, ordered migrations, process lock, database-only backup/restore and evidence limits; links to later persistence components. |
| [`admission.md`](admission.md) | M3-02 implemented offline; merged | Atomic job/attempt/idempotency, frozen references, pending preparation, state/event CAS and admission HTTP. |
| [`scheduler.md`](scheduler.md) | M3-03 implemented offline; merged | Durable FIFO/round-robin fairness, account capacity, fenced local claims, quota policy and the no-remote-side-effect boundary. |
| [`dispatch.md`](dispatch.md) | M3-04 implemented offline; merged | Input freeze, private staging, one-shot mutation intents, fenced reconciliation and cached recovery conditions. |
| [`operations.md`](operations.md) | M3-05 implemented offline; merged | Attempt-scoped controls, immutable receipt replay/current GET, cancellation evidence, frozen-input retry and transfer-only collection tickets. |
| [`collection.md`](collection.md) | M3-06 implemented offline; merged | Immutable result pins, bounded transfers, manifest/byte verification, atomic publication, scoped internal reads and collection-only recovery. |
| [`retention.md`](retention.md) | M3-07 implemented offline; merged | Named holds, atomic expiry/events, bound-store local sweep, metadata preservation and exact-ledger remote dry runs. |
| [`fault-matrix.md`](fault-matrix.md) | M3-08 offline qualification; PR #18 merged | The 25 numbered proposal scenarios, 34 nominated root tests, fresh executable evidence, requirement mapping and explicit limitations. |
| [`recovery.md`](recovery.md) | M3-08 operational semantics; PR #18 merged | Read independent state dimensions and choose receipt/reconcile/collect/retry/cancel/retention actions without fabricated commands or outcomes. |
| [`providers/kaggle-preflight.md`](providers/kaggle-preflight.md) | M4-01 offline/read-only foundation; PR #19 merged | Explicit credential references, local checks, opt-in account/quota reads, bounded SDK transport and process lifetime, operator commands and unchanged live/batch gates. |
| [`providers/kaggle-staging.md`](providers/kaggle-staging.md) | M4-02 offline staging; PR #20 merged | One-shot private creation, separately verified readiness and bytes, explicit source completion, M3 ownership/recovery integration and no automatic re-creation. |
| [`providers/kaggle-execution.md`](providers/kaggle-execution.md) | M4-03 offline execution; PR #21 in review | Locked source, per-attempt submission authority, exact-version/ID/source observation, raw-state truthfulness, SDK upsert and recovery/verification limits. |
| [`roadmap.md`](roadmap.md) | Active | Acceptance-based M-0 through M-6 outcomes and current milestone status. |
| [`implementation-plan.md`](implementation-plan.md) | Active | Dependency-aware task backlog, live/offline boundaries, and acceptance tests. |
| [`risk-register.md`](risk-register.md) | M-0 baseline | Ranked risks, predetermined responses, evidence gates, and decision register. |
| [`compatibility.md`](compatibility.md) | M-0 target matrix | Toolchain/provider/host targets and rules for support claims/live evidence. |
| [`decisions/`](decisions/README.md) | Active ADR system | Material architecture and policy decisions, including accepted ADR-0016 and proposed ADR-0017 for one-shot execution and exact-source observation. |
| [`research/`](research/README.md) | Active evidence system | Primary-source and live-test evidence rules. |
| [`research/kaggle-interface-review.md`](research/kaggle-interface-review.md) | Upstream review complete | Pinned official client surfaces, gaps, transport gate, and M-1 probe sequence. |
| [`research/kaggle-feasibility.md`](research/kaggle-feasibility.md) | Upstream review complete; live blocked | K-01 through K-16 evidence ledger and go/no-go rule. |
| [`providers/`](providers/README.md) | Provider documentation | Adapter-specific capability and compatibility documentation; the current components do not establish a fully integrated production batch Provider. |
| [`development/go-toolchain.md`](development/go-toolchain.md) | Active through M3-08 | Go module, developer/fault qualification commands, CI, build metadata, and dependency baseline. |
| [`development/commit-convention.md`](development/commit-convention.md) | Active policy | Commit subject and history rules. |
| [`development/repository-workflow.md`](development/repository-workflow.md) | Active policy | Branch, PR, validation, and direct-main rules. |

Additional testing, provider support, troubleshooting and security design documents should
be added when implementation makes them concrete. Do not create empty documents merely to
mirror a proposed tree. Offline qualification and provider components do not imply artifact
HTTP, remote cleanup apply, an overall-job deadline service or production runtime composition
is complete. The implementation plan owns the current stop/next-task boundary; historical
evidence in component guides and accepted ADRs must not be relabeled as a newer task's test run.

M3's implemented offline gate closed with PR #18's merge on 2026-09-16. M4-01/M4-02 are merged
in PRs #19/#20. M4-03 in PR #21 adds per-attempt execution under the same durable intent gates.
SDK transport and helper outcomes remain synthetic in their respective test tiers; M1 live
acceptance, M1-08 go and complete integrated batch activation remain separate requirements.
In-review means branch implementation exists, not that every PR check has passed; the PR
conversation records exact-head checks and any unresolved review blockers.

## Status vocabulary

Use precise labels where relevant:

- `planned`: approved or proposed, but not implemented;
- `implemented-offline`: code exists and offline tests pass;
- `documented-upstream`: supported by a cited primary source but not live-tested here;
- `passed-live`: exercised with authorized credentials and recorded evidence;
- `not-tested`: no relevant verification has been completed;
- `unsupported`: evidence establishes the capability is unavailable for the described mode;
- `blocked-environment`: verification could not run because an external prerequisite was
  unavailable.

Capability support may independently be `supported`, `unsupported`, or `unknown`. Evidence
level and capability support must not be collapsed into one optimistic checkbox. The
fault command's `25/25 passed-offline` means the nominated tests ran and passed on the
reported toolchain/platform; it is not 100-percent coverage or live-provider readiness.
Likewise, successful authentication/account checking is a dated read, not an execution permit;
preflight always keeps `batch_ready=false`. Staging readiness does not replace the new durable
submission gate. Kernel terminal observation is neither verified artifact availability nor
hardware-release evidence. Latest raw observations and confirmed attempt state remain distinct.

## Required update paths

| Change | Documents to review |
|---|---|
| Owner-approved scope | proposal, requirement matrix, roadmap, plan, risks |
| Public API/job contract | requirement matrix, architecture, domain model, `api/`, compatibility, plan |
| Domain state/error semantics | domain model, architecture, requirement matrix, `api/schemas/common*`, plan, tests |
| Authentication/object lifecycle | auth-and-objects, storage, ADR-0004/0008, architecture, `api/`, plan, tests |
| Admission/idempotency/profile snapshots | admission, ADR-0009, architecture, `api/`, plan, migration/CAS/HTTP tests |
| Scheduling/capacity/leases/quota | scheduler, ADR-0010, architecture, event schema, plan, migration/fencing/crash tests |
| Preparation/submission/reconciliation | dispatch, ADR-0011, provider contracts, architecture, event schema, plan, intent/crash/status tests |
| Explicit controls/receipt semantics | operations, ADR-0012, admission, dispatch, architecture, `api/`, plan, migration/race/receipt/HTTP tests |
| Result collection/publication | collection, ADR-0013, operations, dispatch, storage, architecture, result schema, plan, pin/transfer/publication/authorization tests |
| Retention/expiry/cleanup preview | retention, ADR-0014, storage, collection, admission, operations, architecture, common event schema, plan, pin/expiry/root/delete/preview tests |
| Fault qualification/evidence | fault-matrix, recovery, devtool catalog/checker/tests, requirement links, developer commands, plan, exact-head CI |
| Credential/preflight boundary | providers/kaggle-preflight, ADR-0015, plan, real pinned-SDK transport tests, environment/process isolation and explicit authorization checks |
| Private staging/readiness | providers/kaggle-staging, ADR-0016, plan, source-completion/process tests, real pinned-SDK fixtures and M3 ownership/restart tests |
| Kaggle execution/observation | providers/kaggle-execution, ADR-0017, plan, locked source/manifest tests, raw-field SDK fixtures, real process and M3 intent/restart/observation tests |
| Bundle/import safety | packaging-and-import, ADR-0005, `api/`, ignore example, plan, tests |
| HTTPS/SSRF policy | https-ingestion, ADR-0006, `api/`, plan, transport/service/API tests |
| Remote runner/execution contract | runner/README, ADR-0007, runner schemas/assets lock, public result schema, architecture, plan, CPU/contract tests |
| Persistence/recovery | storage, admission, scheduler, dispatch, operations, collection, retention, recovery, fault-matrix, ADR-0008/0009/0010/0011/0012/0013/0014, architecture, domain model, risks, plan, migrations/tests |
| Provider client/version/capability | interface review, feasibility, provider docs, compatibility, risk register |
| Live probe | feasibility gate, sanitized evidence, capability matrix, compatibility date |
| Host support claim | compatibility matrix, installation, CI evidence |
| Release behavior | changelog, support/security, compatibility, definition-of-done audit |

## Writing rules

- Date mutable provider research and record exact client versions/tags/commits.
- Link requirements to components and acceptance tests.
- Keep examples synchronized with schemas and executable behavior.
- State unknowns and operational consequences explicitly.
- Never place credentials, sensitive account data, private inputs, or full provider
  responses in documentation.
- Prefer small focused documents and stable relative links.
- Distinguish proposal requirements, upstream documentation, offline tests, and live
  observations.
