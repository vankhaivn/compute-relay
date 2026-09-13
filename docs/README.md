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
| [`scope-and-requirements.md`](scope-and-requirements.md) | M-0 baseline | Stable requirement IDs mapped to components, acceptance evidence, and milestones. |
| [`architecture.md`](architecture.md) | Approved baseline | Provider-neutral layers, responsibilities, entities, and invariants. |
| [`roadmap.md`](roadmap.md) | Active | Acceptance-based M-0 through M-6 outcomes and current milestone status. |
| [`implementation-plan.md`](implementation-plan.md) | M-0 in review | Dependency-aware task backlog, live/offline boundaries, and acceptance tests. |
| [`risk-register.md`](risk-register.md) | M-0 baseline | Ranked risks, predetermined responses, evidence gates, and decision register. |
| [`compatibility.md`](compatibility.md) | M-0 target matrix | Toolchain/provider/host targets and rules for support claims/live evidence. |
| [`decisions/`](decisions/README.md) | Active ADR system | Material architecture and policy decisions. |
| [`research/`](research/README.md) | Active evidence system | Primary-source and live-test evidence rules. |
| [`research/kaggle-interface-review.md`](research/kaggle-interface-review.md) | Upstream review complete | Pinned official client surfaces, gaps, transport gate, and M-1 probe sequence. |
| [`research/kaggle-feasibility.md`](research/kaggle-feasibility.md) | Upstream review complete; live blocked | K-01 through K-16 evidence ledger and go/no-go rule. |
| [`providers/`](providers/README.md) | Provider-doc template | Adapter-specific capability and compatibility documentation. |
| [`development/commit-convention.md`](development/commit-convention.md) | Active policy | Commit subject and history rules. |
| [`development/repository-workflow.md`](development/repository-workflow.md) | Active policy | Branch, PR, validation, and direct-main rules. |

API, job-contract, operations, testing, provider support, troubleshooting, and security
design documents should be added when implementation makes them concrete. Do not create
empty documents merely to mirror a proposed tree.

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
level and capability support must not be collapsed into one optimistic checkbox.

## Required update paths

| Change | Documents to review |
|---|---|
| Owner-approved scope | proposal, requirement matrix, roadmap, plan, risks |
| Public API/job contract | requirement matrix, architecture, OpenAPI/schema docs, compatibility, plan |
| Persistence/recovery | ADR, architecture, risks, plan, operations/testing |
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
