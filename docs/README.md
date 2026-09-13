# Documentation system

This directory separates approved product direction, living architecture, implementation planning, provider evidence, and operational guidance. Documentation must make clear whether behavior is proposed, implemented, tested offline, documented upstream, or verified live.

## Authority and status

1. [`proposal.md`](proposal.md) is the approved product brief and default-answer register.
2. [`../AGENTS.md`](../AGENTS.md) defines mandatory repository workflow and contributor behavior.
3. Accepted ADRs under [`decisions/`](decisions/README.md) record material implementation choices that preserve the approved scope.
4. Research notes under [`research/`](research/README.md) record dated evidence; they do not silently change product requirements.
5. Living architecture and implementation documents describe the current repository state and must be updated with code.

When documents conflict, do not silently reconcile them. Identify the conflict, preserve owner-approved requirements, and record the technical resolution in an ADR or owner-approved proposal update.

## Current map

| Path | Status | Purpose |
|---|---|---|
| [`proposal.md`](proposal.md) | Approved brief | Product decisions, defaults, boundaries, risks, and feasibility gates. |
| [`architecture.md`](architecture.md) | Baseline | Provider-neutral layers, responsibilities, and invariants. |
| [`roadmap.md`](roadmap.md) | Outcome scaffold | Acceptance-based M-0 through M-6 sequence without dates. |
| [`implementation-plan.md`](implementation-plan.md) | Planning template | Required structure for executable tasks, dependencies, and tests. |
| [`decisions/`](decisions/README.md) | ADR system | Material architecture and policy decisions. |
| [`research/`](research/README.md) | Evidence system | Primary-source and live-test evidence rules. |
| [`research/kaggle-feasibility.md`](research/kaggle-feasibility.md) | Not started | K-01 through K-16 evidence ledger. |
| [`providers/`](providers/README.md) | Provider docs | Adapter-specific capability and compatibility documentation. |
| [`development/commit-convention.md`](development/commit-convention.md) | Active policy | Commit subject and history rules. |
| [`development/repository-workflow.md`](development/repository-workflow.md) | Active policy | Branch, PR, validation, and direct-main rules. |

Additional API, job-contract, operations, testing, compatibility, troubleshooting, and security design documents should be added when implementation makes them concrete. Do not create empty documents merely to mirror a proposed tree.

## Status vocabulary

Use precise labels where relevant:

- `planned`: approved or proposed, but not implemented;
- `implemented-offline`: code exists and offline tests pass;
- `documented-upstream`: supported by a cited primary source but not live-tested here;
- `passed-live`: exercised with authorized credentials and recorded evidence;
- `not-tested`: no relevant verification has been completed;
- `unsupported`: evidence establishes the capability is unavailable for the described mode;
- `blocked-environment`: verification could not run because an external prerequisite was unavailable.

Avoid an unqualified “supported” when version, account, region, execution mode, or evidence level changes the conclusion.

## Writing rules

- Date mutable provider research and record tested client versions.
- Link requirements to components and acceptance tests.
- Keep examples synchronized with schemas and executable behavior.
- State unknowns and operational consequences explicitly.
- Never place credentials, sensitive account data, private inputs, or full provider responses in documentation.
- Prefer small focused documents and stable relative links.
