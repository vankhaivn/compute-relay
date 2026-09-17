# Repository instructions

These instructions apply to humans and coding agents throughout the repository. Follow the
owner's current task first, then applicable repository instructions, governance and security.
Do not change approved product scope as a routine implementation preference.

## Start with the task, not the development history

Read [current status](docs/status.md) and the relevant guide from [docs](docs/README.md).
For a substantial design change, consult the [approved proposal](docs/proposal.md),
[requirement IDs](docs/scope-and-requirements.md), applicable ADRs and source/tests.
The proposal defines intended scope; current guides/code define available commands.
Do not make a new user read all completed PR audits before using the repository.

For operator verification, start with [the validation checklist](docs/development/validation-checklist.md)
and existing [results/bugs](docs/development/validation-results.md). Fill non-secret values,
run one stage at a time, inspect the actual result and record it before proceeding. A missing
implementation, unsupported capability or skipped test is not a pass. Preserve failures and
append retests; hand off reproducible bugs before changing code during validation-only work.

## Product and safety invariants

- OSS, self-hosted finite batch runtime; operator-owned accounts, no hosted quota pool or
  mandatory paid service. Go control plane, HTTP/JSON boundary, Docker optional.
- Provider-neutral public/domain contracts. Keep Kaggle-specific behavior inside its adapter.
  Workspaces separate applications of one trusted operator, not hostile SaaS tenants.
- Persist original jobs, attempts, inputs, receipts, intents and exact provider identity.
  Never automatically repeat ambiguous submission, change providers/accounts, fall back to
  CPU or enable billed capacity. Read-only recovery is not a new mutation permit.
- Unknown stays unknown. Cancellation intent, local timeout, cleanup, payload completion,
  artifact availability and hardware release are different facts.
- Keep credentials out of application jobs, chat, logs, fixtures and Git. Real credentials
  belong only in the operator's explicitly configured environment.
- No compute, quota consumption, public data, destructive cleanup, billing or deployment
  without exact authorization and a finite budget. CI is offline, never an operator runtime.
- Preserve state and original binaries/configuration after uncertainty; do not reset or
  delete evidence to manufacture a successful retry.

## Documentation rules

Write usage in usage guides, current constraints in component references and remaining work
in the implementation plan. **Remove completed development narrative rather than appending
another status update.** Do not put owner/agent prompts, audit trails, commit inventories,
local sandbox stories, CI transcripts or repeated merge handoffs in living docs or changelog.
Keep PR-specific checks in the PR. Keep current operator runs and reproducible failures in
validation results. Preserve stable requirement IDs, essential safety semantics, exact test
traceability and dated primary-source evidence; brevity is not permission to invent support.

The [proposal](docs/proposal.md) and requirement definitions are the approved product record.
Do not silently rewrite them or the numbered fault scenarios to satisfy a test. ADRs should
state decisions, reasons and consequences; completed implementation/CI diaries are not ADRs.
The changelog describes a few user-visible unreleased features until an actual release exists.

## Git and validation

Use a focused branch/PR. No direct `main` push, auto-merge, force push or shared-history rewrite
without task-specific authorization. Keep commits atomic and coherent: each commit must represent
a complete logical change or milestone. Do not bunch unrelated subsystems into a monolithic commit,
but strictly avoid artificial micro-commit spam (e.g. splitting a single fix or feature across
multiple single-file commits by artificially separating code and its direct unit tests). A logical
fix or capability and its direct regression tests belong together in one self-contained commit.
Keep large documentation updates and code in separate scoped commits when appropriate, but do
not dilute history with redundant micro-commits. Push reviewable checkpoints and stop for owner
merge. Follow the [commit convention](docs/development/commit-convention.md); no AI/model/agent
authors or co-authors.

Before reporting completion, run relevant format/tests/contracts/link checks that are available,
inspect the final diff for secrets and unrelated changes, and report exact commands and limits.
Distinguish offline component tests, current-host checks and live-provider evidence. Never lower
a dependency/toolchain pin, substitute a driver or weaken an assertion to claim a pass.
If a prerequisite is unavailable, record the exact block and continue independent safe work.
