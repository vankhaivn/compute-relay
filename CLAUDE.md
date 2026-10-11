# Repository instructions

These instructions apply to humans and coding agents throughout the repository; `AGENTS.md`
points other agents here. Follow the owner's current task first, then these instructions,
governance and security. Do not change approved product scope as a routine implementation
preference.

## Start with the task, not the development history

Read [current status](docs/status.md) and the relevant guide from [docs](docs/README.md).
For a substantial design change, consult the [approved proposal](docs/development/proposal.md),
[requirement IDs](docs/development/scope-and-requirements.md), applicable ADRs and source/tests.
The proposal defines intended scope; current guides/code define available commands. Ordinary
usage and validation do not require reading completed development history.

## Operator validation

Start with [the validation checklist](docs/development/validation-checklist.md) and any focused
issue/PR evidence linked by the task. Keep raw run evidence outside the repository; put only
minimal sanitized reproductions and changed support conclusions in focused issues/PRs. Run one
stage at a time and inspect the actual result before proceeding. A missing implementation,
unsupported capability or skipped test is not a pass. Preserve failures, original binaries and
uncertain remote state. A validation request does not authorize every live effect; hand off
reproducible bugs before changing code during validation-only work.

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
  belong only in the operator's explicitly configured environment or Relay-owned protected vault;
  managed write-only administration follows ADR-0025 and never persists secret values in SQLite.
- No compute, quota consumption, public data, destructive cleanup, billing or deployment
  without exact authorization and a finite budget. CI is offline, never an operator runtime.
- Preserve state and original binaries/configuration after uncertainty; do not reset or
  delete evidence to manufacture a successful retry.

## Documentation rules

Write usage in usage guides, current constraints in component references and remaining work
in the implementation plan. **Remove completed development narrative rather than appending
another status update.** Do not put owner/agent prompts, audit trails, commit inventories,
local sandbox stories, CI transcripts or repeated merge handoffs in living docs or changelog.
Keep PR-specific checks and sanitized operator evidence in the relevant PR or issue; do not keep
a growing run ledger on main. Preserve stable requirement IDs, essential safety semantics, exact
test traceability and dated primary-source evidence; brevity is not permission to invent support.

The [proposal](docs/development/proposal.md) and requirement definitions are the approved product
record. Do not silently rewrite them or the numbered fault scenarios to satisfy a test. ADRs
should state decisions, reasons and consequences; completed implementation/CI diaries are not
ADRs. The changelog describes a few user-visible unreleased features until an actual release
exists.

## Git and validation

Follow the workflow in [CONTRIBUTING.md](CONTRIBUTING.md) and the
[commit convention](docs/development/commit-convention.md). No direct `main` push, auto-merge,
force push or shared-history rewrite without task-specific authorization, and no AI/model/agent
authors or co-authors. Each commit is one complete logical change: a fix or capability with its
direct regression tests, not a monolithic bundle of subsystems and not artificial micro-commits.

Conventional Commit subjects apply to commits and PR titles, not issues. An issue title is a plain
sentence without a type prefix or labels: a bug states the observed symptom, a feature starts with
an imperative verb. The body states the problem with its concrete mechanism or measurement, then
the proposed change, then `Acceptance:` with bullets for observable behavior and required checks,
and optionally a closing paragraph of boundaries. Use headings only for large multi-part issues.

Before reporting completion, run relevant format/tests/contracts/link checks that are available,
inspect the final diff for secrets and unrelated changes, and report exact commands and limits.
Distinguish offline component tests, current-host checks and live-provider evidence. Never lower
a dependency/toolchain pin, substitute a driver or weaken an assertion to claim a pass.
If a prerequisite is unavailable, record the exact block and continue independent safe work.
