# Repository instructions

These instructions apply to the entire repository. They are mandatory for human contributors and automated coding agents.

## Instruction priority

Follow, in order:

1. an explicit instruction from the project owner for the current task;
2. this file and any more specific `AGENTS.md` in a modified subdirectory;
3. repository governance, contribution, security, and development documentation; and
4. general tool or language conventions.

Do not silently override an owner-approved product requirement with a routine implementation preference.

## Required reading before changes

Before planning or editing substantial work:

1. read [`docs/proposal.md`](docs/proposal.md) completely;
2. inspect the current repository, relevant history, and applicable instructions;
3. identify whether each relied-on statement is an owner-approved requirement, implementation default, provider fact, or verification gate; and
4. inspect relevant ADRs, research notes, tests, and provider evidence.

Do not restart product discovery or ask the owner to choose between routine reversible technical alternatives already covered by the proposal. Choose the simplest defensible option, record material decisions, and continue.

## Product invariants

Preserve these boundaries unless the owner explicitly changes the approved scope:

- The project is an OSS, self-hosted runtime; there is no maintainer-hosted control plane or shared quota pool.
- The runtime is a sidecar process with a language-neutral HTTP/JSON application boundary.
- Go is the control-plane implementation language; direct execution is primary and Docker is optional.
- The domain and public job contract remain provider-neutral. Kaggle-specific types and behavior stay inside its adapter.
- MVP is finite batch execution for remote Python and explicit Linux shell commands.
- SQLite and local filesystem storage are the initial persistence model; no mandatory broker, cloud database, tunnel, or paid service.
- Jobs, attempts, inputs, submission intent, and provider identity must survive restart.
- Unknown provider or execution state must remain representable as unknown.
- Never automatically repeat ambiguous compute submission, silently switch providers, silently fall back to CPU, or silently enable billed capacity.
- Cancellation intent is not proof of remote termination. Cleanup is not cancellation.
- Applications do not receive provider credentials and do not need Kaggle-specific integration.
- Workspaces separate applications for one trusted operator; they are not a hostile multi-tenant security boundary.
- Live provider behavior is claimed only after dated, versioned, sanitized evidence exists.

## Evidence and documentation rules

- Treat [`docs/proposal.md`](docs/proposal.md) as the approved brief, not as proof that software or provider capabilities exist.
- Keep support status explicit: `planned`, `implemented-offline`, `documented-upstream`, `passed-live`, `not-tested`, `unsupported`, or `blocked-environment` as appropriate.
- Provider facts must cite current primary sources and record the checked date and client/version context.
- Live tests must use authorized credentials, explicit opt-in, and a finite budget. Never request secrets through chat or commit them.
- Update architecture, ADRs, research notes, operations documentation, and examples when behavior or public contracts change.
- Do not create empty package trees merely to mirror the proposal. Add structure when it protects a real boundary.

## Git workflow

- The normal workflow is a focused branch and pull request. Push directly to `main` only when the owner explicitly authorizes it for the current task.
- Keep commits atomic, reviewable, and scoped to one coherent change.
- Follow [`docs/development/commit-convention.md`](docs/development/commit-convention.md). CI validates commit subjects.
- Do not add an AI system, coding agent, or model as an author or co-author. The authenticated human or service identity remains the commit author.
- Do not rewrite shared history, force-push, delete tags, or remove unrelated work unless explicitly instructed.
- Avoid drive-by formatting or unrelated dependency changes.

## Change quality

Before reporting completion:

- run the narrowest relevant formatter, linter, tests, schema validation, and documentation checks available;
- state exactly what was tested and what was not;
- distinguish offline tests from live-provider evidence;
- verify no credentials, tokens, private account data, runtime databases, generated artifacts, or model weights were added;
- confirm public examples match implemented behavior; and
- keep error and status language operationally honest.

A task is not complete merely because code compiles. It must preserve the relevant invariants, include tests for failure semantics, and update durable project knowledge where needed.

## External side effects

Without explicit authorization, do not:

- allocate provider compute or spend quota;
- enable paid services or billing;
- publish private data or provider resources;
- create, rotate, or expose credentials;
- delete remote resources;
- deploy a public service; or
- push to a repository or branch other than the requested target.

When an external prerequisite is unavailable, mark the precise block and continue all safe offline work.

## Operator runtime and CI boundary

- Operators configure credentials in their own runtime environments. Do not centralize
  users' credentials or runtime operation in maintainer-controlled GitHub Actions.
- GitHub Actions are for offline format/lint/compile/unit/component/contract/security checks,
  not deployment, real Kaggle executions, GPU allocation or secret-backed live probes.
- Build and run executable local smoke checks in the available Linux runtime whenever
  possible. Report toolchain or sandbox limitations precisely; do not silently weaken the
  committed toolchain or substitute CI-only execution for feasible local checks.
- Keep live probes as explicit opt-in operator-run commands with separately authorized
  credentials and finite budgets. Do not ask for credentials in chat.
