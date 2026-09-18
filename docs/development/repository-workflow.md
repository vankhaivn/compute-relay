# Repository workflow

## Start from the current task

Read [AGENTS.md](../../AGENTS.md), [current status](../status.md) and the guide for the task.
Consult the approved proposal, requirement IDs, relevant ADRs and dated provider evidence when
design or scope is involved. Routine usage and operator validation do not require reading
completed implementation audits.

Create a focused branch from current `main`; make coherent changes and add appropriate tests.
Use atomic [Conventional Commits](commit-convention.md), push reviewable checkpoints and open
a PR. Resolve findings without rewriting unrelated history, then stop for owner review/merge.
Direct `main` pushes or auto-merge require explicit task-specific authorization, not a permanent
exception inferred from a previous task.

Prefer short lowercase branch names under `feat/`, `fix/`, `docs/`, `research/` or `chore/`.

## Change isolation

Keep code and documentation in separate scoped commits. Do not bundle unrelated cleanup,
reformat entire files for a small change, or modify generated/vendor/lock files without the
source change that requires it. Preserve public behavior unless a deliberate contract change
is documented and tested. Never introduce optimistic provider stubs or weaken verification
to obtain a passing check.

## Evidence belongs with its purpose

A PR records exact checks/results, unrun checks and reasons, credential/provider effects,
evidence tier and remaining risks. Living usage guides describe current usage and constraints,
not commit inventories or completed CI/audit transcripts. Dated provider research preserves
provenance; it does not silently promote a source observation to live support.

For operator testing, follow [the validation checklist](validation-checklist.md) and use
[the bug template](bug-report-template.md) when a failure or changed support conclusion needs to
be shared. Work one observed stage at a time. Keep full run records, raw logs/state and secrets
private; attach only minimal sanitized evidence to a focused issue/PR. A validation-only run does
not authorize code fixes or new remote effects.

Preserve an unexpected failure, report its reproduction and uncertain effects, then track a
focused fix and append a retest. A merged fix without an observed retest remains
`fix-pending-verification`. Do not erase old failures or reset an uncertain acceptance root.

## Review and history safety

Prioritize product boundaries, duplicate-execution/recovery safety, credentials and workspace
isolation, archive/path/SSRF defenses, truthful state/capability semantics and maintainability.
Inspect the final diff for secrets and unrelated changes as well as checking tests.

Do not force-push shared branches, rewrite `main`, delete tags or remove another contributor's
work without explicit authorization. When a decision or provider fact changes, preserve its
context and record the new conclusion; do not rewrite historical evidence into a newer pass.
