# ADR-0020: Explicit fixed GPU acceptance with separate-process recovery

Status: accepted. Requirements: PRD-04, VER-03; preserves DUR-03 and immutable results.

## Decision

Compose real Kaggle ports and durable engines for one fixed, repository-owned CUDA experiment,
not a general provider server. Freeze code/challenge/configuration locally; require explicit
private-staging/GPU approval for one finite attempt and read-only approval for observation/transfer.
Retain one exact executable across all modes. No user-source override, paid fallback or compute retry.

Write create-only submission-process evidence after intent commit but before returning the call
permit. Failure preserves intent but forbids mutation. Resume in a different process with the same
intent/plan, verify published original GPU arithmetic/hardware/files and obtain exact terminal
observation before writing resume evidence. Artifacts cannot replace missing process records.

## Reason and consequences

Device listing and a successful same-process test do not establish real computation or durable
handoff. The fixed run offers bounded reproducibility but not hostile-host attestation, every crash
window, remote exactly-once count or the full M1 timeout/fault/cleanup checklist. A scoped live pass
must still retain those unknowns. Local timeout does not cancel remote work; private resources can
remain because no cleanup apply is supplied. Preserve state, binary and original account.

Use the [runbook](../providers/kaggle-acceptance.md) for repeat qualification. Keep full operator
run records private and attach only minimal sanitized evidence to a focused issue/PR when support
claims change. Offline synthetic success is never live evidence.
