# Reproducible validation bug

Copy the section below into [validation results](validation-results.md) or a linked reviewed
issue. A validation bug is an unexpected result of implemented behavior. Missing planned code
belongs in the implementation blocker register instead. Report vulnerabilities privately through
[SECURITY.md](../../SECURITY.md), not a public issue containing exploitation details or secrets.

## BUG-RUN-YYYYMMDD-NN-01 — short factual title

- **Status:** open / investigating / fix-pending-verification / verified-fixed.
- **Check / run:** Vxx / RUN-YYYYMMDD-NN.
- **Source SHA and dirty patch:** exact commit; explicitly state whether code was modified.
- **Executable SHA-256:** especially the original acceptance binary; retain it unchanged.
- **Environment:** OS/architecture/filesystem and actual Go/Python/uv/client/SDK versions.
- **Configuration:** minimal sanitized shape, relevant limits, account alias and shape; actual
  secrets, paths and complete configuration remain private.
- **Workload/input:** minimal sanitized job specification and synthetic reproduction fixture,
  or the fixed acceptance case ID. Record original bundle/input/pin hashes; do not attach private
  bundles or replace frozen inputs just to reproduce the error.
- **Authorization:** exact approved reads/mutations/budget; state which effects were attempted.

### Reproduction

1. Necessary starting state and prerequisite check IDs. Distinguish fresh state from resumed
   state; do not instruct another agent to delete or reinitialize uncertain remote work.
2. Exact command with literal flags and private path/ID aliases. Token values never appear.
3. Actual UTC time, exit code and decisive sanitized output. Include HTTP status, stable problem
   code and request ID when present, or explicitly state that no response was received. Record
   the first failure, not only the last retry, and reproduction count without rerunning unsafe
   effects to increase it.

**Expected:** One concrete observable result from the current contract.

**Actual:** What happened instead; separate observation from suspected cause.

### Durable and remote effects

| Question | Observed answer / evidence |
|---|---|
| Was a local request possibly committed? | confirmed / possible / not attempted / unknown |
| Was staging or submission intent committed? | Evidence alias; do not dump SQL/database. |
| Could compute have started or still be running? | confirmed / possible / unknown; do not infer from timeout. |
| Original job/attempt/resource/pin preserved? | Sanitized equality result or alias, not private account URL. |
| Process markers / original binary available? | yes/no/unknown with private evidence alias. |
| Could a destination file already be published? | Include `download_may_be_published` when reported. |
| What must not be retried or deleted? | Exact operation/state boundary. |

### Evidence and safe next action

List minimal sanitized excerpts and SHA-256 of retained private evidence. Redact tokens,
Authorization/Cookie, signed URL query strings, account names, secret references when sensitive,
private inputs and raw workload/provider logs. Do not upload a full state or acceptance archive.

Record a safe read/reproduction suggestion only when justified by observed state. Do not propose
a new submission, root reset, CPU/provider fallback or deletion to make the test green.

### Fix and retest

- Root cause after investigation: UNSET (hypotheses remain explicitly hypotheses).
- Fix PR / commit and regression test: UNSET.
- Retest run / binary SHA / exact repeated check: UNSET.
- Expected failure protections still intact: UNSET.
- Remaining remote/local effects: UNSET.
- Reviewer / final disposition / UTC date: UNSET.

A merged fix without a successful observed retest stays `fix-pending-verification`.
