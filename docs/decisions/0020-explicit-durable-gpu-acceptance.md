# ADR-0020: Explicit fixed GPU acceptance with durable restart evidence

- Status: accepted; PR #24 merged on 2026-09-16
- Date: 2026-09-16
- Task: M4-06 harness and integration; live acceptance remains blocked-environment
- Requirements: PRD-04, VER-03; preserves DUR-03, existing publication and authority boundaries
- Extends: ADR-0011/0013 and ADR-0015 through ADR-0019

## Context

M4-01 through M4-05 supply separately tested real Kaggle components but no executable experiment
joining them through durable admission, submission and result publication. A fake lifecycle pass
cannot establish live GPU use, private staging or restart recovery. The owner requested continued
implementation without supplying live credentials; proposal section 23.6 permits useful offline
composition and an explicit operator procedure, not invented live evidence.

A one-process smoke cannot demonstrate a process boundary. Rebuilding an executor after response
loss also cannot reset M3's new-intent mutation authority. The acceptance harness needs original
configuration/source identity, separate invocation permissions and conservative evidence records.

## Decision

Add a finite `cmd/kaggleacceptance` utility and one-job scoped AcceptanceAdapter, not production
`serve`, a new queue or public routes. Compose the existing preflight, Stager, Executor, Monitor,
ArtifactReader and M3 durable services. Delegate cancellation to the existing manual-only path;
return cleanup unavailable rather than manufacturing success or applying remote deletion.

The experimental descriptor expresses conditional implementation support needed to attempt the
fixed experiment. It does not upgrade normal provider/live capabilities. GPU stays unknown until
verified after execution, and an authenticated preflight diagnostic remains not-ready. Require the
original job/attempt/specification, account, configuration revision and input pins on every path.

Use two repository-owned Python sources for a 64-by-64 CUDA matrix multiplication. A new immutable
challenge selects a small exact scale; verify CUDA tensor placement, synchronization, every result
cell and the exact total. Record bounded result/hardware data with original identities and input
digest. Require the expected GPU shape; no CPU fallback, arbitrary workload, dependency installation
or remote internet. Freeze wall/setup/finalization budgets at 120/30/15 seconds without pretending
provider timeout enforcement or hardware release is measured by the local process.

Separate five command modes. Prepare and status use local state only. Submit requires both private-
staging and GPU authorization, checks conservative account quota and runs only one original attempt.
Resume and explicit collect require read-only provider authorization and cannot create preparation
or submission intents. Existing failed collect work needs a new explicit idempotency key, not a new
compute attempt. CLI rejects cross-mode configuration even when an irrelevant flag is set false.

Hash the actual executable and retain its digest with deterministic bundle/spec/input identities
in a new private experiment directory. Reopen only that original configuration and binary. Do not
accept a hash override, source override, overwrite/reset or automatic recovery through a newer
binary. Create-only bounded records fail closed on malformed text, missing fields or incomplete
writes/reads. Keep original database/blob/record state together; none is a complete backup alone.

Add `Store.InspectDispatch` as a current-operate-authorized internal journal read. Verify active
attempt, installation, frozen plan and independent ownership rows without returning any claim,
fence or new mutation permit. The private journal is not a public serializer. The existing SQL
mutation/state machines and migration bytes remain unchanged.

After a successful NEW M3 submission-intent commit, persist and flush the submitting process
nonce **before** returning the mutation permit. A record failure then leaves the intent committed
but invokes no helper. Do not erase the intent or overwrite a record to obtain another permit.
The submit invocation exits at the submission boundary. Resume must execute in a different process,
using the same plan/intent, and records a separate successful exact-terminal read after publication.

Qualification independently rehashes all six published control/result files and validates the
challenge, arithmetic, actual-version strings, GPU shape and consistent process records. Original
operation receipts and execution/result/release dimensions stay distinct. A successful local read,
terminal kernel, payload prefix or complete artifact set without restart evidence is not a pass.

Test-only composition and records carry fixture provenance. The public path refuses fixture
records; fixture success is passed-offline and cannot yield a successful live CLI qualification.
Even an operator-run passed-live report is scoped to this fixed experiment: full_m1_acceptance
remains false, actual remote execution count not observable, timeout enforcement unverified and
cancellation manual-required. Raw provider exceptions, credential references and private account
names do not enter the report. Opaque IDs/digests still require review before external sharing.

## Alternatives rejected

- Treat CI or fake GPU tensors as live acceptance: misstates actual hardware/provider evidence.
- Execute an arbitrary job or use CPU fallback: defeats the fixed bounded GPU acceptance goal.
- Run submission and qualification in one process: does not establish the intended restart boundary.
- Let a repeated submit or missing resource reset an intent: risks duplicate execution.
- Rebuild silently and regenerate different source for recovery: conflicts with original identity.
- Infer full M1 go from arithmetic and artifacts: omits timeout/cancellation/fault/cleanup evidence.
- Delete test resources automatically: changes authorization and can destroy unresolved recovery data.
- Introduce a second durable orchestrator: duplicates existing tested authority and transactions.

## Consequences and limits

The host operator, executable, environment and state are trusted. Process nonces and digests are
consistency checks, not cryptographic hardware/session attestation or protection from malicious
operator edits. Same-version rerun, SaveKernel upsert races, dataset mount and cross-version recovery
limits remain inherited from the components. A free-quota observation is not a reservation.

A corrupt/missing process marker can block automatic resume even when no request reached Kaggle;
manual investigation is safer than rearming a mutation. Local invocation limits and signal handling
do not cancel remote work. Provider resources and partial upload side effects may remain; no cleanup
or reclamation guarantee is added. The operator must preserve recovery state while resolving them.

Local status is provider-read-free, not a bit-for-bit read-only filesystem operation: opening state
uses existing initialization/migration policy and ephemeral local token issue/revoke. A historical
retained pass is not a new provider observation. Missing preinstalled PyTorch/GPU or any inconsistent
result fails without installation, account fallback or fabricated success.

## Verification

Real SQLite/admission/blob/collection integration uses a synthetic backend and injected process
identities to test one-attempt submission/lost-response/reopen/recovery, strict current authority,
late transfer retries and marker faults. Separate actual child processes verify nonce freshness and
execute the real CLI's local prepare/status across durable reopen. These are not a live GPU restart
or new process-kill experiment at every boundary. Existing M3 fault/kill/race suites remain intact.

Python calculation tests inject synthetic tensors without importing torch; negative Go tests reject
wrong identities, hardware, arithmetic and missing/malformed records. CLI tests enforce permissions,
configuration and binary identity before effects. Exact-head CI and narrow local checks are recorded
in PR #24. No live credential, Kaggle resource, GPU, deployment or new workflow was used.

The [acceptance guide](../providers/kaggle-acceptance.md) contains operator commands, sources,
report interpretation, limits and the explicit not-run live ledger. Stop after this PR for owner
review/merge. Harness delivery and live M4-06/M1 acceptance remain separate; M5 has not started.
