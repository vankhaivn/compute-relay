# Recovery and operational state semantics

> **Task:** M3-08, PR #18 in review. This guide describes the implemented offline services
> through M3-07 and their fault qualification. It does not invent production commands.

## Read separate facts, not one success flag

A job is an immutable request with frozen input/provider resolution. An attempt is one
explicit compute execution. An operation is a durable control request with its own history.
The current active attempt can differ from the attempt named by an old operation or result.
Always keep the explicit workspace, job and attempt identities together.

| Dimension | What it establishes | What it does not establish |
|---|---|---|
| Orchestration | What the local runtime is doing: queued, preparing, reconciling, collecting, needs-attention or terminal. | An HTTP receipt alone does not prove remote acceptance, payload success or verified output. |
| Execution | Accepted provider/runner evidence for the particular attempt. Unknown remains unknown. | A local timeout, process exit, missing resource or failed poll is not proof of remote rejection/termination. |
| Result | Not available, collecting, incomplete, invalid, verified available or expired. | Successful execution is not available results; verified failure diagnostics can exist without job success. |
| Cancellation | Intent, acknowledgement, prevention, confirmation, too-late or manual-required outcome. | Accepted cancellation does not prove termination. Local shutdown and resource deletion are not cancellation. |
| Remote activity / release | Conservative possible activity and separately observed resource-release precision. | Expired local leases, elapsed time, downloaded files or cleanup previews cannot manufacture release evidence. |

`needs_attention` is not a successful or failed remote execution guess. It preserves a
condition that requires inspection or explicit recovery. Do not reset it to queued by
editing SQL, removing a submission intent or clearing an ownership barrier.

## Receipt, operation and attempt reads

A successful job/control POST acknowledges committed local metadata. Retain the exact
`Idempotency-Key` and body when the response is lost. Repeating them recovers the original
receipt and identifiers under current authorization; it does not ask for fresh compute.
Changing the body under the same key conflicts.

POST replay returns the **original** acceptance receipt. GET on the returned operation
location reads the **current** operation revision. Current job GET reads the active attempt
without provider polling. Historical operation outcomes remain facts even after later
execution, collection or expiry events. For example, a succeeded collect operation does not
mean its bytes are still retained after the attempt's result becomes expired.

Every control body names `attempt_id`; retry additionally requires a nonblank non-secret
reason. Retry returns a distinct new attempt ID. An operation's success means its defined
control effect succeeded, not that the new attempt's remote workload succeeded. Review
[the controls guide](operations.md) for exact HTTP/request/schema behavior.

There are fifteen composable HTTP handlers. A route is unavailable unless its application
service is composed; there is no in-memory production fallback. Artifact/event/attempt
listings and production CLI/configuration remain separate work. Use the internal APIs only
from a trusted, deliberately composed runtime, not an imagined `compute-relay recover`
command or direct database mutation.

## Recovery by durable boundary

| Last durable evidence | Permitted recovery | Unsafe inference to avoid |
|---|---|---|
| No committed admission receipt | Replay the same key/request under current authority. | A lost response does not mean admission failed. A new key may create different work. |
| Inputs accepted but not prepared | Verify bundle/local bytes and freeze pending HTTPS roles before first staging intent. | Admission is not proof that bytes, staging privacy or provider capabilities are ready. |
| A frozen input role already exists | Use its original digest/bytes. Refuse missing, changed or expired inputs. | Do not refresh a mutable URL or select a new profile to make the old job work. |
| Preparation intent committed, acknowledgement uncertain | Observe the prewritten staging identity; preserve uncertainty when not conclusive. | Do not repeat Prepare or select public staging as a fallback. |
| Submission intent committed, acceptance unknown | Reconcile/observe the exact persisted identity under its frozen binding. | Not-found, timeout, CLI nonzero exit or local restart never rearm Submit. |
| Accepted or active attempt | Observe the same reference/version; reject stale or inconsistent observations. | Do not create another attempt to compensate for an unavailable poll. |
| Terminal execution, results not available | Collect the matching result manifest and selected files, then verify and atomically publish. | Provider wrapper success or a few downloaded files do not establish complete results. |
| Collection snapshot committed, accepted operation interrupted | Reclaim only under current fenced ownership, reuse that pin and rehash complete local files. | Do not re-list a newer run or overwrite the pinned snapshot. |
| Collection operation durably failed | Inspect its condition; a new explicit collect request/key can retry transfer for the same attempt. | Replaying the old key is receipt recovery, not a new ticket. Missing artifacts do not justify compute retry. |
| Complete blobs, uncertain publication acknowledgement | Reload metadata and the durable pin; reuse verified bytes under the collection fence. | Do not fabricate a failed operation, duplicate events or delete completed recovery files. |
| Result/input expiry committed | Respect the tombstone and historical metadata. Local sweep may remove only exact bound-store targets. | Physically remaining bytes are not reusable inputs after expiry; a hold cannot resurrect expired data. |
| Local deletion acknowledgement lost | Recover from the committed tombstone and exact store identity; tolerate owned absence. | Do not broaden the path, swap stores or erase metadata to force success. |
| Remote cleanup preview says would-delete/already-absent | Treat it as a read-only observation under the exact ownership ledger. | It is not a reusable apply permit or proof that this runtime deleted anything. |

The [fault matrix](fault-matrix.md) maps these boundaries to executed regressions. Real
process kills, injected I/O/response faults and synthetic provider evidence are distinguished
there; none of them establishes live Kaggle behavior.

## Cancellation, deadlines and capacity

Before submission intent, cancellation can prevent dispatch. That records prevention, not
remote termination; previously staged resources may still require ownership-safe handling.
After submission may have happened, a newly committed one-shot cancellation intent can
permit one supported, identity-bound call. Lost acknowledgement does not permit repeating
that call. Unsupported capability or missing remote identity remains manual/unresolved.

Only matching terminal evidence can confirm cancellation. Completion may win the race and
remain completion with cancellation too late. A manual-required terminal operation is not
rewritten later merely because an observation eventually resolves the attempt.

Local request, provider-call, preparation, observation, transfer and lease budgets have
different meanings. An expired local observation invocation does not mark execution timed
out or release possible-activity capacity. The matrix also tests a persisted deadline flag
with unknown activity, but does not introduce a production overall-job deadline service.
Remote wall/setup/finalization limits remain the frozen runner/adapter contract.

A fixed worker pool waits for cooperative callbacks rather than replacing stuck work with
unbounded goroutines. Lease expiry can fence a stale publisher; it does not prove that the
old callback stopped or remote compute ended. Retention keeps unresolved held ownership
pinned instead of using age to delete recovery material. Do not manually release an account
slot while activity is possible/unknown.

Quota values carry status, units, observation time and precision. Missing or stale readings
are not unlimited allowance. Strict/default policy decisions and exhaustion latches remain
explicit. A positive observation is not a reservation: provider rejection after external
consumption must still be handled without automatic submission retries.

## Binding and credentials

Recovery resolves the exact accepted configuration revision/account, not the current profile
alias. Credential values may rotate behind that binding only when the adapter verifies the
same effective account. A different account or changed resource/version fails closed; it
cannot become a hidden fallback or a new identity for the old attempt.

The local qualification uses synthetic account checks, not real credential rotation. Live
preflight, official-client compatibility and provider observation semantics require separate
authorized evidence. Never paste credentials, private job bytes or raw provider responses
into chat, issues, tests or recovery notes.

## Retention and restore

Retention defaults are minimum eligibility windows, not guaranteed deletion times. All
historical/shared references, named holds, pending operations and unresolved recovery needs
are checked. Result expiry preserves the business outcome and commits a sequenced
`result.expired` event with the complete result-set tombstones. Original receipts, metadata,
publications and ownership history are retained; there is no metadata pruner in this phase.

Input and result blobs use distinct private stores with persistent `.retention-id` bindings.
Keep those identities with a whole-store backup or move. A replacement root must not inherit
an old database's deletion tickets. Do not remove markers or rewrite bindings to bypass a
mismatch. Unknown files, wrong bytes or conflicting copies are evidence to preserve.

A database-only backup is not an installation backup. Quiesce writers/retention for a
coordinated checkpoint and separately preserve input/result bytes and unpublished recovery
material. Restore into a new state directory, inspect identity/schema/authority and verify
matching stores before enabling work. Old snapshots may lack later receipts, revocations,
expiry facts or deletions; missing local metadata is not proof that remote work never started.
Never activate original and restored installations concurrently against the same provider
identity. Restoring metadata does not stop remote compute or restore deleted bytes.

See [storage](storage.md) and [retention](retention.md) for exact composition APIs, integrity
checks and platform limitations. Remote cleanup remains dry-run-only; staging preview and
remote apply are separate gates. Local unlink is not secure physical erasure and cannot
revoke bytes already delivered to a reader.

## Evidence and next gate

For a recovery report, record the immutable job/attempt/operation identities, current state
and revisions, sanitized condition code/stage, relevant intent/pin/publication/expiry facts,
exact binary/schema version and the check that supports the conclusion. Keep secret values,
input/output bytes, account dumps and arbitrary raw diagnostics out of the report.

M3-08 qualifies the implemented offline services and documents their limits. The existing
quality/native/race CI remains offline; exact final-head results are recorded in PR #18.
Merge closes this task only after owner review. M1 live acceptance, the M4 go decision,
production composition and live cleanup retain independent authorization/evidence gates.
Do not begin them automatically when this PR becomes green.
