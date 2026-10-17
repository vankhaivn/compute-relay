# Safe recovery

Preserve the original receipt, request/key, attempt, state directory, provider binding and
compatible executable before diagnosing failure. Do not delete intent rows, reset roots, clear
capacity or submit a replacement merely because a response is missing.

## Read the right fact

Operation acceptance, current operation state, remote execution, artifact availability and
hardware release are independent. A 202 is a local receipt. Provider terminal status opens
collection, not verified result availability. Cancellation intent is not termination. Local
shutdown/timeout is not remote cancellation. The latest raw unknown can coexist with stronger
confirmed running/activity evidence.

| Observed boundary | Safe action |
|---|---|
| Lost admission/control response | Explicitly replay the identical request and original key under current authority; then read current state. Do not generate a new key automatically. |
| Failed before an intent committed | No external permit exists; inspect the actual transaction outcome before retrying local work. |
| Intent committed, acknowledgement lost or external call uncertain | Reload the journal and observe original identity only, even if a lookup returns not found. |
| Staging pending or exhausted observation failures before submission | Explicitly reconcile the original attempt. This resumes staging observation with its original preparation ID and frozen plan, without another upload/create call or attempt grant. Privacy or identity mismatch still requires intervention. |
| Submission rejected before any provider request | No compute was requested. Read `problem.details.reason` to tell configuration, staging and transient stops apart (see [Kaggle](kaggle-runtime.md)); fix the cause, then retry compute explicitly. |
| Remote active/unknown | Retain account capacity and recovery material; inspect/reconcile the recorded resource. |
| Cancellation unsupported or target missing | Keep manual-required/unknown termination. Never delete resources or substitute IDs to pretend cancellation. |
| Manual terminal job, `collection.state=awaiting_request` | Wait until results are wanted, then collect explicitly for the original attempt. Polling/reopening is read-only. |
| Terminal execution, results missing | Collect for the same attempt, not compute retry. |
| Interrupted accepted collection | Reclaim after the transfer lease under normal fencing; reuse the original result pin. |
| Committed failed collection | One explicit new collect request/key for the same attempt; old-key replay remains the old receipt. |
| Complete bytes but failed final transfer acknowledgement | Keep destination unpublished; complete bytes alone are not success. |
| Publication acknowledgement lost | Read committed result state; do not duplicate publication/events. |
| Download failed after local file publication | Inspect the chosen destination and original request state; never overwrite/delete it automatically. |
| Result expired | Preserve history. Do not refetch inputs or rerun compute to resurrect expired results. |
| Identity/source/version/root mismatch | Stop and retain evidence; do not adopt a replacement or rewrite markers to bypass the check. |

Staging readiness can require verification of every original input byte; Kaggle checks metadata
and leaves the byte check to its remote runner. Staging observation
and submission preparation use the finite preparation budget, constrained by any shorter parent
deadline. Short provider control calls retain their separate control timeout; a staging timeout
does not authorize another upload, creation or submission.

Read controls and receipts over the [HTTP API](../api/README.md).
[Artifact delivery](artifact-delivery.md) explains collection and publication boundaries.

Source/version/nonce checks reduce accidental mixups, not malicious-host attestation or proof that
a provider never reran the same version. Upstream upsert and mount limits remain. Do not infer an
exact remote execution count, provider timeout enforcement or hardware release without evidence.

## Backup and incident handling

Preserve SQLite with its committed WAL state, both blob roots and all identity/process markers.
For managed file credentials, also preserve the private `credentials/` directory and
`state/credential-store`; Keychain-backed installations need their original Keychain entries.
A database-only backup does not restore deleted bytes or stop remote work. Never activate original
and restored copies concurrently against one provider identity. Review restored grants/revocations
before exposing work. Use the coordinated procedure below rather than ad-hoc database/file copies.

For an unexpected failure, retain raw state/logs privately and open a focused issue/PR with only
the binary/source revision, exact command, actual result, possible committed/remote effects and
minimal sanitized reproduction. A merged fix still needs an observed retest.

## Storage and coordinated backup

SQLite uses WAL/FULL, foreign keys, short immediate transactions and one private connection.
Jobs, attempts, grants, receipts, resource journals, publications and tombstones commit at local
boundaries; no transaction spans provider or blob I/O. Store-owned time supplies local transaction
timestamps, with a persisted nondecreasing floor across clock rollback and restart;
provider/progress observation times remain separate evidence. Busy/timeout errors do
not grant blind transaction replay. Root/database/sidecar symlinks and unsafe types/permissions
are rejected. Shared/network filesystems and arbitrary power-loss guarantees are unqualified.

Relay has no backup/restore CLI or store backup API. Preserve a whole stopped installation
using an operator-managed filesystem backup; a live database-file copy can omit committed WAL
state and is not a coordinated checkpoint.

1. Stop Relay and wait for its handlers/workers to finish and the installation lock to release.
   Keep the original installation intact before copying anything.
2. Copy the complete private runtime root, including SQLite and any sidecars, both blob stores
   and unpublished recovery files, `.compute-relay-runtime`, root format/retention identities,
   `state/credential-store` and the file vault when selected. Keychain entries remain separate
   and must retain their original native identity. Keep the compatible executable.
3. Recover only with all matching components in a private destination. Preserve original marker
   bytes and lock files; do not initialize over the copy, rewrite identities or use SQL to force
   an open. Validate through the normal host open before serving.
4. Review restored tokens/grants/expiry against newer revocations. Never run recovered and original
   copies concurrently against the same provider identity. A filesystem backup neither stops remote
   compute nor restores bytes deleted before it was taken.

The hosted installation uses one top-level `runtime.lock`, the `.compute-relay-runtime` marker
and the database installation row. New roots do not create `state/.installation` or child locks.
For older roots, the host validates an existing `state/.installation` before removing that redundant
marker, and only after the entire installation validates. Existing lock files are never unlinked;
blob format markers and separate input/result `.retention-id` bindings remain authoritative.

## Local retention

Default minimum windows are 24 hours for unreferenced inputs and seven days for safe completed
references/verified results. These are eligibility windows, not deletion deadlines. Active/unknown
execution, unresolved preparation/submission, incomplete collection, worker ownership, pending
operations and shared references override age. Terminal execution without verified
collection remains recovery material.

Expiry commits an irreversible inventory tombstone. Results expire as a complete publication with
one sequenced event; receipts, metadata, execution outcome and release evidence remain. Repeated
scans do not duplicate expiry. New reads fail after expiry (HTTP 410 under current authority), but
already delivered bytes cannot be recalled. Old receipt replay does not resurrect bytes.

The managed sweep runs at startup and hourly. It deletes only exact committed tombstones after
checking the original root, workspace/object identity, size and hash. Input/result roots have
separate persistent `.retention-id` bindings. Failed/unknown expiry permits no deletion in that
invocation. Unexpected bytes/children preserve evidence; lost deletion acknowledgement recovers
from the tombstone. No prefix deletion, manual SQL cleanup or automatic root rebinding is allowed.
Local retention never authorizes [remote cleanup](remote-cleanup.md).

## Sequential upgrades

Applied migration version/name/bytes are checksummed. Add numbered migrations; do not squash or
edit applied ones. Newer, foreign, tampered or gapped state fails closed; downgrade is unsupported.
Supported v14 and v22 roots upgrade sequentially while retaining historical jobs, attempts,
receipts, connections and verified artifacts. Retain a stopped complete backup before upgrading.

Migration 0024 retires the removed explicit composition. Untouched queued work with no remote
intent/barrier becomes locally failed; other unfinished work becomes `needs_attention`. Original
remote identities and evidence remain intact. Pending controls/cleanup fail locally; pending
resource deletions are retained and started deletions become unknown. Existing remote activity is
never automatically cancelled, submitted, retried or deleted during migration. Inspect and resolve
the original evidence explicitly; creating a managed connection does not adopt old remote work.

Receipt, grant, lease and cleanup migrations preserve original identities, request hashes, receipt
bytes and released generations. Historical cleanup requests remain data for reading/recovery;
old request fields do not grant new deletion authority. Current cleanup uses the explicit operation
request and exact original-target checks described in [remote cleanup](remote-cleanup.md).
