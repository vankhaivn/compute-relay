# Remote cleanup

Cleanup removes remote data recorded for one settled attempt. It never cancels compute, releases
hardware, changes quota facts or expires verified local artifacts. Nothing is deleted by age,
name prefix or a preview. Local files follow [local retention](recovery.md#local-retention).

## Inspect the candidates

Check `/v1/info` for `job_cleanup_preview`, then use an `operate` token:

```http
GET /v1/workspaces/{w}/jobs/{job_id}/cleanup?attempt_id={attempt_id}
```

The response lists at most two ledger resources, execution first and staging second. Each entry
has its original resource ID, purpose, target fingerprint, a typed target description when the
provider can interpret it, and an eligibility/refusal reason. Target fields identify the object,
version and deletion scope. `bytes: null` means size is unknown. Opaque provider receipts,
credential references, raw errors and host paths are excluded.

`present` or `absent` is a read-only observation. `not_checked` means no remote check was made;
`unknown` means current state could not be verified. Unsupported resources still show their recorded identities when
available. The preview rechecks authority and the inventory after provider reads. Changes during
the read invalidate it; a successful preview cannot authorize a later deletion.

## Request deletion

Use `job_cleanup`, an `operate` token and an `Idempotency-Key` of 8–256 printable non-whitespace
ASCII bytes:

```http
POST /v1/workspaces/{w}/jobs/{job_id}/cleanup
Idempotency-Key: cleanup-attempt-001
Content-Type: application/json

{"attempt_id":"att_example"}
```

Uncollected outputs require the explicit body field `"discard_uncollected":true`. The same field
may be included as a Boolean query parameter to preview that choice. A prepared attempt that
never started, including a definitively rejected submission, does not require output discard.

Kaggle deletes by owner/name. The explicit cleanup request authorizes deletion only after checking
the original account, numeric ID, name, version, privacy and source/ownership marker. The request
has no separate name-based-deletion acceptance field; the preview exposes no such flag or warning.
If someone replaces the resource between the check and deletion, the replacement may be deleted,
including its versions and outputs. A rename can leave the original resource undeleted even when
the recorded name is absent. These are limits of the provider's deletion API, not permission to
adopt a target already known to have a different identity.

Active or unknown execution, unresolved creation/submission, current worker leases, pending
controls/collection and a reference shared with another attempt
prevent cleanup. A ready preparation still held for start is ineligible. The worker checks these
conditions again before each mutation, using the original frozen binding and target snapshots.

HTTP 202 only confirms durable intent. Follow its `Location` with GET to read the operation and
its `resources` outcomes:

| Resource state | Meaning |
|---|---|
| `pending` | No deletion intent has been committed for this resource. |
| `running` | A fenced deletion intent exists; its result may still be unknown. |
| `deleted` | The adapter confirmed deletion; name-based targets retain the check/delete replacement risk described above. |
| `absent` | The adapter confirmed the original target is already absent. |
| `retained` | This resource was excluded or refused without a new deletion. |
| `failed` | Access was refused; no deletion was confirmed. |
| `unknown` | The deletion outcome could not be established. |

Execution must be deleted or confirmed absent before staging deletion. The whole operation
succeeds only when every resource has one of those two confirmed outcomes; otherwise it fails
with individual outcomes intact. A failed operation can therefore contain a confirmed deletion.
Pending, applied or uncertain cleanup blocks collection from the affected remote data.

The same key replays its original receipt. After a lost acknowledgement, restart recovers only
the original targets under a new lease fence. For name-based deletion, recovery only reads and
never repeats the delete RPC. A terminal failure needs an explicit new request
to try cleanup again; that request preserves the old operation and target evidence. Neither
recovery path prepares, starts or retries compute.

## Provider limits

| Provider | Scope and conditions |
|---|---|
| SSH | Deletes only the recorded stopped container by its full immutable ID on the recorded Docker engine. Requires frozen SSH host/account identity, preparation receipt, plan and matching ownership labels. Never forces removal or removes volumes. |
| Kaggle | Verifies the recorded account, numeric ID, name, version 1 and privacy, plus the dataset marker bytes or kernel source hash. Requires terminal kernel status. Deletes the whole object by owner/name; replacement between check and deletion remains possible. |

SSH cleanup removes the container's writable data, including staged copies and outputs. Original
host inputs, bind-mount contents, shared/named volumes, other containers, images and build cache
remain. Workload output cannot supply a host deletion path. Old SSH receipts without a recorded
engine ID remain readable but are retained with `engine_identity_not_recorded`; the current
engine cannot retroactively prove the original one.

The native Kaggle adapter checks unchanged private version-1 kernels and staging datasets on the
original account. A validated synchronous deletion followed by the expected missing-name response
is stronger than an unauthenticated or ambiguous absence read. A 403 alone, including recovery
after a lost acknowledgement, remains unknown and retains staging if execution deletion was not
confirmed. Historical versions and physical provider garbage collection are outside this boundary.

Offline tests establish exact-target and recovery behavior. Native-adapter live cleanup must be
qualified separately; do not transfer historical Python-client evidence to the native transport.
Use the [operator validation procedure](development.md#operator-validation) before real effects.
