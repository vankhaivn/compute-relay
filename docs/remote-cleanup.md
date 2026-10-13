# Remote cleanup

Cleanup removes remote data recorded for one settled attempt. It never cancels compute, releases
hardware, changes quota facts or expires verified local artifacts. Nothing is deleted by age,
name prefix or a preview. Local files follow [local retention](development/retention.md).

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
`unknown` means current state could not be verified. Local admission mode lists inventory with
`provider_not_configured`. Unsupported resources still show their recorded identities when
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

Kaggle targets show `name_based_delete: true` and a `warning` in the preview. Kaggle deletes by
owner/name: if someone replaces the resource after Relay checks it, the replacement may be deleted,
including its versions and outputs. Renaming can leave the original object undeleted even when
the recorded name is absent. To accept these risks, add `"accept_name_based_delete":true` to
the cleanup body. The same Boolean query parameter previews eligibility with that acceptance;
it does not authorize deletion. Omission or false retains the target with
`name_based_delete_not_accepted`. Acceptance never permits deletion of a resource already known
to have a different ID, version, ownership marker or source.

Active or unknown execution, unresolved creation/submission, current worker leases, pending
controls/collection, named input/job/attempt holds and a reference shared with another attempt
prevent cleanup. A ready preparation still held for start is ineligible. The worker checks these
conditions again before each mutation, using the original frozen binding and target snapshots.

HTTP 202 only confirms durable intent. Follow its `Location` with GET to read the operation and
its `resources` outcomes:

| Resource state | Meaning |
|---|---|
| `pending` | No deletion intent has been committed for this resource. |
| `running` | A fenced deletion intent exists; its result may still be unknown. |
| `deleted` | The adapter confirmed deletion; name-based targets retain the replacement risk shown in the preview. |
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
| Kaggle | Requires `accept_name_based_delete=true`. Verifies the recorded account, numeric ID, name, version 1 and privacy, plus the dataset marker bytes or kernel source hash. Requires terminal kernel status. Deletes the whole object by owner/name; replacement between check and deletion remains possible. |

SSH cleanup removes the container's writable data, including staged copies and outputs. Original
host inputs, bind-mount contents, shared/named volumes, other containers, images and build cache
remain. Workload output cannot supply a host deletion path. Old SSH receipts without a recorded
engine ID remain readable but are retained with `engine_identity_not_recorded`; the current
engine cannot retroactively prove the original one.

Kaggle deletion and its scope across historical versions and outputs are not live-qualified.
Relay accepts only version-1 targets and confirms deletion through an explicit subsequent 404.
A permission denial, including Kaggle's missing-name 403, is insufficient: the outcome remains
unknown and staging is retained if execution deletion was not confirmed. See the dated
[Kaggle API limitation](development/providers/kaggle-operations.md#remote-cleanup) and
[SSH implementation boundary](development/providers/ssh-host.md#remote-cleanup).

Offline tests establish contract and recovery behavior, not live provider support. Exact-target
live qualification requires the separate authorization in the
[validation checklist](development/validation-checklist.md).
