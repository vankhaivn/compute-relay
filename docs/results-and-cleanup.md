# Results and cleanup

Paths below are under `/v1/workspaces/{w}/jobs/{job_id}`. Every request names the original
`attempt_id`, including historical attempts.

## Collection

After the attempt finishes, Relay lists the remote outputs, downloads the declared ones, checks
them against the runner's manifest (job, attempt, nonce, input digests, sizes, SHA-256) and
publishes the whole set at once. Partial or unverified bytes are never visible.

- `result_collection: "automatic"` (default) collects on its own.
- `result_collection: "manual"` waits with `collection.state=awaiting_request` until you send
  `POST /collect` with `{"attempt_id": …}`, an operate token and an `Idempotency-Key`. This
  transfers files; it does not run compute.

`collection.state` moves through `not_ready`, `awaiting_request`, `pending`, `discovering`,
`transferring`, `verifying`, then `available` (or `failed`, `expired`, `unknown`). Progress counts
selected output bytes; there is no partial resume. A failed payload can still publish its failure
manifest and logs.

A failed collection records a provider-neutral `details.reason` naming the failed step (for example
`artifact_manifest_download_failed` or `artifact_credential_unavailable`) on the job and the
operation; provider error text is never stored. Retrying means a new collect request for the same
attempt, never a compute rerun.

A temporary provider verification fault is a retryable collection failure; only a confirmed
identity mismatch invalidates the identity. Collection receives its configured base timeout
(ten minutes by default) plus one second per MiB of all selected files, including manifests and
logs, rounded up, capped at two
hours. Before output discovery, the admitted output bound supplies the size. An exhausted
collection budget records a retryable failure instead of silently restarting from zero when its
lease expires. Runtime shutdown leaves the accepted ticket for recovery.

## Download

| Request | Returns |
|---|---|
| `GET /artifacts?attempt_id=…` | Paged list of published artifacts |
| `GET /artifacts/{id}?attempt_id=…` | Metadata: path, size, SHA-256 |
| `GET /artifacts/{id}/content?attempt_id=…` | The bytes |

All need a read token. Unpublished or foreign targets return 404; expired results return 410.
Range requests are not supported.

**A download is complete only when the trailer says so.** Content is sent as HTTP/1.1 chunked
data with `Trailer: X-Compute-Relay-Verified`; the final value `true` arrives only after Relay
re-checked the bytes, hash and your authority. A client must:

1. Check the identity headers, byte count and SHA-256 itself.
2. Require exactly one trailer `X-Compute-Relay-Verified: true` after a clean end of body.
3. Treat a missing, false or early trailer as failure — even for empty files.
4. Write to a temporary file in the destination directory and rename only after all checks pass.

Do not follow redirects or proxies; HTTP/2 and generic proxies are not qualified.

## Release local results

After durably importing a verified result, a consumer can explicitly waive the seven-day local
retention window. `POST /release-results` requires an operate token, an `Idempotency-Key` and one
exact verified output:

```json
{"attempt_id": "att_example", "artifact_id": "art_example", "sha256": "<64 lowercase hex characters>"}
```

This expires the **whole local result set** for that attempt, including logs and manifests.
Active, unknown, recovering or locally busy work is refused. It does not delete inputs, metadata,
provider resources or results belonging to another attempt.

The response names the workspace, job, attempt and output, with `released_at`, `files`, `bytes`,
`pending_files` and `reclaimed_bytes`. HTTP 200 means all files are reclaimed; HTTP 202 means
deletion is still pending. Tombstones commit before file deletion, so downloads return 410
immediately. Repeat the original key and body after a lost response or partial deletion; the
ordinary retention worker also finishes pending deletion after a restart. Reusing a key for
another target returns 409. Already expired results can be acknowledged through the same exact
identity. Availability is advertised by the `result_release` feature.

## Release local inputs

After durably importing results, preview exact collected jobs with
`POST /v1/workspaces/{w}/input-release-preview` (read scope, no idempotency key):

```json
{"jobs":[{"job_id":"job_example","attempt_id":"att_example","artifact_id":"art_example","sha256":"<64 lowercase hex characters>"}]}
```

Supply 1–100 exact attempts, each through one verified `output` artifact. The output metadata
remains usable after result release. The response includes normalized `jobs`, `files`,
`releasable_bytes`, `preview_sha256`, `workspace_id` and `measured_at`; shared inputs count once.
It contains no host paths or unbounded file list. No deletion or release is recorded by preview.

Confirm with `POST /v1/workspaces/{w}/input-releases` (operate scope, `Idempotency-Key`), sending
the same `jobs` plus `preview_sha256`. The server rechecks the entire inventory transactionally;
a changed preview returns 409 and requires a new preview. Job order is immaterial. Unknown fields,
duplicate attempts and mismatched output identities are rejected.

Only requested collected attempts waive their seven-day input window. Every referencing attempt
must be inactive, settled and free of local work; another attempt's window remains in effect
unless that exact attempt was also explicitly released. Requested attempts' consent persists, so
the ordinary sweep can release shared bytes once the remaining references become eligible.
New retries never inherit another attempt's waiver. This action does not release results or
remote resources, and preserves job and input metadata.

The receipt returns `workspace_id`, `jobs`, `preview_sha256`, `released_at`, `files`, `bytes`,
`pending_files` and `reclaimed_bytes`. Counters describe only the exact objects tombstoned by this
confirmation, not shared objects that remain pinned. HTTP 200 means no pending files; HTTP 202
means deletion remains pending. Up to 100 files are drained per request. Replay the original key
and body to recover a lost response or continue deletion; the hourly sweep also recovers pending
tombstones after restart. Reusing a key with a changed body returns 409. Availability is advertised
by `input_release`. Requests are capped at 64 KiB and inventory scans at 100,000 objects/references.

## Cleanup

Remote data stays until you delete it (or a [retention rule](#automatic-remote-retention) does).
Cleanup deletes only what Relay recorded for one settled attempt; it never cancels compute and
never deletes by name prefix or age.

1. Preview: `GET /cleanup?attempt_id=…` (operate token). Lists up to two resources (execution,
   then staging) with their eligibility. A preview does not authorize anything.
2. Delete: `POST /cleanup` with an operate token and an `Idempotency-Key`:

   ```json
   {"attempt_id": "att_example", "discard_uncollected": true}
   ```

   `discard_uncollected` is required only if results were never collected. A prepared attempt
   that never started does not need it.
3. Follow the `Location` (`GET /operations/{id}`) until each resource settles:

| Resource state | Meaning |
|---|---|
| `pending` | Not started. |
| `running` | Deletion in progress; outcome may still be unknown. |
| `deleted` / `absent` | Confirmed gone. |
| `retained` | Excluded or refused; nothing deleted. |
| `failed` | Access refused; no deletion confirmed. |
| `unknown` | Outcome could not be established. Inspect; Relay never repeats it automatically. |

The operation succeeds only if every resource is `deleted` or `absent`. Cleanup is refused while
the attempt is active or unknown, has pending controls or collection, or shares a resource with
another attempt.

| Provider | What is deleted |
|---|---|
| SSH | The attempt's stopped container (by full ID, with ownership labels checked), including its staged inputs and outputs. Never forced; volumes, images and shared cache stay. |
| Kaggle | The attempt's kernel and staging dataset, after checking account, ID, name, version, privacy and marker. [Shared input datasets](kaggle.md#shared-input-datasets) are never deleted. Kaggle deletes by name, so a resource swapped in between check and delete could be deleted instead. |

## Automatic remote retention

Optionally declare at admission when Relay should run that same cleanup by itself:

```json
"retention": {"after_collection_seconds": 0, "uncollected_max_seconds": 86400}
```

| Field | Range | Clock starts |
|---|---|---|
| `after_collection_seconds` | 0–2592000 (30 days) | When verified results are published. 0 = right away. |
| `uncollected_max_seconds` | 1–2592000 | When the attempt is confirmed inactive or never started, without published results. Discards uncollected results. |

Give at least one field. Without `retention`, nothing is cleaned automatically. Retention never
stops running work, never acts while remote activity is possible, never repeats an unknown
deletion, and leaves shared cache, application images and Kaggle shared datasets alone.

An unsubmitted attempt that ends with `staging_unconfirmed` releases compute capacity without
claiming its staging dataset is absent. Cleanup preview may observe the original frozen Kaggle
staging identity after the attempt settles. Exact discovery records its numeric ID and marker,
making it eligible for ordinary cleanup without changing the failed state or permitting compute.
An exact HTTP 404 displays `absent`; it does not create a deletion target or durable absence
receipt. A private-name 403 remains unknown. Empty references stay `creation_unresolved`, retaining
local input pins until confirmed cleanup; a terminal state never permits guessing or shared
dataset deletion.
