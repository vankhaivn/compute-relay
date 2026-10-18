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
