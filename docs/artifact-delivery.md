# Read and download published artifacts

Deliver results already committed by the collector, with current workspace authority and
end-to-end byte verification. These commands/routes do not fetch from Kaggle, start collection
or make a queued job produce results. A job admitted with `result_collection=manual` first needs
an explicit `POST /jobs/{job_id}/collect` request for its original terminal attempt; poll additive `collection` status
until verified publication is available. Its progress describes selected provider output work,
separately from these local content reads. Collection and publication semantics are described below.

## HTTP contract

```text
GET /v1/workspaces/{w}/jobs/{j}/artifacts?attempt_id={a}
GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}?attempt_id={a}
GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}/content?attempt_id={a}
```

The [API contracts](../api/README.md) defines parameters and response shapes. All require current
workspace read authority and explicit target IDs. Wrong/duplicate query fields, bodies, encoded
path ambiguity, non-GET methods and Range/If-Range are rejected. Host paths, blob IDs and provider
URLs are not content selectors. A nil result reader disables the routes, not a fallback.

Only committed publications are visible. Missing/unpublished/foreign targets are not found;
expired results return 410; revoked credentials are rejected before disclosing expiry. Pagination
binds a sorted complete publication and target to a digest/offset. Unchanged cursors survive
reopen; changed or mismatched snapshots fail. Reads do not alter receipts, attempt state or events.

## Completion means more than the last byte

Content is an octet-stream attachment with exact target/size/SHA-256 identity headers and
no-store/safe-rendering headers. It deliberately uses HTTP/1.1 chunked framing without
Content-Length. The server declares `Trailer: X-Compute-Relay-Verified` and sends the final
value `true` only after exact bytes/hash, explicit EOF, successful source Close, live context
and renewed authority/expiry/snapshot checks.

A late read/Close/panic/timeout/expiry/revocation aborts the stream without JSON appended to
private bytes and without the success trailer. Already delivered bytes cannot be recalled;
a final check is not a lock against future authority changes.

Clients must independently validate target headers, byte count, digest, clean body EOF/Close and
exactly one properly declared final true trailer. An initial header is not final acknowledgement.
Missing, false, duplicate or early trailers, compression and fixed-length replacement must fail,
including empty files. Preserve HTTP/1.1 trailers end to end; generic proxy/browser/HTTP/2
interoperability is unqualified. Requests should not follow redirects or ambient proxies.

Publish a downloaded file only after end-to-end verification. Use a private same-directory
candidate and a new caller-selected destination; do not derive a host path from provider metadata.
Already delivered bytes cannot be recalled after later revocation or expiry.

## Collection and publication

`result_collection` is immutable `automatic` or `manual`; omission keeps automatic behavior.
Manual jobs never create automatic tickets, including diagnostics. Request collection for the
original terminal attempt through `POST /v1/workspaces/{w}/jobs/{j}/collect` with operate authority,
`Idempotency-Key` and `{"attempt_id":"att_EXAMPLE"}`. This grants transfer, not compute.
An interrupted accepted ticket may be reclaimed; a committed failure needs an explicit new request.

The collector verifies the original provider binding, complete bounded candidate listing and result
manifest, commits one immutable result pin, transfers selected bytes and reopens/hashes complete
local blobs. It then commits the entire artifact set, attempt/result state, events and operation
outcome atomically. No transaction spans provider/blob I/O; stale fenced owners cannot publish.
Reuse completed blobs only after hashing; a newer listing cannot replace the original result pin.

The manifest must match job/attempt/nonce, original bundle/input digests, frozen output declarations,
GPU requirements and consistent phase/exit/time evidence. Select only declared outputs and fixed
controls/logs/environment data. Paths are logical relative names, never host deletion selectors.
Required empty directories cannot be proven without a declared marker file. Candidate hashes alone,
complete bytes without successful source Close, and a failed final provider receipt are insufficient.

Collection status distinguishes `not_ready`, `awaiting_request`, `pending`, `discovering`,
`transferring`, `verifying`, `available`, `failed`, `expired` and `unknown`. Progress counts selected
output bytes, excluding control/protocol overhead. Total is null before discovery. Completed bytes
include rehashed reused blobs; received bytes count only new stream bytes in the current fenced
generation and can be null when unsupported. Full counts can precede verification failure.
Only a committed `available` publication is readable. There is no partial-byte range resume.

A failed collection preserves a bounded provider-neutral `details.reason` on the job condition and
collection operation when the adapter identifies the failed step (for example,
`artifact_manifest_download_failed` or `artifact_credential_unavailable`). Provider messages,
responses and underlying error text are not stored in those public problems. Retry still requests
collection for the original attempt and never reruns compute.

A verified failure manifest/log can be available when the payload failed. Provider terminal status,
result availability and hardware release remain separate. See [recovery](recovery.md) for pin,
publication and retention uncertainty and [compatibility](compatibility.md) for evidence limits.
