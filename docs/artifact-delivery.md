# Read and download published artifacts

Deliver results already committed by the collector, with current workspace authority and
end-to-end byte verification. These commands/routes do not fetch from Kaggle, start collection
or make a queued job produce results. See [collection](collection.md) for publication semantics.

## Commands

Use actual job, attempt and artifact IDs plus an application token with `read` scope:

```sh
./compute-relay job artifacts --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --limit 100
./compute-relay artifact show --workspace app --token-file /absolute/private/runtime/app-token --job job_ID --attempt att_ID --id art_ID
./compute-relay artifact download --workspace app --token-file /absolute/private/runtime/app-token --job job_ID --attempt att_ID --id art_ID --output /absolute/private/results/chosen-name.bin
```

Listing returns one explicit page; use its cursor for continuation. There is no implicit latest
attempt. Metadata records historical verification and workload phase, not a fresh disk check
or a guarantee of successful execution. Retained stdout/stderr are artifacts, not live streams.

The output parent must already exist, be private and controlled by the operator. The destination
must be new. Its name comes only from `--output`, never a provider path or Content-Disposition.
Existing files fail before token/network work. On Windows use corresponding `.exe` and paths.

## HTTP contract

```text
GET /v1/workspaces/{w}/jobs/{j}/artifacts?attempt_id={a}
GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}?attempt_id={a}
GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}/content?attempt_id={a}
```

The [OpenAPI](../api/openapi.json) defines parameters and response shapes. All require current
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

The CLI independently validates headers, bytes, digest, clean body EOF/Close and exactly one
properly declared final true trailer. An initial header is not final acknowledgement. Missing,
false, duplicate or early trailers, compression and fixed-length replacement fail, including
empty files. Fresh no-replay transports do not follow redirects or ambient proxies.

## Create-only local publication

Download writes a private same-directory temporary file. After HTTP verification it flushes,
rehashes local bytes, checks file identity/Close and hard-links to a new final name without
replacement. Unsupported hard links fail; overwrite-by-rename is not a fallback. A racing
creator's destination is not replaced.

The success receipt says `delivery=verified-new-file`; stdout contains metadata, not payload.
Failure after final linking, directory sync or stdout can retain `download_may_be_published=true`.
Do not assume rollback or delete the destination to retry automatically. Ordinary errors remove
owned temporary names; a crash can leave them for private investigation.

HTTP/1.1 trailer preservation and same-filesystem hard-link support are requirements. Byte-range
resume, transparent proxy/HTTP/2/browser interoperability, secure erasure and hostile-host or
arbitrary power-loss guarantees are not supplied. Transfer budgets are finite, not throughput
promises. The current normal server serves existing results only; it runs no workers.

Use the [operator checklist](development/validation-checklist.md) for host verification and
[recovery](recovery.md) for uncertainty. File/stream tests are offline evidence until an actual
operator run is recorded.
