# ADR-0023: Current-authority artifact delivery with final acknowledgement

- Status: proposed; implemented offline in PR #27, pending owner review/merge
- Date: 2026-09-17
- Task: M5-01c, within the still-incomplete M5-01 runtime/application milestone
- Requirements: API-01/02/03, JOB-05, DX-01; preserves DUR-01/03
- Extends: ADR-0013/0014 and ADR-0021/0022

## Context

M3 publishes verified local results, M5-01a serves local HTTP, and merged M5-01b supplies
application/profile commands. Applications still need authorized access to those published
bytes. A prior successful collection does not prevent later disk damage, expiry or revocation.
A fixed Content-Length or last payload byte can arrive before the source's final Read/Close
fails; declaring download success at that point loses the acknowledgement boundary.

## Decision

Expose three GET-only operations for published artifact pages, metadata and binary content
through the existing `collection.Reader`. Require explicit workspace/job/attempt and artifact
identity; never default to a current/latest attempt. A nil result reader disables routes rather
than falling back to provider collection. These requests do not activate workers or alter
attempts, operations, receipts, events or provider resources.

Sort and validate the entire immutable publication before paging. Bind the cursor to its
target, verification time, complete file set and offset. Preserve cursor identity across
reopen when publication is unchanged, with current authority checked on every request. Treat
metadata as historical publication evidence, not a fresh disk-byte check. Return expiry as
HTTP 410 after authorization, distinct from missing/unpublished/invisible 404.

Serve unencoded octet-stream downloads with opaque attachment names, nosniff and sandbox
headers. Do not interpret remote paths as local destinations or trust payload MIME for inline
rendering. Retained logs remain artifacts, not live log streaming.

Require HTTP/1.1 chunked content and a declared post-body `X-Compute-Relay-Verified: true`
trailer. Emit it only after verified size/hash, explicit EOF, successful source Close and a
final current-authority/unexpired-publication check. Abort late failures with ErrAbortHandler
instead of appending JSON to binary data. The client must independently check the original
identity/size/hash, clean EOF/Close and one declared true trailer; an initial header, complete
body or HTTP 200 is insufficient. Missing/stripped trailers fail closed.

Use the existing private-token application boundary and fresh no-replay/no-proxy/no-redirect
HTTP transport. Download only into an unpublished destination. The CLI requires a user-chosen
new file in a private directory, flushes and independently rehashes temporary bytes, checks
file identity/Close, then hard-links the final name without replacement. Unsupported linking
fails closed; no rename-overwrite fallback. Report possible local publication after a late
filesystem/stdout failure instead of claiming rollback or automatically retrying.

Add strict artifact schema/fixtures, OpenAPI operations and exact contract inventory/lock.
Preserve existing contracts and M3 persistence; the operation inventory becomes eighteen.
Keep all dependency, migration, provider/runner and worker activation boundaries unchanged.

## Alternatives rejected

- Expose arbitrary blob paths or provider URLs: bypasses publication and identity authority.
- Infer the latest attempt: risks downloading results from a different execution.
- Treat fixed length or checksum alone as source completion: loses late Read/Close/authority errors.
- Append an error JSON after streaming begins: contaminates private binary output.
- Buffer complete artifacts before HTTP: defeats bounded streaming for large results.
- Use server-returned paths or replace existing files: risks host path misuse and destructive retries.
- Start collection or compute from GET: changes a read into an unrequested operation.

## Consequences and limits

Clients and any intermediaries must preserve and validate the completion trailer. Generic
browser/download tools that ignore it cannot claim verified delivery. Transparent proxy/HTTP/2
support, byte-range resume and automatic retry are not supplied. Rechecking authority at the
end cannot revoke bytes already sent or lock against a subsequent permission change.

Metadata and legitimate artifacts may contain private user information. No universal secret
scanner, hostile-host attestation or protection from the local operator is claimed. The private
parent directory and same-user filesystem writers are inside the existing trust boundary.
Hard-link and directory-sync guarantees apply only where supported/tested; crashes may leave
private temporary files, and a final file can exist despite a failed stdout receipt.

The server remains admission-only with no provider/collector/scheduler/sweeper workers.
Only an already committed publication can be downloaded. The parent M5-01 milestone, remaining
log/cleanup/provider lifecycle surfaces, TOML configuration and live acceptance retain separate
gates. Nothing in this slice closes M1 or M4-06 live verification.

## Verification

Exact metadata/stream tests, real-loopback HTTP faults, source/Close/expiry/revocation races,
empty/large files, client trailer/pin validation and private file publication/race tests cover
both successful delivery and complete bytes without valid acknowledgement. Actual serialized
responses validate against the schema and operation inventory.

Real M3 collection/SQLite/blob integration publishes five files, then reads them through the
HTTP client and application CLI across reopen. It preserves original receipts and events,
never calls the provider again, and checks explicit expiry/current authorization. Local
standard-library tests and full pinned native/race CI are recorded as distinct evidence in
PR #27 and the [delivery guide](../artifact-delivery.md), which links the reviewed Go sources.
Owner review/merge only; do not start the next slice automatically.
