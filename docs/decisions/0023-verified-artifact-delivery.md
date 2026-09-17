# ADR-0023: Authorized artifact delivery with final acknowledgement

Status: accepted. Requirements: API-01/02/03, JOB-05, DX-01; preserves DUR-01/03.

## Decision

Expose GET-only pages, metadata and content for existing publications under current workspace
authority and explicit job/attempt/artifact identity. Bind sorted publication cursors to target/
snapshot/offset. Metadata is historical verification; absent results are not found and authorized
expired results return 410. Reads neither mutate history nor call providers/collectors.

Serve safe octet-stream attachments with HTTP/1.1 chunked framing and a declared final
`X-Compute-Relay-Verified: true` trailer only after byte count/hash, EOF/Close and renewed
context/authority/expiry checks. Late failure aborts without JSON in payload. Client checks the
same facts independently; initial headers, HTTP 200 or the last byte are insufficient.

The CLI writes a user-chosen new private file via flushed/rehash-verified temporary bytes and
no-overwrite hard link. Never trust server paths or replace existing files. Late local publication/
stdout failure retains `download_may_be_published`; unsupported linking fails closed.

## Reason and consequences

Fixed-length or prefix success loses post-byte failures and permission changes. Final acknowledgement
protects delivery but cannot recall bytes already sent. Trailer-preserving HTTP/1.1 and hard links
are requirements; transparent proxy/HTTP/2/browser use, Range resume, secure erasure and hostile-host
proof are not supplied. Normal serve remains admission-only. See [delivery](../artifact-delivery.md)
and [API contracts](../../api/README.md).
