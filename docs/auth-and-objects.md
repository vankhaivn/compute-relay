# Workspace authority and immutable objects

This boundary authenticates applications and stores immutable input bytes. It does not execute
uploaded commands. See [local runtime](local-runtime.md) for token administration and
[application CLI](application-cli.md) for upload commands.

## Authority

Tokens are scoped to an enabled workspace with explicit read/write/operate permissions, expiry
and revocation. SQLite stores digests, not token values. Current authority is checked before
resource access and again where publication/commit requires it. Knowing an ID or an idempotency
key does not bypass authority. Foreign resources are not exposed as alternative lookup targets.

The HTTP server is literal loopback with bearer, Host/Origin, body/time/rate/concurrency guards.
Only health is anonymous. These are application boundaries for a trusted operator, not hostile
multi-tenant isolation or proof against malicious same-user services. Token issuance/delivery
limits and uncertain file delivery are documented in the local-runtime guide.

## Upload and publication

`POST /v1/workspaces/{w}/objects` accepts bounded binary bytes under write authority. The service
streams, checks length/digest and explicit EOF, flushes bytes plus identity metadata and publishes
a create-only blob. Ownership metadata commits separately before a successful 201 receipt.
Object metadata includes owner/ID/length/SHA-256, never a physical storage path.

A failed stream never becomes a usable object. Complete bytes preceding a failed or uncertain
ownership acknowledgement remain recovery material; speculative deletion could erase committed
work. Byte publication and SQLite acknowledgement are not an atomic cross-resource transaction.
Upload has no job-idempotency-key guarantee: a deliberate second upload may create another object.

Blob roots have private permissions and exclusive OS ownership; unsafe/symlink/special-file
layouts are rejected. They are not a hostile-host filesystem sandbox. Do not write to blob
directories directly or reuse result storage as an input upload root.

## Other input sources

[Local import](packaging-and-import.md) uses operator-named allowlisted roots and explicit relative
selections. [HTTPS ingestion](https-ingestion.md) validates public destinations and redirects.
Both feed the same immutable verified-byte/ownership boundary. They exist as optional composition
services and are disabled in the normal local host; no undocumented server flag enables them.

Expired input inventory cannot be used for new admission/retry, even when bytes await deletion.
Original receipt replay still requires current authority. See [retention](retention.md) and
[storage](storage.md) for root identity and recovery.
