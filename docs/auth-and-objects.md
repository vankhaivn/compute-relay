# Workspace authentication and immutable object uploads

> **Tasks:** M2-05 and M2-06, implemented offline and merged in PR #7.
>
> **Boundary:** tested library components and a developer smoke command, not a complete
> production runtime. SQLite-backed token/object ownership and `compute-relay serve` are
> not implemented by this change. No provider credential or compute is required.

## Components and authority

| Component | Responsibility |
|---|---|
| `internal/auth` | Issue/revoke local application tokens; authenticate; enforce scopes, workspaces, profiles and stored resource ownership. |
| `internal/api` | Authenticated loopback HTTP, safe request boundaries, upload and metadata handlers. |
| `internal/objects` | Authorize, publish verified bytes, commit ownership metadata, then return an object receipt. |
| `internal/blobfs` | Stream/hash/flush/publish immutable bytes; enforce disk limits and recover interrupted staging. |
| `internal/testsupport` | Explicitly nondurable fixtures for tests and the developer smoke command only. |
| `cmd/uploadsmoke` | Finite synthetic end-to-end check over actual loopback HTTP and temporary storage. |

A local API token is not a Kaggle credential. Tokens contain 32 cryptographically random
bytes and are returned once through the explicit issuance service. Persistence receives
only a digest, identifier, workspace, scopes, timestamps and revocation state. Ordinary
formatting/JSON redacts the secret wrapper; callers must deliberately request its value.
Revocation/expiry/workspace availability are checked on authentication and again after a
long upload. This does not promise to cancel an already committed request retroactively.

Scopes are `read`, `write` and `operate`; none grants administrative access. Resource
checks use stored ownership for jobs, attempts, objects, artifacts, events, logs and
operations. Foreign and absent object IDs produce the same not-found result. Nested
resource services must additionally verify parent/attempt associations; future route
implementation is not implied by the reusable authorization matrix.

## HTTP surface

| Route | Access and result |
|---|---|
| `GET /healthz` | Minimal public liveness; no provider access. |
| `GET /readyz` | Authenticated local readiness callback; absent/unready dependency returns 503. |
| `GET /v1/info` | Authenticated build/API information; job admission explicitly not implemented. |
| `POST /v1/workspaces/{w}/objects` | Workspace `write`; raw binary upload; 201 only after bytes and ownership commit. |
| `GET /v1/workspaces/{w}/objects/{o}` | Workspace `read`; committed metadata only, never a host path. |

Send `application/octet-stream` or `application/gzip` without Content-Encoding. Archives
are stored as opaque bytes, not extracted. Optional `Content-Length` and
`X-Content-SHA256` declare expected size/digest. Unknown-length/chunked and empty uploads
are accepted within configured limits. The receipt contains `object_id`, `workspace_id`,
`bytes` and canonical `sha256`, with a metadata `Location` header.

M2-07 adds opt-in `POST /v1/workspaces/{w}/objects/import` using this same upload/ownership
path. See [packaging-and-import](packaging-and-import.md) for named roots, strict JSON,
snapshot verification and bundle commands. The table above describes the original M2-05/06
surface. Job/operation routes remain planned; no handler returns successful job `202`
before durable admission exists. URL ingestion, artifact download and runner extraction
belong to later tasks.

The server factory accepts only literal loopback binds (default `127.0.0.1:7331`). Host
must match the configured IP/port. Origin must be absent or exactly same-origin. Cross-site
fetch metadata is rejected; CORS is off and forwarded headers are not trusted. Runtime
errors omit arbitrary backend strings, tokens and physical paths. Incoming request IDs
are replaced, not reflected. Only liveness is unauthenticated.

## Default bounds

| Setting | Default |
|---|---|
| JSON body / one uploaded object | 1 MiB / 2 GiB |
| Blob payload quota / free-disk reserve | 20 GiB / 2 GiB |
| Published plus in-flight objects | 100,000 |
| Copy buffer | 64 KiB |
| Concurrent HTTP requests / uploads | 32 / 4 |
| Global rate / burst | 50 per second / 100 |
| Workspace rate / burst | 2 per second / 20 |
| Workspace limiter keys | 1,024, bounded with idle-only eviction |
| Header / ordinary request / upload / idle timeout | 5 / 10 / 120 / 30 seconds |

These are configurable local limits, not provider limits or performance promises. Byte
quota counts payload bytes; free reserve and object-count limits also bound metadata and
empty-object pressure. Rate exhaustion returns 429 with Retry-After. Oversize returns 413;
invalid size/digest returns 400; a timed-out transfer returns 408; unavailable storage or
reserve returns 503. No local backpressure is described as provider quota exhaustion.

## Publication and recovery

```text
Authenticate and authorize
  -> generate opaque object ID
  -> bounded streaming into private temporary directory
  -> verify length and SHA-256; flush data and identity manifest
  -> atomic no-overwrite directory publication
  -> revalidate token authority
  -> commit workspace ownership metadata
  -> return 201 and object ID
```

Incomplete data is never readable as a complete object. A blob may be complete yet remain
invisible through the metadata API because ownership is not committed. A lost metadata
commit acknowledgement returns an error and preserves the blob: deleting it could destroy
an already committed object. M3 reconciles these records; no speculative orphan deletion
is enabled here.

A version marker and OS lock protect the dedicated blob root. Startup removes unfinished
`upload-*` staging only after obtaining that lock; complete objects survive. IDs are hashed
into path components, avoiding reserved names and path syntax on host platforms. Opening
bytes verifies identity, size and digest before returning a reader.

Unix modes and Windows DACLs are checked separately. File flushes are implemented on all
target platforms; Windows directory-sync and sudden-power-loss durability are not claimed.
Operator-controlled local storage is not protected from its own administrator. Callers of
BlobStore must supply a bounded/cooperative reader; HTTP additionally applies actual socket
read deadlines so a blocked body is not governed by context cancellation alone.

## Executable checks

With the repository's pinned Go toolchain, run from the repository root:

```text
go test -race ./internal/auth ./internal/api ./internal/objects ./internal/blobfs
go test -count=25 ./internal/auth ./internal/api ./internal/objects ./internal/blobfs
go run ./cmd/uploadsmoke
```

The smoke command creates synthetic tokens, two fixture workspaces, an actual loopback
listener and a temporary blob root. It verifies 4,480,000 streamed bytes and their digest,
metadata access, cross-workspace rejection, revocation and blob reopen. It exits and removes
its temporary files. Its report labels metadata as `nondurable-test-fixture`.

The implementation was checked locally on Go 1.23.2 Linux using an external temporary
module file because toolchain/dependency downloads were unavailable. The committed module
remains Go 1.27.1. Targeted vet/race/build, 25 repeated tests and the executable smoke passed;
full-repository and native-platform checks use the existing offline CI with Go 1.27.1.

Fault tests include injected disk writes/sync/rename failures, reserve exhaustion, actual
process kill/restart, simultaneous uploads, digest mismatch, incomplete body, slow/disconnected
TCP transfer, unknown-length limits, corrupted files, unsafe permissions and revoked tokens.
No Kaggle authentication, resource creation, workload execution or GPU allocation occurs.

See [ADR-0004](decisions/0004-workspace-auth-and-atomic-objects.md) for alternatives and
[`../api/README.md`](../api/README.md) for versioned contracts.
