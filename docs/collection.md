# Verified artifact collection and recovery

> **Task:** M3-06, implemented offline; PR #16 in review until owner merge.
>
> **Scope:** an explicitly composed transfer service, SQLite publication and authenticated
> internal result reads. No production `serve`, artifact HTTP routes or live provider claim.

## Collection boundary

`internal/collection.Engine` consumes M3-05 collect tickets for attempts with matching
terminal provider evidence. When explicitly run, it can create the first ticket for a
collectible attempt with no collection history. Migration, admission, cached GET and
terminal dispatch do not start an engine. A failed ticket is not replaced automatically.

```text
terminal attempt and matching frozen provider identity
  -> commit collection ticket/lease and collecting state
  -> resolve and verify the original provider/configuration/account binding
  -> enumerate bounded pages; retrieve the result manifest
  -> validate schema, identity, frozen requirements and selected file metadata
  -> commit an immutable per-attempt result snapshot
  -> stream selected bytes into a dedicated private blob store
  -> reopen and independently hash every pinned blob
  -> atomically commit all artifact metadata, result/state/events and operation outcome
```

The worker's private source interface exposes only binding verification, artifact listing
and artifact fetch. It has no Prepare, Submit, Cancel or Cleanup method. Profile remapping
cannot change the accepted binding. Collection neither reruns compute nor refreshes inputs.

Compose `collection.New(store, registry, resultBlobs, clock, config)` and invoke `RunOnce`
for one finite operation or `Run` for a fixed transfer pool. Supply a **dedicated
collector-owned `blobfs.Store` root**, not the application upload root. This is a trusted
Go composition API, not a shipped CLI command. The blob store keeps its existing process
lock, private permissions, create-only publication and local capacity policy. Configure its
per-object and total limits deliberately; lower blob limits can reject a transfer even
when collection's aggregate ceiling permits it.

## Identity, manifests and paths

The collector uses the existing embedded result-manifest v1 schema, with external schema
loading disabled. It also rejects duplicate decoded keys, invalid UTF-8/unpaired surrogates,
trailing JSON and excessive nesting. Identity must match job, attempt, nonce, bundle digest
and input-manifest digest. Provider file references must match the exact resource/version.

The manifest is `control/execution-result.json`. Its artifact paths are relative to the
logical `outputs/` namespace. Each file must match the frozen output declaration and the
provider catalog's size and digest. Required outputs must be present for a completed payload;
per-directory byte ceilings apply to the sum of its manifest files. GPU-required/verified
flags are checked against the frozen specification, not trusted as a replacement for it.
Timestamps must be ordered and timeout/phase evidence must be consistent.

Portable ASCII path, case-collision and file/prefix-collision checks apply to the complete
catalog and manifest. Remote paths never become host filesystem paths and provider archives
are never extracted. The provider port supplies logical regular-file streams, not an archive
or a local symlink. A provider adapter remains responsible for meeting that port's contract.

Only declared manifest outputs and a fixed control-file allowlist are selected. Available
`control/stdout.log`, `control/stderr.log` and `control/environment.json` are retained as
historical logs/provenance, not live streaming or trusted instructions. Scratch/code and
other unselected files are not downloaded. Artifact IDs bind the complete remote identity,
logical path, size and digest; a different attempt does not overwrite prior artifacts.

**Directory limitation:** manifest v1 lists files but has no directory-presence entries.
An empty required directory cannot prove presence and fails with `ARTIFACT_MISSING` rather
than inventing a verified output. Nonempty declared directories are verified through their
file entries. Use an explicit marker/output file when empty-directory presence matters;
changed output requirements require a new job.

## Transfer and publication safety

An independently hashing, size-bounded writer sits between the provider and blob storage.
The pipe delivers EOF only after the provider call returns successfully and both actual
bytes and the provider transfer receipt match the pinned identity. An error after the last
byte is still a failed transfer. Ignoring a writer overflow error cannot make it succeed.

Every completed blob is reopened and hashed before the engine creates its private
verification value. `Store.CompleteCollection` accepts only that value for the current
lease and durable snapshot. This is a typed trusted-process boundary, not cryptographic
attestation against malicious in-process code or an untrusted provider operator.

SQLite migration 7 adds `collection_leases`, `collection_snapshots`,
`collection_publications` and `artifacts`. The publication transaction commits all file
rows, result/attempt state, sequenced events, cached condition, collect-operation completion
and lease release together. A failed insert or SQLITE_FULL rolls back all publication
metadata. Complete blobs can exist before that commit but are not application-visible.

No SQL transaction spans provider calls or blob I/O. Leases check workspace/job/attempt,
operation, generation, random fence, expiry and attempt revision. Cancellation changing an
attempt during transfer invalidates the old publisher; recovery uses the latest revision
without changing the remote execution identity or fabricating a cancellation.

## Outcomes and recovery

Verified artifact availability and payload success remain separate. A valid failure
manifest and available logs can be published with failed orchestration. A successful
provider wrapper plus a failed payload is not job success. Provider execution, cancellation
and release evidence are not rewritten from local transfer timing. Provider cancellation
without confirmed local intent stays needs-attention rather than inventing that intent.

| Interruption | Safe next behavior |
|---|---|
| Before a snapshot is committed | Reclaim the accepted ticket after lease expiry and revalidate the same remote identity. |
| Lost snapshot-commit acknowledgement | Reload the durable pin; do not select a newer result. |
| Partial/erroring transfer | Record incomplete/invalid results and a failed operation; publish no artifact metadata. |
| After complete blobs, before publication | Reclaim and rehash the pinned cache; fetch only missing complete files. |
| Lost publication acknowledgement | Read committed status; do not create another publication or duplicate its events. |
| Wrong identity/digest or missing manifest/output | Preserve evidence and a sanitized condition; inspect or explicitly retry collection, never compute. |

A committed failure requires a **new explicit collect request/key** for the same attempt.
Replaying the previous key still returns its original immutable receipt, not a fresh ticket;
GET returns the current operation revision. Interrupted accepted tickets can be reclaimed
without creating another operation. A previously pinned snapshot is reused across both
restart and explicit collection retry; it is never silently replaced with a current listing.
Completed files are reused only after hashing. Partial-byte range resume is not implemented.

Failure conditions appear through existing cached job status. Identity mismatches retain
the domain's `observation` failure stage; artifact failures use `results`. No provider
message, internal cause or credential is copied into the public condition.

## Defaults and operational limits

Defaults are two workers (configurable 1–16), a ten-minute invocation deadline, a lease
lasting that deadline plus thirty seconds, and a one-second idle poll. Selected result bytes
are capped at 4 GiB, with at most 10,000 payload files plus four control files. Manifest and
environment JSON are capped at 1 MiB each, and each selected log at 20 MiB. Lower settings
are supported. Listings use pages of at most 100 entries, bounded cursors/page count and
cycle detection; oversized catalogs fail instead of being silently truncated.

Callbacks must honor context and bounded writes. On blob failure or panic the pipe is
closed and the callback is joined. A callback ignoring cancellation can delay shutdown;
the fixed pool does not replace it with an unbounded new goroutine. Lease expiry alone is
not proof that a callback has stopped, nor a change to remote activity/accounting.

`collection.Reader` requires current workspace `read` authority and explicit job/attempt
identity; `Open` additionally requires a committed artifact ID. It returns metadata and a
stream from the dedicated result store, never a provider URL. Token revocation and foreign
workspace/attempt/ID reads are rejected. Public artifact download/listing routes and CLI
composition remain separate work; the existing fifteen HTTP handlers are unchanged.

Do not delete pins or completed blobs to repair a failed collection. Retention/cleanup is
M3-07 and has not started. Database-only backups do not include either input or result blob
roots. Retain matching bytes and recovery evidence; a restored database does not stop remote
compute. Arbitrary downgrade from schema 7 is unsupported.

## Verification

With the repository's pinned Go 1.27.1 toolchain:

```text
go test -race ./internal/collection ./internal/store/sqlite
go test -count=10 -run=TestCollection ./internal/store/sqlite
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

The first pair are focused developer checks. The existing offline CI runs the latter
repository-wide commands plus native Linux/macOS/Windows tests and CGo-free builds. The
component tests compose actual SQLite and private filesystem blobs with synthetic provider
bytes; no admitted command executes. They force pagination, partial/late-error transfers,
wrong identity/digest/required-output failures, twenty-way claim contention, cancellation
races, profile remapping, revoked reads, schema-6 upgrade and actual SQLITE_FULL rollback.

Separate tests cover snapshot/publication acknowledgement loss and an actual Go subprocess
kill after a durable pin. The killed child performs no provider call; the fake backend stays
in the parent process. Recovery asserts one attempt and one simulated compute submission,
not a live-provider exactly-once guarantee. EOF/panic tests verify callback joining and
that unacknowledged bytes cannot reach publication.

Local engineering had Go 1.23.2 with no pinned-toolchain/module download access. Formatting
and the exact strict-JSON decoder's isolated standard-library race/ten-repeat tests passed;
a two-second bounded fuzz run executed 43,491 inputs. That harness supplies only constants
and an error sentinel and is not shipped. It is **decoder-only** evidence, not local schema,
modernc, full-engine or production-runtime execution. PR #16 records exact final-head CI
results separately. No dependency downgrade, replacement driver or CI workflow was added.

See [ADR-0013](decisions/0013-verified-collection-and-publication.md),
[durable controls](operations.md), [dispatch](dispatch.md) and [storage](storage.md).
