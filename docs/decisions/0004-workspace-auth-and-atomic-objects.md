# ADR-0004: Separate workspace authority, blob publication, and object admission

- **Status:** accepted
- **Date:** 2026-09-14
- **Scope:** M2-05 and M2-06
- **Requirements:** PRD-07, DAT-01, DUR-01, API-01/02, SEC-02

## Context

Applications need authenticated uploads without depending on a compute provider. M2 builds
portable components; SQLite transactions and the production runtime composition are M3/M5
work. A working filesystem must not be mistaken for a durable ownership database, and an
uncertain metadata acknowledgement must not cause deletion of already committed bytes.

The owner requires operator-managed credentials and local executable smoke checks. GitHub
Actions are offline code-quality/build/test checks, not a credential or GPU runtime.

## Decision

Use the standard library and small consumer-owned repository interfaces:

- `auth.TokenRepository` stores only SHA-256 digests of randomly generated 256-bit API
  secrets, public token identifiers, workspace binding, scopes, expiry and revocation.
  Token creation and revocation are local administrative services, not application routes.
- `auth.WorkspaceRepository` and stored ownership/profile checks enforce workspace access.
  Raw provider credentials do not enter these interfaces. Long uploads revalidate token
  authority before metadata publication.
- `objects.Repository` owns public object visibility. Its insert-only `CommitObject` must
  acknowledge a durable commit. The only supplied implementation in this task is explicitly
  nondurable test support, never a production fallback.
- `blobfs.Store` implements the M2-04 BlobStore port. Data and a small identity manifest are
  written and flushed inside one temporary directory, verified, then published by a
  no-overwrite directory rename. The filesystem manifest is recovery evidence, not public
  ownership authority.
- The object service publishes bytes first, commits ownership metadata second, and only then
  returns a successful receipt. A failed or ambiguous metadata commit leaves the complete
  blob intact for future reconciliation; it never guesses that deletion is safe.
- Hold an OS-level blob-root lock while writing or recovering. Startup removes only
  unfinished staging beneath this versioned, dedicated root. Complete orphan blobs remain.
  This lock does not replace the future runtime state-directory/database lock.
- Hash opaque identifiers into portable path components. Reject symlinks/nonregular data
  and unsafe permissions. Unix uses restrictive modes; Windows uses a protected DACL
  granting the current user, SYSTEM and administrators. Existing roots are validated,
  never silently given a different ACL.
- Keep HTTP restricted to literal loopback addresses, expected Host and same/no Origin.
  Enable bearer authentication, body/time/concurrency/rate limits, no-store responses and
  generated request IDs. Do not expose a production `serve` mode before durable composition.

## Alternatives and trade-offs

A memory-backed production server would be simpler but falsely imply durable tokens and
object admission. A separate JSON ownership database would duplicate the approved SQLite
store and require another migration/recovery design. Both are rejected.

Publishing separate data and manifest files would add a two-file visibility race. Publishing
one directory keeps them together. It creates more filesystem entries and does not implement
deduplication, resumable upload or automatic complete-orphan garbage collection.

Files are synced before rename. Unix parent directories are synced as well. The portable
Windows implementation does not claim directory-sync or sudden-power-loss durability.
Process-crash/restart behavior is tested separately from storage-device guarantees.

The root is operator-trusted: this is not a sandbox against the host administrator or
malicious same-user filesystem mutation. The 20 GiB quota counts blob payload bytes;
metadata/inode pressure is additionally bounded by object count and free-disk reserve.

## Verification and follow-up

Local Linux tests cover revocation, resource/profile isolation, real loopback HTTP,
streaming limits, disconnected/blocked bodies, corruption, disk/write/sync/rename faults,
concurrent writes, and an actual killed-process recovery. The finite `uploadsmoke` command
verifies 4,480,000 bytes, digest, isolation, revocation and reopened blob storage.

Native offline CI covers Linux, macOS and Windows. Windows ACL-specific tests reject a
world-readable disposable root. No test calls Kaggle or allocates GPU compute.

M3 must implement persistent token/workspace/object repositories, coordinate blob manifests
with committed ownership, and test database crash/commit ambiguity. M5 supplies local token
commands, strict runtime configuration and production server wiring.

See [`../auth-and-objects.md`](../auth-and-objects.md) for the implemented API and limits.
