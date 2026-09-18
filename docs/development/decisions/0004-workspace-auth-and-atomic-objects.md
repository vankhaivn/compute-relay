# ADR-0004: Separate authority, bytes and object ownership

Status: accepted. Requirements: PRD-07, DAT-01, DUR-01, API-01/02, SEC-02.

## Decision

Store only random application-token digests and scoped workspace/expiry/revocation metadata.
Keep token administration local, provider credentials out of applications, and recheck authority
before ownership publication. Blob identity metadata alone is not application authorization.

Flush data and its identity manifest in one private temporary directory, then publish it without
replacement. Commit ownership separately and return success only after acknowledgement. Failed
or uncertain ownership acknowledgement retains complete bytes; speculative deletion can destroy
committed input. No nondurable repository is a production fallback.

Hold an OS blob-root lock, hash opaque IDs into portable paths, reject unsafe permissions/links/
special files and validate existing roots rather than changing their trust. Unix private modes
and Windows protected DACLs implement the host boundary. HTTP remains guarded literal loopback.

## Reason and consequences

A separate JSON ownership database duplicates SQLite; separate file publication creates a race.
Directory publication retains whole-byte identity but is not cross-resource atomicity, deduplication,
range upload or complete-orphan cleanup. Windows directory-sync/power-loss and hostile same-user
filesystem guarantees are not claimed. Object counts/free space also constrain metadata pressure.

See [objects](../auth-and-objects.md), [storage](../storage.md) and [local access](../../local-runtime.md).
