# Verified collection and publication

The collection engine consumes transfer-only work for an exact terminal attempt. It does not
execute compute, refresh inputs or switch profiles. Normal `serve` exposes existing results
but does not start this engine. The fixed acceptance utility composes it explicitly.

## Stages

```text
commit ticket/lease -> verify original provider binding -> complete candidate listing
 -> retrieve and validate original manifest -> commit immutable result snapshot
 -> transfer selected bytes -> reopen/hash local blobs
 -> atomically publish all artifacts + state/events + operation outcome
```

Use a dedicated private result blob store, separate from application uploads. No transaction
spans network or byte I/O. Leases bind workspace/job/attempt/operation, generation/fence, expiry
and attempt revision. Stale owners or cancellation races cannot publish using an old claim.

## What qualifies

The strict result manifest must match job/attempt/nonce, original bundle/input digests, frozen
output declarations, GPU requirements and consistent phase/exit/time evidence. The complete
catalog must be bounded, collision-free and tied to the same provider resource/version.

Select only declared manifest outputs and fixed controls: `control/execution-result.json`,
optional stdout/stderr and environment JSON. Code/input/scratch files are not outputs. Paths are
logical portable relative names, never host paths; no provider archive/link extraction occurs.
Manifest v1 has no directory-presence entries: an empty required directory cannot be proven and
fails rather than becoming a fabricated output. Use an explicit marker file when necessary.

Candidate hashes are not verified payload bytes. A bounded independent writer checks actual
length/digest; successful blob EOF is withheld until the provider call and receipt succeed.
Errors after the last byte still fail. Reopen and rehash every completed blob before publication.
SQLite commits the complete artifact set, result/attempt state, sequenced events and operation
outcome together. A failed commit publishes no partial metadata, even if complete blobs exist.

## Recovery

One immutable snapshot per attempt survives restart and explicit collection retry. Reuse complete
blobs only after hashing, and fetch missing files against the original pin. Never replace the pin
with a newer listing. An interrupted accepted ticket may be reclaimed; a committed failure needs
one explicit new collect request/key. Lost pin/publication acknowledgement is resolved by reading
committed state, not new compute or duplicate publication. Partial-byte range resume is absent.

Verified failure manifests/logs can be available even when the payload failed. Provider terminal
status alone is not business success, cancellation or hardware release. Original control receipts
remain immutable. [Kaggle retrieval](providers/kaggle-artifacts.md) additionally checks terminal
status/identity after transfer; failed final checks invalidate complete temporary bytes.

## Access, bounds and retention

`collection.Reader` requires current workspace read authority and explicit job/attempt; opening
content also requires a committed artifact ID. It returns local bytes, never provider URLs.
[HTTP/CLI delivery](artifact-delivery.md) adds final verified streaming and create-only file output.

Defaults are two workers, ten-minute invocations, leases with thirty seconds additional margin,
4 GiB selected bytes, 10,000 payload files plus four controls. Manifest/environment are at most
1 MiB each; each log at most 20 MiB. Lower blob/policy limits still apply. Callbacks must cooperate;
shutdown joins them rather than replacing stalled workers without bound.

Unpublished complete blobs and incomplete collection remain recovery material. [Retention](retention.md)
expires a whole verified publication atomically and preserves metadata/history. Expiry prevents
new reads but cannot recall delivered bytes. Database-only backups do not include either blob
root. See [storage](storage.md), [recovery](recovery.md) and the
[operator checklist](development/validation-checklist.md).
