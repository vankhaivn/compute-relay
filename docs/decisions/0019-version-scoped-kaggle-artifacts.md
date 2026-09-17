# ADR-0019: Version-scoped reads and immutable artifact publication

Status: accepted. Requirements: JOB-05, PRV-02, VER-02.

## Decision

Bind ArtifactReader to the original Executor and frozen declarations. Read all bounded explicit-
version output pages, reject unsafe/colliding/incomplete lists and ignore listing URLs. Use
exact-file/version SDK downloads, not ZIP extraction. Verify original manifest/nonce/input hashes
first and select declared outputs plus fixed controls only. Candidate hashes are claims until
actual byte verification and full M3 manifest validation.

In-memory catalog cursors bind a complete snapshot; durable collection pins remain M3-owned and
survive restart. Fetch only against original pinned path/length/digest. Require independent hashes,
EOF/Close, final terminal/identity checks and successful helper exit before transfer success.
A fully written temporary payload can still fail. M3 withholds successful blob EOF and atomically
publishes only the complete verified set. Retry collection, never compute or a new result pin.

## Reason and consequences

Arbitrary listing URLs, bulk archives and successful-byte-prefix assumptions bypass identity or
completion boundaries. Repeated per-file metadata/list/manifest checks trade throughput for safety;
no range resume or persistent signed-URL cache. One approved storage redirect carries no account
headers; no hidden retries. Same-version rerun/original-binary limits and trusted-host assumptions
remain. See [artifacts](../providers/kaggle-artifacts.md) and [collection](../collection.md).
