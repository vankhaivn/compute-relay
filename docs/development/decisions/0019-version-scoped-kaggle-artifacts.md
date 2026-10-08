# ADR-0019: Version-scoped reads and immutable artifact publication

Status: accepted. Requirements: JOB-05, PRV-02, VER-02.

## Decision

Bind ArtifactReader to the original Executor and frozen declarations. Download the original
explicit-version result manifest first and derive a bounded, collision-free candidate catalog
from its declared outputs plus fixed controls. Do not enumerate unrelated provider files. Use
exact-file/version SDK downloads, not ZIP extraction. Verify original manifest/nonce/input hashes
first. Omit an optional control only when its exact provider download returns 404; other errors
remain failures. Candidate hashes are claims until
actual byte verification and full M3 manifest validation.

In-memory catalog cursors bind a complete snapshot; durable collection pins remain M3-owned and
survive restart. Fetch only against original pinned path/length/digest. Require independent hashes,
EOF/Close, final terminal/identity checks and successful helper exit before transfer success.
A fully written temporary payload can still fail. M3 withholds successful blob EOF and atomically
publishes only the complete verified set. Retry collection, never compute or a new result pin.

## Reason and consequences

A provider working directory can contain large code, input and scratch trees unrelated to
published results. Enumerating these trees consumes read requests without proving payload hashes.
Manifest-derived candidates preserve the original execution source and allow recovery of existing
attempts. A missing candidate fails its exact-file transfer before publication; manifest metadata
alone does not prove its existence. Arbitrary URLs, bulk archives and successful-byte-prefix
assumptions bypass identity or completion boundaries. Repeated per-file metadata/manifest checks
trade throughput for safety;
no range resume or persistent signed-URL cache. One approved storage redirect carries no account
headers; no hidden retries. Same-version rerun/original-binary limits and trusted-host assumptions
remain. See [artifacts](../providers/kaggle-artifacts.md) and [collection](../collection.md).
