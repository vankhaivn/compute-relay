# ADR-0019: Version-scoped reads and immutable artifact publication

Status: accepted. Requirements: JOB-05, PRV-02, VER-02.

## Decision

Bind ArtifactReader to the original Executor and frozen declarations. Download the original
explicit-version result manifest first, verify its nonce and input hashes, and derive a bounded
candidate catalog from its declared outputs plus fixed controls. Do not enumerate unrelated
provider files; use exact-file/version downloads, not ZIP extraction. Candidate hashes are claims
until the bytes are verified.

Fetch only against the original pinned path, length and digest, and require independent hashes,
EOF/Close, final identity checks and successful helper exit before transfer success. Collection
publishes only the complete verified set. Retry collection, never compute or a new result pin.

## Reason and consequences

A provider working directory can contain large code, input and scratch trees unrelated to
published results; enumerating them consumes read requests without proving payload hashes.
Manifest-derived candidates preserve the original execution source and allow recovery of existing
attempts. Arbitrary URLs, bulk archives and byte-prefix assumptions bypass identity or completion
boundaries. Per-file checks trade throughput for safety, with no range resume or hidden retries.
See [artifacts](../providers/kaggle-artifacts.md) and [collection](../collection.md).
