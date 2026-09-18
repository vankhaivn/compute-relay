# ADR-0013: Immutable result pins and verified publication

Status: accepted. Requirements: JOB-05, PRV-02, VER-02, DUR-01/03.

## Decision

After exact terminal execution evidence, commit a fenced collection ticket/lease, validate the
complete catalog and original runner manifest, then persist one immutable per-attempt result
snapshot before payload transfer. Select only declared outputs and allowed controls. No Prepare,
Submit, Cancel or Cleanup operation belongs in the collector's transfer source interface.

Require independent bounded size/hash checks and successful provider acknowledgement before
blob EOF. Reopen and hash complete local blobs, then atomically publish the whole artifact set,
result/state/events and operation outcome. No transaction spans network/blob I/O. Stale ownership,
revocation/publication races and partial commits cannot expose a partial result.

## Reason and consequences

A provider terminal status or complete temporary file is insufficient. A durable pin prevents
restart/retry from mixing newer outputs; verified cached blobs may be reused, but the pin is never
replaced. Committed failed collection needs explicit transfer-only retry, not another compute run.
Failed payload manifests/logs can remain available without reporting business success. Empty required
directories cannot be established by manifest v1, and partial-byte resume is not supplied.

See [collection](../collection.md), [artifact retrieval](../providers/kaggle-artifacts.md) and
[authorized delivery](../../artifact-delivery.md).
