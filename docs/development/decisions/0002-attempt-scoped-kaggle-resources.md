# ADR-0002: Isolate remote resources by attempt

Status: accepted. Requirements: JOB-04/05, DOM-01/02, DUR-03, PRV-02, OPS-04, VER-02.

## Decision

Use a deterministic provider-safe execution identity per attempt, not a shared latest-run
resource per application or job. Persist installation/workspace/job/attempt/instance identity,
nonce, original input digests, frozen configuration and submission intent before a remote effect.
Keep stronger numeric/version/session evidence when actually available.

Bind the runner result manifest and every artifact to the original attempt/nonce/input hashes.
Reject missing/mismatched identity and stale terminal regressions. Detectable external edits
or reruns require attention, not automatic adoption. Never repeat an ambiguous push automatically.
Cleanup requires exact owned, safely resolved resources; it is not cancellation.

## Reason and consequences

Shared resources confuse retries, delayed observers and newer outputs. Per-attempt resources
provide a stable recovery key but accumulate until separately authorized cleanup. They do not
establish remote exactly-once execution or prove that an unobservable same-version rerun never
occurred. Current checks and the SDK upsert limitations are described in
[execution](../providers/kaggle-execution.md) and [collection](../collection.md).
