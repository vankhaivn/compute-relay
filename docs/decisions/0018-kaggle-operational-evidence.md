# ADR-0018: Conservative quota, log snapshots and manual cancellation

Status: accepted. Requirements: PRV-03, OPS-02/03/04; preserves DUR-03.

## Decision

Use explicit serialized account reads and original-attempt log references. Parse raw quota
durations at nanosecond precision before SDK defaults/conversion, requiring free policy under
the pinned quota schema and explicit total/used/reserved messages. Its implicit scalar billing
boolean may omit false in ProtoJSON; only absence defaults, while explicit true/null/wrong types
remain blocked. Duration messages never inherit zero. Floor total, ceil used/reserved and clamp
remaining to zero; mark lower-bound seconds. Missing data stays unknown, errors unavailable,
reset absent. Timestamp before work and preserve provenance when ageing; stale/unknown data
cannot clear exhaustion. See the [field-specific mapping](../providers/kaggle-operations.md#quota).

Read only explicit-version log snapshots with identity checks before/after. Missing/null differs
from empty. Bound UTF-8 snapshots/lines/pages with explicit truncation and reference/snapshot
cursors; changed bytes cannot mix pages. Redact the current token literal, not claim universal
secret-safe logs or live SSE.

Without a verified session target, Cancel validates reference/operation and returns manual-required
without provider I/O. A kernel ID is not a cancellation session ID. Report frozen timeout layers
and support/evidence independently; local timeout and acknowledgements are not termination/release.

## Reason and consequences

Unreviewed defaults, upward rounding, omitted reservations, guessed reset times and latest log/session
targets invent operational facts. A schema-defined implicit scalar default is not an absent duration
message or evidence of capacity. Conservative observations preserve account and attempt safety.
Quota is not reservation, logs are not artifacts, and merged code is not live verification.
See [operational mappings](../providers/kaggle-operations.md).
