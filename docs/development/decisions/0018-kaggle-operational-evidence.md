# ADR-0018: Conservative quota, log snapshots and manual cancellation

Status: accepted. Requirements: PRV-03, OPS-02/03/04; preserves DUR-03.

## Decision

Use explicit serialized account reads and original-attempt log references. Parse raw quota
before SDK defaults, require the free policy and explicit duration messages, and round so the
remaining capacity is a lower bound. Missing data stays unknown, errors unavailable and reset
absent; stale or unknown data cannot clear exhaustion. The
[field-specific mapping](../providers/kaggle-operations.md#quota) owns the exact rules.

Read only explicit-version log snapshots with identity checks before/after. Missing/null differs
from empty. Bound UTF-8 snapshots/lines/pages with explicit truncation and reference/snapshot
cursors; changed bytes cannot mix pages. Redact the current token literal, not claim universal
secret-safe logs or live SSE.

Without a verified session target, Cancel validates reference/operation and returns manual-required
without provider I/O. A kernel ID is not a cancellation session ID. Report frozen timeout layers
and support/evidence independently; local timeout and acknowledgements are not termination/release.

## Reason and consequences

Unreviewed defaults, upward rounding, omitted reservations, guessed reset times and latest log/session
targets invent operational facts. Conservative observations preserve account and attempt safety.
Quota is not reservation, logs are not artifacts, and merged code is not live verification.
