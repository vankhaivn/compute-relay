# ADR-0018: Conservative quota, versioned log snapshots and manual cancellation

- Status: accepted; PR #22 merged on 2026-09-16
- Date: 2026-09-16
- Task: M4-04
- Requirements: PRV-03, OPS-02/03/04; preserves DUR-03
- Extends: ADR-0015/0017 and existing M3 quota/control semantics

## Context

M4-03 supplies original-identity execution and read-only recovery. Operational features must
not infer account allowance from SDK defaults, expose a changed resource's logs, or treat a
kernel ID as the session ID required by cancellation. Source documentation and offline SDK
fixtures are not live-account support. M1 acceptance remains separate.

The pinned SDK supplies used/reserved/total quota durations and version-scoped output metadata
with a log field. Its timedelta/default conversion can discard nanoseconds or make omitted
fields look like zero/false/empty. The existing provider-neutral ports can represent bounded
snapshots, conservative quota and manual-required cancellation without a public contract change.

## Decision

Add an explicitly invoked, serialized account Monitor and original-attempt LogReader. No
background poller, new journal, full Provider stub or public route is introduced. Use the
original explicit credential reference, local pins and verified server account on each read.
Reuse the fixed execution helper's read guard in an isolated module; do not run its mutation
entry point or change generated execution/runner source.

Require explicit free-GPU policy and complete raw quota durations. Parse canonical nonnegative
protobuf seconds at nanosecond precision, with a defensive 366-day ceiling per field. Report
whole seconds with total rounded down, consumed/reserved rounded up and remaining clamped at
zero. Mark the remaining value lower_bound and identify reservation inclusion. Missing data
stays unknown, failed/malformed reads unavailable, and reset time absent. This is GPU/account
observation, not a workspace entitlement or numerical paid-capacity feature.

Timestamp before provider work and age known observations after five minutes without changing
provenance. Keep existing Store.RecordQuota and scheduler freshness/strict/exhaustion rules;
unknown/stale evidence cannot clear an exhausted latch. A fresh positive read is not an atomic
reservation and cannot prevent subsequent external consumption or account-setting changes.

For logs, use only ListKernelSessionOutput's explicit version-1 log field. Verify original
source/private/account/kernel ID before and after the read. Ignore artifact rows/URLs/cursors.
Absent/null log differs from explicit empty. Mark terminal snapshots after_completion and all
other snapshots delayed, never live. Bound UTF-8 snapshots to 64 KiB, lines to 16 KiB, and pages
to the existing 1–100 limit with explicit truncation. Bind cursor to full reference, snapshot
digest and offset; changed snapshots fail instead of mixing pages. There is no hidden cache,
SSE reader or assertion that the provider supplied all historical bytes.

Redact the current token literal before byte truncation; logs otherwise remain untrusted data,
not commands or a universal secret-safe diagnostic export. Ports do not replace application
workspace authorization. Future public handlers must authorize/load the exact recorded target
and revalidate authority before returning private bytes.

Describe capabilities using independent support/evidence fields with no fabricated live check.
Quota/completed-log components are conditional offline support, live logs and provider timeout
enforcement unknown. Current batch cancellation is unsupported because a verified session target
is missing. Validate reference/operation and return manual_required without credential/provider
I/O; never guess a session, create an interactive session or delete as cancellation. Existing M3
controls preserve active evidence and original receipts across manual outcome/restart/completion.

Expose frozen remote wall/setup/finalization and local invocation budgets without conflating
them. Local deadlines, generic ERROR or cancellation acknowledgements are not proven remote
timeout, termination or release. No overall-job deadline service is added by this task.

## Alternatives rejected

- Use SDK zero/false/empty defaults as evidence: can fabricate allowance, free eligibility or logs.
- Subtract used time only: can spend capacity already reserved by another active session.
- Round remaining upward or assume a weekly reset: overstates usable quota.
- Treat output file pagination as log continuation: mixes different contracts and leaks metadata.
- Cache/append changing pages without snapshot identity: combines inconsistent logs.
- Call cancel with kernel ID or deletion: targets the wrong resource or invents termination.
- Advertise all optional features as supported after unit tests: hides account/live uncertainty.
- Change M3 state/receipt semantics to fit a fixture: weakens already-established durable behavior.

## Consequences and verification

Conservative field requirements and bounds can withhold data from an upstream response that
omits fields; no silent compatibility fallback is supplied. Used includes rounding but not
reservations, so remaining need not equal limit minus used in the unchanged port. Logs may be
delayed, incomplete upstream or locally truncated; a changing snapshot requires a fresh read.
Kernel-version/source identity retains M4-03's same-version rerun limitations. Literal redaction,
contexts and buffer clearing do not protect against a hostile operator, arbitrary retained
copies or uncooperative callbacks/kernel operations.

Unit/real-process tests validate protocol, isolation, pagination, precision and manual behavior.
Pinned SDK tests use actual types with mocked HTTP. Real SQLite tests check persistent exhaustion
and operation/attempt/receipt separation without new submissions. Local protocol/format evidence
and full pinned CI are recorded separately in PR #22 and the
[operations guide](../providers/kaggle-operations.md), which links the exact primary sources.

No dependency, workflow, migration, public contract or original execution/runner source changes.
Owner review/merge only; M4-05 retrieval, complete runtime registration and live acceptance retain
their own gates. This decision extends rather than rewrites accepted earlier ADRs.
