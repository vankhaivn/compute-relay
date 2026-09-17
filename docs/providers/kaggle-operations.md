# Kaggle quota, logs and control evidence

The read-only Monitor and original-attempt LogReader provide operational observations, not
execution permits. Cancellation is manual without a verified session target. These are component
ports used by explicit composition, not normal-server public log/quota routes or live SSE.

## Quota

`NewMonitor(config, resolver, clock)` performs no work until called. Each serialized ReadQuota
checks local pins and active server account before the SDK read. Require raw free-GPU policy
`isPayToScaleEnabled=false` plus total, used and reserved durations; SDK default zeros/false
cannot fill missing evidence. Missing fields/policy give unknown; malformed/failed reads unavailable.
Reset time is absent rather than guessed.

Parse canonical nonnegative protobuf durations at nanosecond precision, bounded defensively to
366 days per field, then expose conservative whole seconds:

```text
Limit     = floor(total)
Used      = ceil(used)
Remaining = max(0, floor(total) - ceil(used) - ceil(reserved))
Precision = lower_bound
Resource  = gpu
Unit      = seconds
```

Remaining need not equal Limit minus Used because reservations are included separately. Timestamp
before provider work; known observations become stale at five minutes without a refreshed timestamp.
The existing store/scheduler owns exhaustion policy: unknown or stale positive data cannot clear
an exhausted latch. A fresh positive observation is not a reservation against external consumption
or later account changes. This is account quota, not a workspace entitlement or billing feature.

## Log snapshots

`NewLogReader(executor, monitor)` requires the same frozen configuration and recorded reference.
Read the explicit version-1 log field from ListKernelSessionOutput, not artifact URLs/cursors.
Check exact source/account/private/kernel ID before and after the read. Failed final identity
checks discard all log bytes. No session or kernel is created to obtain a log.

Absent/null log is unavailable; an explicit empty string is valid empty. Terminal snapshots are
`after_completion`, others `delayed`, never a live-stream claim. Limit snapshots to 64 KiB and
lines to 16 KiB at UTF-8 boundaries, with explicit truncation and pages of 1–100 lines. Cursors
bind full reference, snapshot digest and offset; changed snapshots require a deliberate fresh
read rather than mixed pages. Pagination cannot recover truncated history.

Redact the current token literal before truncation. Other secrets or transformed credentials
may remain: logs are untrusted workload/provider text, not commands or ordinary diagnostic
exports. A future public handler must enforce current workspace authority around private reads.
[Retained artifact logs](kaggle-artifacts.md) are a separate verified-byte/publication path.

## Cancellation, timeout and capabilities

A kernel ID is not the session ID needed by the reviewed cancellation RPC. Current Cancel validates
reference/operation then returns manual-required with zero provider/credential I/O. Do not guess
IDs, start an interactive session or delete resources as cancellation. Existing controls preserve
active evidence and original receipts through later completion.

TimeoutBudgets exposes frozen remote wall/setup/finalization and local invocation budgets.
Local deadline, generic ERROR or cancellation acknowledgement does not prove remote timeout,
termination or release. No overall-job deadline controller is added.

OperationalDescriptor separates implementation support, conditions and live evidence. Quota and
completed-log components are conditional offline support; live logs and provider timeout enforcement
remain unknown. Current batch cancellation, custom containers and retained sessions are unsupported
by this path. No fabricated account-check time or unconditional batch readiness is supplied.

## Process boundary

Monitor uses the pinned SDK's version-bound private session/read guard without running its mutation
entry point. Token/request arrive over stdin in isolated Python with an empty temporary home/cwd,
no ambient credentials/proxies or retries/redirects. A 60-second service context contains local
checks and a 30-second child budget/25-second watchdog; connect/read limits are 5/10 seconds.
Requests/token are capped at 8 KiB, stdout 128 KiB and discarded stderr 16 KiB. Nonzero/late/oversized
or contradictory output fails without reflecting raw errors. This is a cooperative leaf boundary,
not hostile-host protection or secure erasure. See [ADR-0018](../decisions/0018-kaggle-operational-evidence.md)
and record actual account conclusions through [validation](../development/validation-checklist.md).
