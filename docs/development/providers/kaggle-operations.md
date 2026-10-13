# Kaggle quota, logs and control evidence

The read-only Monitor and original-attempt LogReader provide operational observations, not
execution permits. Cancellation is manual without a verified session target. These are component
ports used by explicit composition. Normal serve exposes authenticated bounded attempt logs;
a public quota route is not shipped and account-scoped live-log qualification remains pending.

## Quota

`NewMonitor(config, resolver, clock)` performs no work until called. Each serialized ReadQuota
checks local pins and active server account before the SDK read. Require a GPU quota object,
free-GPU policy and explicit total, used and reserved duration messages. For the pinned
`ApiAcceleratorQuota`, an omitted `isPayToScaleEnabled` means its schema-defined scalar `false`;
explicit `false` is equivalent. Explicit `true`, null or a non-boolean remains blocked.
Missing/null duration messages remain unknown, never zero; malformed/failed reads unavailable.
Reset time is absent rather than guessed.

This is a field-specific [ProtoJSON default](https://protobuf.dev/programming-guides/json/#presence-and-default-values),
matching [SDK 0.1.35's quota type](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py).
It does not relax missing account, privacy, status, log or duration evidence. Explicit null is
still conservatively rejected by this adapter even though generic ProtoJSON parsers can accept it.

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

## Bounded log replay

`NewLogReader(executor, monitor)` requires the same frozen configuration and original reference.
Normal serve exposes it through authenticated [explicit-attempt log pages](../../../api/README.md#reading-attempt-logs).
Current read authority is checked around provider bytes; the read never grants compute, selects
another account, reconciles submission or changes collection. A legitimate attempt without an
accepted remote reference returns an empty unavailable page.

The pinned client's kernel log route is read with a finite SSE window; completed
JSON event replay is bounded separately. Check source/account/privacy/kernel identity before
and after reading, discarding bytes if the final check fails. No session or kernel is created
for a read. Opaque cursors bind the complete original reference, consumed redacted prefix and
offset. Append-only replay can resume while caught up; a changed prefix returns
`LOG_CURSOR_RESET` and requires an explicit empty-cursor restart with a continuity gap.
A foreign or malformed cursor is rejected. Stream EOF never establishes execution completion.

Pages contain at most 100 lines and 64 KiB of adapter output, with 16 KiB lines at UTF-8 boundaries.
Input replay, events, lines and read duration are separately bounded; truncation remains explicit
and discarded output cannot be recovered by pagination. Running availability can be `live`, while
terminal replay is `after_completion`; these observations remain distinct from current job status.
The HTTP service caps the whole read at 10 seconds and revalidates authority before release.

Redact the current token literal and recognized credential patterns before output bounds and
prefix hashing. Other secrets or transformed credentials may remain: logs are untrusted
workload/provider text. Runner child output is mirrored after redaction through a bounded,
best-effort queue; a blocked or failed notebook writer cannot stall child drainage or execution.
[Retained artifact logs](kaggle-artifacts.md) remain a separate verified-publication byte path.
Account-scoped running delivery and reconnect behavior still require live qualification.

## Cancellation, timeout and capabilities

A kernel ID is not the session ID needed by the reviewed cancellation RPC. Current Cancel validates
reference/operation then returns manual-required with zero provider/credential I/O. Do not guess
IDs, start an interactive session or delete resources as cancellation. Existing controls preserve
active evidence and original receipts through later completion.

TimeoutBudgets exposes frozen remote wall/setup/finalization and local invocation budgets.
Local deadline, generic ERROR or cancellation acknowledgement does not prove remote timeout,
termination or release. No overall-job deadline controller is added.

OperationalDescriptor separates implementation support, conditions and live evidence. Quota and
completed-log components are conditional offline support. Bounded running replay is implemented
offline, while account-scoped live availability and provider timeout enforcement remain unknown. Current batch cancellation, custom containers and retained sessions are unsupported
by this path. No fabricated account-check time or unconditional batch readiness is supplied.

## Remote cleanup

The composed runtime supports explicit name-based cleanup. Preview exposes
recorded private staging dataset and execution kernel IDs, names and versions, unknown size,
`name_based_delete: true` and a replacement-risk warning. Apply requires durable
`accept_name_based_delete=true`; otherwise targets are retained. The adapter verifies the exact
account, numeric ID, canonical name, version 1, privacy and original dataset-marker bytes or kernel
source hash. Active or unresolved kernels are retained. Execution deletion must be confirmed
before staging deletion. No new preparation, submission or quota reservation occurs.

Primary-source review on 2026-10-13, pinned SDK 0.1.35:

- [DeleteDataset request](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/datasets/types/dataset_api_service.py)
  contains owner and dataset slug;
  [its service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/datasets/services/dataset_api_service.py)
  forwards that request to deletion.
- [DeleteKernel request](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py)
  contains user and kernel slug;
  [its service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py)
  forwards that request to deletion.
- The [official API client 2.2.4](https://github.com/Kaggle/kaggle-api/blob/v2.2.4/src/kaggle/api/kaggle_api_extended.py)
  treats the typed delete call as synchronous completion: dataset deletion returns success and
  kernel deletion reports completion after the call returns.

Neither request carries an immutable resource ID/version or compare-and-delete precondition.
A verified GET followed by one of these calls could delete a replacement object using the same
name. The SDK's object-addressed delete surface also does not establish a version-scoped cleanup
contract. The explicit acceptance covers the replacement race, not a known mismatch. Each resource
permits one delete RPC per running intent, with no transport retry. Relay validates HTTP 200 and
the closed, error-free delete response, then reads the target again. An explicit 404 confirms
deletion; Kaggle's post-delete 403 retains the preceding synchronous completion. A still-present
target or other contradictory response remains unknown. HTTP 403 alone cannot establish absence.
Lost-acknowledgement recovery only reads the original name/identity; if it remains present or
unreadable, the outcome stays unknown.

Live qualification on 2026-10-13 covers whole-object deletion of private version-1 kernels and
datasets, synchronous completion and read-only recovery after deliberately lost completion on
one account. This evidence covers the shipped SDK leaf, not the full public cleanup API against
that account. Historical versions and physical reclamation of provider output bytes remain
unqualified.
See [qualification evidence in #97](https://github.com/vankhaivn/compute-relay/issues/97) and
[remote cleanup](../../remote-cleanup.md) for operation and recovery semantics.

## Process boundary

Monitor uses the pinned SDK's version-bound private session/read guard without running its mutation
entry point. Token/request arrive over stdin in isolated Python with an empty temporary home/cwd,
no ambient credentials/proxies or retries/redirects. A 60-second service context contains local
checks and a 30-second child budget/25-second watchdog for quota reads; log mode instead has a
10-second parent/child cap and a finite replay window. Stream connect/read limits are 3/0.5 seconds.
Requests/token are capped at 8 KiB, stdout 128 KiB and discarded stderr 16 KiB. Nonzero/late/oversized
or contradictory output fails without reflecting raw errors. This is a cooperative leaf boundary,
not hostile-host protection or secure erasure. See [ADR-0018](../decisions/0018-kaggle-operational-evidence.md)
and record actual account conclusions through [validation](../validation-checklist.md).
