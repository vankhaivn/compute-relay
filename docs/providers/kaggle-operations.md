# Kaggle operational capabilities, quota and log snapshots

> **Task:** M4-04, implemented offline; PR #22 merged.
> **Requirements:** PRV-03, OPS-02/03/04; preserves original attempt identity and DUR-03.
> **Evidence:** actual pinned SDK with mocked HTTP, real SQLite/dispatch integration with
> synthetic helper results, isolated process tests and local protocol tests. No live account.

M4-03 execution is merged in PR #21. This task adds numeric GPU-quota interpretation,
bounded version-scoped provider logs, capability descriptions, frozen timeout reporting and
an explicit manual-required cancellation path. It does not enable a complete production
Provider, public log/quota endpoints, live SSE, output-artifact retrieval or remote mutations.
M1 live acceptance and M1-08 go remain separate under proposal section 23.6. M4-05's separate
[artifact reader](kaggle-artifacts.md) adds selected output transfer and publication integration;
it does not turn these provider log snapshots into verified retained artifacts automatically.

## Explicit composition

`NewMonitor(config, resolver, clock)` constructs a read-only account-bound service. Construction
performs no credential or provider access. Share one Monitor for a configuration/account to
serialize expensive reads; each explicit invocation has a 60-second budget including waiting,
local pin checks, credential resolution and provider work. There is no background polling,
automatic refresh, secret cache or SQLite transaction around provider I/O.

Monitor implements `provider.QuotaReader`. `NewLogReader(executor, monitor)` requires the same
immutable M4-01 configuration as the original per-attempt Executor and implements
`provider.LogReader`. Logs require the original persisted `RemoteReference`; source is not
retransmitted. `Executor.Cancel` implements the manual-only Canceller behavior described below.
These methods are internal composition APIs, not newly shipped HTTP/CLI commands.

The ports trust the application's resolved configuration/reference. They are not substitutes
for current workspace authorization. A future public log endpoint must authorize the exact
workspace/job/attempt, load its recorded reference and revalidate authority before publishing
bytes. Quota is account-wide, not a workspace entitlement. No private account name, credential
reference or provider exception body is added to the normalized quota report.

## Capability evidence

`OperationalDescriptor` returns independent descriptions for the existing fourteen capability
names. The descriptor is implementation evidence, not account-scoped live verification:
`AccountChecked=false`, no fabricated check timestamp and no `passed-live` entry.

| Capability | Current meaning |
|---|---|
| Quota reporting | Supported by this offline-tested component, conditional on usable free-GPU quota fields; not capacity reservation or live eligibility. |
| Logs after completion | Supported as bounded version-scoped provider snapshots, conditional on exact identity and an explicit log field. |
| Logs while running | Unknown as a live capability; a delayed snapshot may be returned, but no live-stream guarantee or SSE client exists here. |
| Remote cancellation | Unsupported by this batch-reference path; manual-required without a provider call. |
| Execution timeout | Requested SDK/runner budgets exist; actual provider enforcement remains unknown. |
| Custom containers / retained sessions | Unsupported by this finite batch implementation. |
| Remaining batch/environment/identity capabilities | Conservative unknown entries retain the integration/live requirements and M4-03 identity limitations. |

A supported component with an explicit condition is not a verified unconditional execution
permit. In particular, this descriptor must not be used to hide missing full Provider methods
or satisfy a live-account release gate. Capabilities are not upgraded merely by a successful
preflight or one quota/log response.

## Quota: units, reservations and freshness

An explicit `ReadQuota` verifies local pins, resolves the selected token afresh, and requires
an active token and exact server account match before `GetAcceleratorQuotaStatistics`.
The current component reports only the GPU quota object, not TPU/CPU or billing capacity.

The raw response must explicitly contain `isPayToScaleEnabled=false`, `totalTimeAllowed`,
`timeUsed` and `timeReserved`. Missing/null fields or unknown/paid policy yield **unknown**
without numeric pointers. Malformed duration data or a failed read yields **unavailable**.
The SDK's default false/zero values cannot supply missing evidence. A genuine explicit zero
is retained and is distinct from absent data.

Durations are parsed from their original protobuf strings at nanosecond precision before the
SDK's timedelta conversion can lose sub-microsecond information. Accepted values are nonnegative,
canonical seconds with at most nine fractional digits, bounded to 366 days per field as a
local defensive ceiling, not a claimed provider limit. No floating-point parsing is used.

The existing provider-neutral quota view uses whole seconds:

```text
Limit     = floor(total_time_allowed)
Used      = ceil(time_used)
Remaining = max(0, floor(total_time_allowed) - ceil(time_used) - ceil(time_reserved))
Precision = lower_bound
Resource  = gpu
Unit      = seconds
```

`Precision` describes the conservative remaining allowance: Limit is rounded down, Used is
rounded up and reservations are additionally subtracted. Thus Remaining need not equal
Limit minus Used; the unchanged port has no separate Reserved field. The source string records
`reservations_included`. All returned whole-second integers are exactly representable by the
port's numeric type. No hard-coded weekly entitlement or implicit CPU/paid fallback exists.

`ObservedAt` is local time **before** the bounded read, so transport/queueing cannot make an
older observation look newer. The SDK response supplies no usable reset time here; `ResetAt`
remains absent. `AgeQuota` preserves values/provenance and marks known observations stale at
five minutes. It returns independent pointer values and rejects backward/future-clock misuse.
It does not refresh a timestamp or infer a new allowance when an assumed reset day passes.

A caller can explicitly record the observation with the existing `Store.RecordQuota`, under
the original account scope and outside provider I/O. Existing scheduler freshness/strict-policy
and exhaustion rules remain authoritative. Unknown/unavailable or stale positive readings do
not clear a durable exhausted latch; only a newer fresh positive observation can do so. A stale
zero remains exhausted. No scheduler, schema or migration code is changed by this task.

Even a fresh lower bound is a point-in-time observation, not an atomic reservation: external
activity or account settings can change afterward. M4-03's pre-save free-only gate and normal
provider rejection handling still apply. Numeric quota must not be represented as proof that
a requested GPU is available or that an account was verified live by this PR.

## Logs: snapshots, identity and pagination

The pinned SDK exposes `ListKernelSessionOutput` with user name, kernel slug and explicit
version label. This component requests **version 1** and only the response's `log` field.
It requests one artifact-list entry to bound unrelated metadata; artifact rows/URLs and their
pagination token are neither followed nor exposed as log cursors. There is no arbitrary URL,
archive, payload-output or runtime-event fallback.

Before using the log, the helper verifies current and explicit-version kernel metadata with
the M4-03 exact source/account/private/numeric-ID checks. It queries version-scoped status,
reads the log snapshot, then verifies current metadata again. Identity/source/version/privacy
changes or a failed final read discard all log bytes, including a plausible prefix received
before failure. No new kernel/session is created to obtain logs.

An absent or null log is **unavailable**; an explicitly empty string is a valid empty snapshot.
COMPLETE/ERROR status labels a usable snapshot `after_completion`. Other, unknown, missing or
failed status evidence labels it `delayed`, never `live`. This does not assert that a provider
returned every historical byte or supplied real-time availability. The original same-version
rerun/identity limitations in the execution guide still apply.

The current provider-token literal is redacted before truncation. CRLF/CR become LF, malformed
Unicode is rejected, and the snapshot is limited to 64 KiB at a UTF-8 boundary. Pagination
returns at most the requested 1–100 lines; individual lines are limited to 16 KiB with explicit
`Truncated=true`. Snapshot truncation is also explicit on returned pages. Wire responses over
the existing 3 MiB guard fail instead of being partially decoded.

Each cursor binds the full remote-reference digest, snapshot digest and line offset. A new
page re-reads the snapshot rather than maintaining a hidden server cache. Changed bytes yield
`ErrLogChanged`; restart explicitly with an empty cursor instead of mixing snapshots. Malformed
or foreign-reference cursors are rejected before credential/provider I/O. A cursor is an opaque
position, **not authorization or cryptographic session attestation**. Pagination cannot recover
bytes beyond the local snapshot cap; verified retained payload logs belong to artifact collection.

Provider logs are untrusted workload/provider text. Literal-token redaction is not a universal
secret detector and does not identify arbitrary user secrets or transformed credentials.
Applications must treat the strings as data, not commands, HTML or trusted terminal control.
Do not include raw logs in ordinary diagnostics or support bundles without explicit authority.

## Cancellation and timeout layers

The reviewed cancellation RPC requires a **kernel session ID**. M4-03 records a numeric kernel
ID, source and version, but not a verified association to a cancellable session. These IDs must
not be substituted. There is no call to start an interactive session, discover an unbound
current session, delete a kernel/dataset or guess a cancellation target.

`Executor.Cancel` validates context, operation ID and the original reference, then returns
`manual_required` with no termination confirmation and **zero provider/credential I/O**.
The capability is unsupported for this adapter path, not a claim that all Kaggle modes lack
cancellation. M3's existing capability gate records the manual operation without invoking a
provider cancel. Current execution/activity remain possible/active until terminal observation.

The original POST receipt remains immutable across manual outcome, restart and later execution
completion. Use current operation/attempt reads to inspect later facts; completion must not
retroactively turn the old receipt into confirmed cancellation. The direct manual method and
real durable-control integration are both tested.

`Executor.TimeoutBudgets` reports the frozen remote wall, setup, finalization and local control
invocation budgets. Provider enforcement stays unknown. A local invocation deadline, ERROR,
cancellation acknowledgement or an unfamiliar timeout-like string cannot become proven remote
`timed_out`/`cancelled` or hardware release. This task does not add an overall-job deadline
controller or alter M3's deadline/uncertainty semantics. Original runner/SDK budget enforcement
and live timeout acceptance remain separate evidence layers.

## Process and transport boundaries

The monitor embeds its own fixed helper and reuses the exact existing execution helper's
read guard/identity decoder in a fresh module namespace. The execution command entry point is
not run. The guard receives read-only mode, denies SaveKernel, and exposes no cancellation or
delete RPC. Public SDK request/response types remain in use; the private session seam and its
version-bound limitations are unchanged and documented in ADR-0015/0017.

The process uses isolated Python, an empty temporary home/cwd and restricted environment.
Tokens/requests travel through stdin, not command-line arguments or files. Local Python/client
metadata checks precede token access. The child has a 30-second parent budget and independent
25-second watchdog. Connect/read timeouts remain 5/10 seconds, with exact HTTPS RPC guards,
verified certificates, no redirects/retries/ambient proxies and at most ten guarded RPCs.
Normal quota/log reads use two/six RPCs respectively.

Token and request lines are capped at 8 KiB each, normalized stdout at 128 KiB and discarded
stderr at 16 KiB. Oversized output, nonzero exit after a plausible report, timeout, malformed
or contradictory protocol results expose no successful prefix or raw exception. Owned input
and output buffers are cleared after use; immutable strings/OS copies and arbitrary trusted
callbacks are outside a secure-erasure claim. This fixed leaf is not a general process-tree
supervisor; callbacks and OS operations must cooperate with cancellation.

## Verification

With the pinned toolchain, from the repository root:

```text
go test -race ./internal/provider/kaggle
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

In `tools/kaggle-client`:

```text
uv run --locked python -m unittest discover -s tests -v
```

Go tests cover capability independence/no live inflation, manual cancellation without I/O,
timeout separation, exact rounding, reservation-aware scheduler decisions, freshness/exhaustion,
strict protocol, credential order/rotation/cancellation, serialization, log cursor identity,
changed/empty/unavailable/truncated logs and real isolated-process bounds. Real SQLite tests
exercise `RecordQuota`, exhaustion through unknown/stale/restart, original cancel receipts and
manual activity retention followed by completion. These integration helper results are synthetic.

Python tests independently exercise raw duration fields, free-policy defaults, Unicode/byte
limits, redaction, local-before-stdin and watchdog behavior. The actual pinned SDK tests replace
HTTP transport only and cover quota precision, wrong accounts, request versions, source/privacy/
ID changes around log reads, empty/missing logs and failed/redirected/late responses. No provider
request is made. Original execution/runner sources, contracts, dependencies and workflows remain
unchanged; full existing Go/race/fault suites continue to run.

Local Python 3.13.5 ran the five pure protocol/watchdog roots; local Go 1.23.2 checked formatting.
A newer pinned-Go callback-format difference was corrected without changing assertions. These
are narrow local results, not full local SDK/Go 1.27.1/modernc integration. Exact-head native,
race and locked Python 3.11.16/Kaggle 2.2.4/SDK 0.1.35 CI evidence is recorded in merged PR #22.
M4-05 artifact-selection, transfer and publication evidence is recorded separately in PR #23
and the artifact guide; the historical M4-04 checks are not relabeled as artifact verification.

## Sources and next gate

Primary sources reviewed on 2026-09-16 at unchanged pins:

- [SDK quota/cancel/output request and response types](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py).
- [Official SDK service methods](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py).
- [Protobuf-duration and default-value conversion](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_object.py).
- [Official CLI log tests, including version-scoped output reads](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/tests/unit/test_kernels_logs.py).

These are source/fixture findings, not live account support. See
[ADR-0018](../decisions/0018-kaggle-operational-evidence.md),
[execution](kaggle-execution.md), [artifacts](kaggle-artifacts.md),
[scheduler](../scheduler.md) and [controls](../operations.md). The
[implementation plan](../implementation-plan.md) owns the current owner-review/next-task gate.
Full Provider/runtime registration, public log/quota/artifact routes, production `serve`,
remote cleanup and live acceptance remain separate tasks.
