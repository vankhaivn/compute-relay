# Durable scheduling and local preparation ownership

> **Task:** M3-03; implemented offline, PR #13 merged.
>
> A claim grants bounded local preparation ownership, not permission to submit compute.
> M3-04 now supplies separate staging, frozen-input and write-ahead submission components.
> No production `serve` command starts this scheduler implicitly.

## Components

`internal/scheduler` owns pure selection/quota policy, the consumer-sized repository
contract and an explicit fixed-size local preparation pool. `internal/store/sqlite` owns
queue order, fairness cursor, account policy, quota snapshots and fenced leases. Migration 4
adds these records without modifying migrations 1–3 or accepted job/attempt identities.

An attempt insert enqueues in the same admission transaction. An idempotency replay does
not insert an attempt or queue entry. M3-05's explicit compute retry atomically creates one
new attempt and queue entry; it does not reset the source attempt or its dispatch barrier.
Existing admission limits remain 100 outstanding jobs per workspace and 1,000 globally by
default; the scheduler does not accept unbounded work.

## Selection and capacity

FIFO means start order within a workspace, using committed queue sequence, not wall-clock
sorting. A waiting head blocked on capacity/quota/local backoff is not overtaken by a later
job in that workspace. An already leased attempt has started and does not block another
start when effective capacity permits it.

Among eligible heads, select the workspace after the last committed cursor in stable
lexical order, wrapping when necessary. The selected cursor survives restart. A failed
transaction advances neither cursor nor state/lease/event.

Capacity uses the account scope and instance in the accepted job's immutable profile
revision. Remapping a profile affects future admissions only. Two instances with the same
account scope share capacity; different runtime installations do not coordinate. The scope
is operator configuration and must not be described as a provider-verified account identity.

A live local preparation lease reserves a worker and account slot. Submitted, ambiguous,
running, cancelling or otherwise possibly active attempts keep account capacity even when
the lease expires or the local deadline passes. Inactive execution evidence releases remote
capacity; a still-running local collection worker can remain counted separately.

## Local defaults

| Setting | Default |
|---|---|
| New/migrated database | Scheduler paused until explicit local configuration |
| Local workers | 4, hard configurable range 1–64 |
| Active reservations per account | 1, effective minimum of global and account-specific limits |
| Local lease duration | 30 seconds |
| Fresh quota age | Less than 5 minutes |
| Deferred local preparation recheck | 5 minutes |
| Outstanding-attempt scan ceiling | 100,000; fail closed, never select from a truncated view |

These are connector bounds, not provider concurrency or performance claims. Policy changes
and pause affect new claims; they do not cancel active work, delete attempts or release
possible remote capacity. The internal inspection view reports waiting-head reasons and
quota warnings without making a provider call. No public scheduler/administration HTTP
endpoint is added by this task.

## Claim lifecycle

```text
Read committed queue, state, profile/account policy and quota
  -> select one eligible workspace head
  -> reserve worker/account capacity
  -> persist owner + generation + random fence + expiry
  -> persist preparing state + scheduler.claimed + fairness cursor
  -> commit
  -> return local ownership claim
```

Renewal, defer and cooperative release must present the exact workspace/job/attempt,
queue sequence, owner, generation, fence, expiry and attempt revision. Expired/replaced
claims fail. Reclaim increments generation but preserves attempt ID, attempt number and
nonce. Release retains lease history so an old generation cannot become valid again.

Defer writes `blocked`, a bounded not-before time and `scheduler.deferred` with lease
release in one transaction. Cooperative release writes `scheduler.released`; it does not
claim cancellation or inputs readiness. Reclaim/release can add ownership events without
changing the state revision when the state itself did not change.

A monotonic dispatch barrier prevents an attempt that left the safe local phase from being
reclaimed automatically, even after an accidental reset to queued. This barrier is distinct
from the M3-04 remote submission-intent ledger. The legacy unfenced `CommitAttempt` rejects
any attempt with scheduler lease history; M3-04 supplies fenced orchestration mutations.
M3-05 controls preserve those fences and never use cancellation or retry to rearm an
ambiguous submission.

Stored clock watermarks reject backwards time rather than resurrecting old ownership.
Production callers use the scheduler service's serialized clock/repository boundary;
explicit timestamps exist for deterministic internal tests, not application requests.

## Worker and shutdown boundary

`RunLocal` creates a fixed number of goroutines and accepts only a trusted runtime
`PrepareLocal` implementation. The implementation must honor cancellation, use bounded
local work, isolate temporary state by claim/fence, and make no provider mutations or
uploaded-command execution. Fencing is not a sandbox against malicious in-process code.

An expired worker is cancelled but is not replaced until its callback returns. Shutdown
stops new claims and waits for local callbacks; a callback that ignores cancellation can
therefore delay shutdown. The pool does not advertise an impossible forced Go-goroutine
kill. Stopping it does not cancel remote compute.

The local-only pool defers after either local success or failure. It never guesses that
bundle/input/provider-readiness gates are complete. Actual preparation and provider intent
coordination are explicitly composed through [M3-04 dispatch](dispatch.md), not optimistic
placeholder callbacks in `RunLocal`.

## Quota policy

Preserve `known`, `unknown`, `stale` and `unavailable`, source, observation/reset timestamps,
units and precision from the existing provider contract. Recognized seconds/hours can be
compared with the finite remote wall request. Unknown units cannot prove sufficiency.

Known zero allowance blocks. Exhaustion remains latched through stale observations,
transport failures or a reset timestamp passing; only newer fresh positive evidence clears
it. Fresh known allowance below the requested wall budget blocks conservatively. Default
missing/stale/unavailable/rounded observations allow eligible bounded free-only work with a
warning; strict mode blocks until fresh sufficient exact/lower-bound evidence exists.

No quota probe, entitlement constant, account rotation, automatic compute retry or billing
estimate is introduced. This is a decision over supplied observations, not a guarantee that
a provider will accept a job or reserve the reported time.

## Verification

With the repository's pinned toolchain:

```text
go test -race ./internal/scheduler ./internal/store/sqlite
go test -count=25 ./internal/scheduler ./internal/store/sqlite
go test ./cmd/schedulersmoke
go run ./cmd/schedulersmoke
```

The finite command uses temporary SQLite, real local admission/authentication, synthetic
object metadata and two workspaces sharing one account scope. It checks capacity,
`a -> b -> a` selection across restart, same-attempt reclaim and stale-fence rejection.
Its clock is explicitly advanced for expiry; it reports metadata-only evidence and zero
provider/payload calls. It does not check blob bytes or GPU execution.

The engineering Linux runtime had Go 1.23.2 and system SQLite 3.46.1, without toolchain/module
download access. New scheduler/SQL code was tested in an external isolation harness with a
non-shipped CGo adapter and reduced domain/admission/store fixtures. Vet, race tests,
25 repetitions, 8,759 bounded selection fuzz executions and a compiled SQL test executable
passed there, including actual process kill before/after commit and disk-full rollback.
This does not verify the production modernc driver, full domain/auth/schema integration
or the real schema-3 upgrade. The command and real upgrade test are covered separately by
the existing pinned-driver/native offline CI; they are not claimed as locally executed.

PR #13 records exact CI runs and source-identity checks. No harness, test database,
generated token, alternate driver or replacement directive is shipped. Actions remain
offline quality/build/unit/component/contract tests, with no runtime deployment.

See [ADR-0010](decisions/0010-fair-scheduling-and-fenced-local-claims.md),
[admission](admission.md), [storage](storage.md), [dispatch](dispatch.md) and
[operations](operations.md). M3-04 is merged and M3-05 is in review in PR #15;
collection, retention/cleanup and complete orchestration remain separate gates.
