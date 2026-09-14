# ADR-0010: Durable fairness and fenced local ownership are not remote submission

- **Status:** accepted
- **Date:** 2026-09-14
- **Task:** M3-03
- **Requirements:** OPS-01/02/03; preserve DUR-01/03 and DOM-02

## Context

M3-02 durably accepts immutable jobs and their first attempts. M3-03 needs bounded,
fair ownership of eligible preparation work without inventing M3-04's provider-resource
or submission-intent ledger. A crashed worker's lease expiry says nothing about remote
termination. Different configured instances may share the same operator account scope.

The approved proposal requires FIFO within a workspace, round-robin across eligible
workspaces, one potentially active attempt per account by default, explicit quota
uncertainty, and no automatic compute retry or provider fallback.

## Decision

Use the existing immediate SQLite transaction boundary and add migration 4:

- Enqueue each attempt with a monotonic integer sequence in its admission transaction.
  Backfill earlier attempts using persisted job insertion order and attempt number.
  Keep queue identity immutable and retain the round-robin workspace cursor durably.
- Read a transaction-consistent outstanding-attempt snapshot. Fail closed when it exceeds
  the scan bound rather than dropping old reservations. Select the oldest waiting head
  per workspace, then the first eligible workspace after the cursor, wrapping in stable
  lexical order. An already started attempt is not a waiting FIFO head.
- Atomically reserve worker/account capacity, assign owner/generation/random fence/expiry,
  update local state to `preparing`, append `scheduler.claimed`, and advance the cursor.
  Return a claim only after commit. No transaction includes a transfer or provider call.
- Resolve capacity from the job's frozen account/instance binding, never the current
  profile alias. A lease reserves local preparation; possible remote activity reserves
  account capacity independently of lease expiry. Inactive execution evidence can free
  account capacity while local result collection still occupies a worker.
- Reclaim only local queued/preparing/blocked attempts with no cancellation/deadline,
  remote activity or dispatch barrier. Preserve job/attempt/nonce and increment the
  fencing generation. Renewal/defer/release validate the entire stored ownership tuple.
  The old unfenced CAS port rejects an attempt once it has scheduler lease history.
- Latch a conservative dispatch barrier whenever an attempt leaves the safe local phase.
  It prevents accidental reset-to-queued replay, but is explicitly NOT a submission intent.
  No M3-03 method promotes a claim into provider staging or compute submission.
- Persist quota observations with their original units, precision, source and timestamps.
  Normalize only recognized seconds/hours. Retain known exhaustion through aging, reset
  timestamps and unavailable observations; clear it only on newer fresh positive evidence.
  Default uncertainty warns for bounded free-only work. Strict mode requires fresh,
  sufficient exact or conservative lower-bound observations.
- Store initialization/migration starts paused. Local operator composition explicitly
  configures the scheduler and starts a fixed-size local preparation pool. The pool does
  not execute uploaded commands. Both successful and failed local passes defer rather
  than fabricate `inputs.ready` or a remote outcome.

SQLite's [AUTOINCREMENT semantics](https://www.sqlite.org/autoinc.html) provide monotonic
committed queue identifiers; gaps are acceptable. Its [transaction semantics](https://www.sqlite.org/lang_transaction.html)
underpin atomic queue/lease/state/event/cursor writes. These are implementation choices,
not guarantees of exactly-once provider execution.

## Alternatives and consequences

An in-memory queue/cursor loses fairness and ownership on restart. Ordering only by wall
clock can reorder simultaneous admissions or follow clock corrections. Both are rejected.
The selected queue sequence adds a small persistent index and is not a priority scheduler.

Releasing account capacity solely on lease expiry risks duplicate remote work. Conversely,
retaining a local preparation reservation forever after a worker crash prevents recovery.
Separate local ownership from execution evidence, and fence every ownership mutation.

The worker pool uses a fixed goroutine count and will not replace a processor until it
returns. It cancels a timed-out processor, but cannot forcibly stop arbitrary in-process
Go code. A processor must honor context, isolate temporary work by claim/fence, and never
publish shared results without fenced orchestration. Shutdown waits for actual local
workers; there is no false bounded-shutdown claim for uncooperative code.

Account scopes remain explicit operator configuration, not verified provider account
identity or cross-installation coordination. Rounded quota is not an allocation guarantee,
and this task does not poll quota or estimate provider billing. Known exhausted capacity
requires new evidence; a guessed reset time cannot unblock it.

## Verification and follow-up

Unit/component tests cover fairness, multiple instances sharing an account, concurrent
claimants, frozen profile remapping, stale fences, rollback, disk-full, actual process kill,
restart, schema-3 backfill and missing/stale/exhausted quota. The finite scheduler smoke
uses real admission and SQLite but synthetic metadata, never provider compute.

Local Linux evidence uses the disclosed out-of-tree system-SQLite isolation harness;
production driver/domain/schema and native host integration are separate CI checks.
See [scheduler guide](../scheduler.md) for exact commands and limitations.

M3-04 must add fenced input readiness, provider-resource intent and submission/reconciliation
transactions before any worker can create remote side effects. M3-05 retains its explicit
control-operation gate. This ADR does not authorize either task or complete M3.
