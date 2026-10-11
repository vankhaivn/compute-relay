# Scheduling, dispatch and recovery

The dispatch engine selects queued work fairly, freezes original inputs and coordinates provider
work under durable ownership. The explicit and managed serve modes start it; admission-only serve
does not. Workload commands never run on the control-plane host.

## Fair selection and fenced claims

SQLite records FIFO order within workspaces, round-robin fairness across workspaces, account
policy, quota observations and worker leases. Selection, reservations, claim, state/event and
fairness cursor commit atomically. Account scope comes from the accepted binding, so aliases or
multiple workspaces cannot multiply the same account capacity.

Claims carry owner, generation, unpredictable fence, expiry and original queue/attempt identity.
Reclaim is limited to safe local/recovery work, and stale owners cannot publish callbacks after
another owner takes over. A claim is not authority to submit remote compute; leases bound
cooperative local ownership, not remote session lifetime.

Missing, unavailable, stale or lower-bound quota is an explicit observation, never invented zero
or allowance, and a fresh positive observation is not a reservation against external account use.
Possible, active or ambiguous remote work retains account capacity independently of lease expiry;
only proven inactive execution evidence releases it. Kaggle rounding and reservations are in
[operational mappings](providers/kaggle-operations.md).

## One-way gates

```text
fenced claim -> freeze/verify inputs -> verify frozen provider binding
 -> commit preparation intent -> one Prepare -> observe private readiness
 -> commit submission intent -> one Submit -> observe/reconcile same attempt
 -> terminal evidence -> collection (not yet verified results)
```

Only an invocation receiving a successful **new** begin-intent commit may perform that mutation.
A crash after commit but before the provider call is deliberately conservative: recovery observes
the original identity even when the effect might never have happened. A not-found response does
not rearm a potentially accepted request. Adapters must not hide mutation retries.

| Journal boundary | Permitted behavior |
|---|---|
| local | Freeze and verify original inputs; begin preparation once. |
| staging | Observe original preparation; do not repeat Prepare. |
| ready | Recheck policy and commit first submission intent. |
| submitting | Reconcile original submission; do not repeat Submit. |
| submitted | Observe the exact persisted resource/version. |
| rejected / failed / attention / prevented | Do not automatically reopen mutation; inspect explicit control eligibility. |
| collectible | Transfer-only verification/publication handoff. |

A rejection journals Relay's own problem for the code. Of the adapter's problem only a closed
`details.reason`, a short lowercase identifier, survives, and job status exposes that one detail.

## Identity and observations

Pending HTTPS roles become immutable object pins through guarded ingestion. Already frozen
roles reuse original bytes, never changed source URLs. Bundle/runner compatibility and file
length/digests are checked before mutation. Provider plans contain object identity, not source URLs.

Resolve original profile/revision/instance/account/credential-reference snapshots, not a current
alias. Adapters verify the effective account. Same-account credential rotation is not account
fallback. Credentials are not stored in the journal or passed to workloads.

Journal/state/events and claim-fence checks share short transactions; no transaction spans
provider or blob I/O. Resource ownership is recovery evidence, not unconditional deletion authority.
Observations must match resource/version/identity and valid timing. The latest raw unknown can
be retained while confirmed attempt state remains stronger. Stale polls cannot undo terminal facts.
A submitted remote run is observed at most every 15 seconds; cancel and reconcile requests still
wake it at once. Five consecutive unresolved outcomes stop for attention; possible activity and
capacity remain retained. Retries double the poll delay up to two minutes. After submission they
start from 30 seconds, so the attention threshold spans about seven minutes rather than a brief
outage.

Terminal provider evidence opens collection with unavailable results. Only the collector validates
manifest/declared output bytes and publishes final result availability. Hardware release remains
whatever the adapter can actually observe.

## Controls and lifetimes

[Cancellation](operations.md) may prevent dispatch before submission intent; after a possible
submission, intent/acknowledgement is not termination. Explicit reconcile only observes; compute
retry creates a new attempt using original frozen inputs, never resets the old journal. Collection
retry does no compute. [Retention](retention.md) preserves active/ambiguous/recovery references.

Invocation deadlines/poll backoff are local budgets, not an overall provider-session watchdog.
Bounded workers run orchestration callbacks, which must cooperate with cancellation; stalled
callbacks do not cause unbounded replacement workers. Shutdown joins them before releasing owned
stores, and stopping a local engine does not cancel or delete remote resources. See [recovery](../recovery.md) and
[Kaggle execution](providers/kaggle-execution.md) for safe actions and provider-specific limits.
