# Preparation, dispatch and recovery

The explicitly composed dispatch engine freezes original inputs and coordinates provider work
under durable ownership. Normal `serve` does not start it. Workload commands never run on the
control-plane host.

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
Repeated unresolved outcomes stop for attention; possible activity and capacity remain retained.

Terminal provider evidence opens collection with unavailable results. Only the collector validates
manifest/declared output bytes and publishes final result availability. Hardware release remains
whatever the adapter can actually observe.

## Controls and lifetimes

[Cancellation](operations.md) may prevent dispatch before submission intent; after a possible
submission, intent/acknowledgement is not termination. Explicit reconcile only observes; compute
retry creates a new attempt using original frozen inputs, never resets the old journal. Collection
retry does no compute. [Retention](retention.md) preserves active/ambiguous/recovery references.

Invocation deadlines/poll backoff are local budgets, not an overall provider-session watchdog.
Bounded workers join cooperative callbacks before shutdown; stopping a local engine does not
cancel or delete remote resources. See [recovery](recovery.md) and
[Kaggle execution](providers/kaggle-execution.md) for safe actions and provider-specific limits.
