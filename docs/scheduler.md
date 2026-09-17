# Scheduling and fenced local ownership

The scheduler selects local work fairly and reserves conservative capacity. It is an explicitly
composed service, not automatically started by normal `serve`. A claim is not authority to submit
remote compute; [dispatch](dispatch.md) supplies the separate write-ahead mutation gate.

## Durable selection

SQLite records FIFO order within workspaces, round-robin fairness across workspaces, account
policy, quota observations and worker leases. Selection, reservations, claim, state/event and
fairness cursor commit atomically. Account scope comes from the accepted binding, so aliases or
multiple workspaces cannot multiply the same account capacity. Initialization starts paused.

Claims carry owner, generation, unpredictable fence, expiry and original queue/attempt identity.
Reclaim is limited to safe local/recovery work. Stale owners cannot publish callbacks after
another owner takes over. Once scheduling ownership applies, legacy unfenced transitions cannot
bypass it. Leases bound cooperative local ownership, not remote session lifetime.

## Quota and activity

Missing/unavailable/stale/lower-bound quota are explicit observations, not invented zero or
allowance. Apply the configured uncertainty/strict policy; a fresh positive observation is not
an atomic reservation against external account use. Durable exhaustion is not cleared by missing
or stale evidence. Numeric rounding/reservations for Kaggle are in
[operational mappings](providers/kaggle-operations.md).

Possible, active or ambiguous remote work retains account capacity independently of local lease
expiry. Pausing new dispatch does not erase intent or prohibit safe observation. Proven inactive
execution evidence, not elapsed local time, resolves remote capacity. Retention likewise cannot
use lease expiry alone to delete recovery material.

## Workers and shutdown

Workers form a bounded pool. They run orchestration callbacks, not admitted workload commands
on the host. Callbacks must cooperate with context cancellation; stalled callbacks do not cause
unbounded replacement workers. Shutdown joins work before releasing owned stores. These rules
do not establish remote termination or hardware release.

Use [recovery](recovery.md) for uncertain work, not manual queue/intent SQL edits. General production
worker lifecycle and operator scheduling surfaces remain [planned](implementation-plan.md).
