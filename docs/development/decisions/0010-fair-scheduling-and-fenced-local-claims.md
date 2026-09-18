# ADR-0010: Fair scheduling with fenced local claims

Status: accepted. Requirements: OPS-01/02/03, DUR-01/03.

## Decision

Persist FIFO order within workspaces, round-robin selection across them, account/worker capacity,
quota policy and fenced leases in SQLite. Selection, reservations, queue/attempt state, event
and fairness cursor commit atomically. Use the original account binding, not a current alias.
Initialize paused; claim ownership alone cannot authorize remote submission.

Require owner/generation/random fence/expiry and original identity on callback publication.
Lease expiry permits only safe reclaim/observation, not clearing possible remote activity or
resetting a submission barrier. Unknown/stale quota follows explicit policy; an exhausted account
is not refreshed by missing data. Workers are bounded and join cooperative callbacks at shutdown.

## Reason and consequences

In-memory fairness and time-only lease recovery lose ordering or overcommit unresolved compute
across restart. Separate local ownership from remote activity and the later write-ahead mutation
gate. Conservative capacity can block work until evidence resolves uncertainty; a fresh quota
read is not an external reservation. See [scheduler](../scheduler.md), [dispatch](../dispatch.md)
and [operational quota](../providers/kaggle-operations.md).
