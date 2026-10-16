# Remote attempt retention

Retention is an optional per-job instruction for Relay-created remote attempt data. It is frozen
with the admitted job and uses Relay's existing exact-identity cleanup operation, operation
receipt and resource checks. It does not expire Relay's verified local results or change local
retention windows.

## Declare a policy

Add at least one property in the job's optional `retention` object:

```json
{
  "retention": {
    "after_collection_seconds": 0,
    "uncollected_max_seconds": 86400
  }
}
```

`after_collection_seconds` accepts an integer from 0 through 2,592,000 (30 days). Zero makes
cleanup due immediately after Relay durably publishes the complete verified result set. A positive
value keeps remote data for that many seconds after publication.

`uncollected_max_seconds` accepts an integer from 1 through 2,592,000. It permits discard of
uncollected remote results only after the attempt is terminal and remote activity is confirmed
inactive, or the attempt never started. Its timer begins at the first such settled observation;
later polls and restarts do not reset it. A terminal execution with possible or unknown remote
activity does not start this timer and cannot be cleaned by this rule.

When both fields are present, the uncollected rule applies only while there is no verified
publication. After publication, only the after-collection rule applies. `retention: {}`, null
durations, zero uncollected lifetime, fractional values, and values above 30 days are invalid.

If the job has no `retention` field, Relay preserves today's behavior: it does not automatically
clean that attempt. Explicit cleanup remains available under the existing rules.

## Scheduling and receipts

Relay stores immutable due times for the two policy events: verified publication, and terminal
inactive/not-started execution. When a due rule becomes eligible, Relay admits an ordinary
attempt-cleanup operation with the frozen workspace, job and attempt identity. The uncollected path
sets the existing `discard_uncollected` authorization because the operator declared a maximum
uncollected lifetime at admission. Cleanup removes the exact attempt container and its per-attempt
staging through the existing engine. Shared cache volumes, Kaggle shared input datasets and
reusable application images have separate lifecycles and are not removed by attempt retention. No
separate remote deletion path is introduced.

The scheduler rechecks publication, remote activity, prior cleanup and operation state before it
commits the cleanup request. A busy or ambiguous prior cleanup is deferred. A confirmed deletion
or absence settles through its operation receipt. A failed confirmed refusal may be followed by a
new policy cleanup operation; an unknown or still-running deletion remains pending for inspection
and is never automatically replayed. A later preview reporting `unknown` does not reverse a
confirmed `deleted` receipt, including for providers such as Kaggle that return 403 for deleted
private resources.

Retention never stops running work, treats cancellation intent as proof of inactivity, or deletes
uncollected data while activity is possible. If an operation is failed, running or unknown, inspect
its receipt and resolve the original attempt before taking another action. See [remote cleanup](remote-cleanup.md)
for explicit preview and cleanup procedures and [recovery](recovery.md) for local result retention.
