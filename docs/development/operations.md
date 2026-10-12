# Explicit attempt controls

The control service commits intent and immutable receipts; workers perform permitted effects
separately. HTTP handlers do not call providers or stream collection bytes. Admission-only serve
starts no provider/collection worker; explicitly configured provider modes do. See [application CLI](../application-cli.md) for command syntax.

## Request and receipt contract

| Operation | Meaning |
|---|---|
| cancel | Prevent new dispatch when possible, otherwise record an attempt-scoped cancellation request. |
| retry | Create one eligible new compute attempt from original frozen inputs/binding. |
| reconcile | Observe the original identity; never repeat preparation/submission. |
| collect | Create a transfer-only ticket after matching terminal execution evidence. |
| cleanup | Request deletion of the remote resources the adapter created for one settled attempt. |
| operation GET | Read current durable status without provider work, including cleanup receipts. |

POSTs require current workspace operate authority, explicit `attempt_id` and one idempotency key
of 8–256 printable non-whitespace ASCII bytes. Bodies are at most 4,096 bytes; optional non-secret
reasons at most 512 UTF-8 bytes. Retry requires a nonblank reason. Ambiguous/unknown/null fields,
invalid Unicode and duplicate keys are rejected. Reasons are not returned to applications.

A successful POST returns 202 and Location, even for an effect completed locally in the commit.
Replay returns the original receipt with `replay=true`; GET returns current state with
`replay=false`. Changed content under the same key conflicts. Current authority precedes replay,
while mutable target/input checks do not invalidate an already committed matching receipt.
The retry route's kind is `retry_compute`; source and `new_attempt_id` remain distinct.

## Cancellation and retry safety

Before submission intent, `dispatch_prevented` does not claim remote termination and does not
delete staging. After a possible submission, only a newly committed, capability-verified cancel
intent permits one adapter call. Lost acknowledgement or restart does not repeat it. Unsupported
capability or missing target remains unresolved/manual-required. Completion winning the race is
not relabeled cancelled. A terminal operation is historical; later execution facts belong to the
current attempt. Kaggle's current batch target requires [manual cancellation](providers/kaggle-operations.md).

Compute retry requires eligible terminal work with no possible/active unresolved remote execution.
It verifies original unexpired input bytes outside SQL, rechecks authority/eligibility during
commit and creates a new nonce/attempt/queue event atomically. No new URL fetch, remapped profile
or journal reset is allowed. Concurrent keys cannot create several retries from one source.
Successful execution with missing artifacts uses collect, not compute retry.

## Collection and recovery

Accepted collect means transfer requested, not available artifacts. Manual jobs require this
request for all results, including failure diagnostics; automatic collection remains the default.
Distinct pending keys coalesce, and already available results are acknowledged without refetching. [Collection](collection.md)
pins one result snapshot and publishes only after every selected file is independently verified.
An interrupted accepted ticket can resume after its lease; a committed failed ticket needs an
explicit new collect request/key. Original replay stays the original receipt. Neither path reruns
compute or replaces the original result pin.

Expired results retain historical receipts/publications but are not downloadable. Pending and
unresolved work stays pinned against cleanup. Interpret `safe_operation_retry`,
`compute_may_have_started` and `recommended_action` separately; never replace a missing response
with an automatic new key. See [recovery](../recovery.md) and [retention](retention.md).

## Remote cleanup

`POST .../jobs/{j}/cleanup` takes `attempt_id`, optional `discard_uncollected` (default `false`)
and an optional reason, under the same key, replay and coalescing rules. Acceptance only records
intent; `pending` is not deletion. Admission targets the attempt's staging ledger entry with a
recorded resource reference and refuses (409) while a scheduler or collection lease is held, a
control is accepted or running, remote activity is not inactive, execution with submission intent
is unknown or non-terminal, the submission journal is unresolved, or orchestration is not terminal
(`needs_attention` counts only with terminal execution and inactive remote activity). Unresolved
remote execution returns `REMOTE_EXECUTION_UNRESOLVED`. Results never collected and verified
locally require `discard_uncollected=true`; otherwise the 409 recommends collect. A prepared,
never-submitted attempt is eligible.

A pending cleanup blocks other controls on the attempt; a pending or succeeded cleanup blocks new
collection tickets. Explicit Kaggle and managed worker modes run a worker that commits `running`
under a fenced lease, rechecks eligibility, verifies the attempt's frozen binding and makes one
adapter apply call per pass; a request that is no longer eligible fails without a provider call. Succeeded receipts
report `remote_deleted` or `already_absent`; errors, including an adapter without cleanup support
such as Kaggle's today, become failed receipts that preserve the resources. If the process stops
after the provider call but before the outcome commits, a later pass may apply the same request
again after the lease expires; adapters report an already deleted resource as absent. A failed
cleanup never blocks a new request. Admission-only serve runs no worker. Nothing is deleted
automatically; see [ADR-0027](decisions/0027-explicit-remote-cleanup.md).
