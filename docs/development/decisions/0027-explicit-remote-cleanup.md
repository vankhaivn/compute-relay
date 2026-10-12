# ADR-0027: Explicit attempt-scoped remote cleanup

Status: accepted. Related requirements: OPS-05, PRV-01, API-03, DUR-01/02, DOM-01, VER-02.

## Decision

Add an explicit `cleanup` control that deletes the remote resources a provider adapter created
for one named attempt. It uses the existing control rules: operate scope, one idempotency key,
strict attempt-scoped body, original receipts, replay and coalescing of distinct pending keys.
The HTTP handler and admission transaction never call a provider.

Admission targets the attempt's exact staging ledger entry with a recorded resource reference.
It refuses an attempt with a held scheduler or collection lease, an accepted or running control,
non-inactive remote activity, unknown or non-terminal execution with submission intent, an
unresolved submission journal, or a non-terminal orchestration other than `needs_attention`
with terminal execution and inactive remote activity. Results that were never collected and
verified locally require `discard_uncollected=true`. A prepared attempt that was never submitted
is eligible. A pending cleanup blocks other controls on the attempt, and a pending or applied
cleanup blocks new collection tickets.

A separately composed worker claims the oldest accepted request under a fenced lease, commits
`running` before any provider call, resolves the attempt's frozen binding, requires binding
verification and makes one `Cleanup` apply call per pass with the ledger identity, its creation
operation and the local collection attestation. A submitted attempt is addressed by its recorded
execution reference; a never-submitted attempt by its prepared staging reference. `Deleted`
becomes `remote_deleted`, `AlreadyAbsent` becomes `already_absent`, and every error becomes a
failed receipt with a fixed public problem. Adapters without support return
`provider.ErrCleanupUnsupported`; Kaggle's runtime adapter does today.

Cleanup state lives in separate tables, so existing operation, ledger and receipt tables are not
rebuilt. Cleanup receipts share the operation identifier namespace and are read through the
existing operation route. See [controls](../operations.md) and [retention](../retention.md).

## Reason and alternatives

[ADR-0014](0014-retention-tombstones-and-cleanup-preview.md) kept remote work to a dry-run
preview because nothing could prove deletion was safe. Owners still need a way to remove an
attempt's remote data once they no longer need it. Reusing the preview as an apply permit was
rejected: its plan is an observation, not authority, and its port cannot reach staging. An
automatic policy was rejected for the same reason ADR-0014 gives: age is not ownership or
resolution evidence. Adding `cleanup` rows to the existing operations table would have required
rebuilding tables that other tables reference.

## Consequences

The preview stays dry-run only and nothing is deleted automatically. Repeat application is
possible when the process stops after the provider call but before the outcome commits; lease
expiry then lets a later pass apply the same request again, so adapters must report an already
deleted resource as absent. A failed cleanup never blocks a new explicit request or collection.
Admission-only serve accepts requests but runs no worker, so they stay accepted until a provider
mode starts. Kaggle requests fail clearly until its adapter implements a reviewed exact-target
delete. Live cleanup still needs the explicit authorization in the
[validation checklist](../validation-checklist.md).
