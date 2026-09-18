# Architecture

Compute Relay separates a provider-neutral application contract from durable orchestration,
provider adapters and filesystem storage. This reference describes responsibilities and invariants;
[current status](../status.md) says which compositions are available.

## Layers

```text
Application CLI / HTTP clients
        -> authenticated workspace API
        -> object, admission, control and result services
        -> domain / durable scheduler, dispatch and collection
        -> provider, store, blob, credential and clock ports
        -> Kaggle components / SQLite / filesystem / isolated helpers
```

Handlers do not schedule provider work or execute commands. Adapters do not own the durable
queue or edit arbitrary database rows. The composition root decides which services/workers run.

## Available compositions

The normal `runtimehost` always serves uploads, admission/status/control and reads of published
results with SQLite and separate input/result stores. With no provider flags it creates no provider
workers and remains local-admission-only. With an exact configured Kaggle profile plus explicit
private-staging/GPU authorization and a finite attempt budget, it additionally composes one durable
dispatch worker and one collector against the frozen provider snapshot. A local `202` remains an
admission receipt, not remote-success evidence.

The separate fixed Kaggle acceptance utility remains the narrower live-qualification workflow. It
uses the same reviewed provider components but a different state format and explicit
submission/read-only modes; do not interchange its root with a normal runtime installation. See
[acceptance](providers/kaggle-acceptance.md) and
[ADR-0024](decisions/0024-explicit-bounded-provider-workers.md).

## Responsibility map

| Boundary | Responsibility |
|---|---|
| [Auth and objects](auth-and-objects.md) | Current workspace authority, verified immutable bytes and separately committed ownership. |
| [Admission](admission.md) | Strict request identity, frozen profile/input references and original job/attempt receipt. |
| [Storage](storage.md) | Short atomic transactions, installation identity, migrations, OS locks and database-only backup. |
| [Scheduler](scheduler.md) | Fair queue selection, account/worker capacity and fenced local claims. |
| [Dispatch](dispatch.md) | Freeze inputs; commit preparation/submission intents before one permitted mutation; recover by observation. |
| [Controls](operations.md) | Explicit attempt-scoped cancel/retry/reconcile/collect receipts and current operation state. |
| [Collection](collection.md) | Immutable result pin, selected byte verification and atomic publication. |
| [Retention](retention.md) | Recovery pins, irreversible expiry, exact local deletion and remote dry-run limits. |
| [Artifact delivery](../artifact-delivery.md) | Authorized local reads and final acknowledged, create-only downloads. |

## Durable boundaries

No database transaction spans provider calls or blob I/O. Input bytes are published before
ownership acknowledgement. Job/attempt/event/receipt admission is atomic. Preparation and
submission ownership commit before external effects. Collection pins the original manifest/file
set before payload transfer, then publishes metadata/state/events only after complete verified
bytes. Expiry commits before local deletion. Cross-resource failures retain uncertainty instead
of pretending database, network and filesystem effects share one transaction.

Jobs and original receipts are immutable; explicit compute retry creates a new attempt with
frozen original inputs/binding. Current authority still gates replay. Cancellation, execution,
result availability and hardware release are independent. Unknown remote activity retains
capacity and recovery material; lease expiry is not proof that remote work stopped.

## Trust and extensibility

The operator, installed interpreter/packages and same-user code are trusted. Workspaces isolate
applications, not hostile tenants. Credentials remain behind explicit references; applications
receive no provider token. Host paths/remote URLs are not public artifact selectors.
Provider-specific SDK/state/transport behavior stays in adapters. Optional capabilities carry
support and evidence separately. No fallback account/provider/CPU/billing or ambiguous compute
replay is hidden below a port. See [domain model](domain-model.md),
[provider contract](providers/contract.md) and [decisions](decisions/README.md).
