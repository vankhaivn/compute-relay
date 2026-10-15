# SSH host storage budget and inventory

Relay tracks the Docker objects owned by one installation on one verified machine and engine.
Connection aliases to the same machine and engine share a storage scope and physical budget.
Kaggle does not expose this storage accounting feature.

## Budget and accounting

New SSH connection revisions default to a soft limit of 64 GiB and a hard limit of 80 GiB.
Adjust these limits to the Docker engine's available capacity and other workloads. Host filesystem
`df` is not a substitute for the engine's storage capacity.

Configure future SSH revisions with both byte limits:

```json
{
  "storage_budget": {
    "soft_bytes": 68719476736,
    "hard_bytes": 85899345920
  }
}
```

Both values must be positive integral byte counts, soft must be below hard, and hard cannot exceed
1 PiB. Omitting the pair freezes the defaults in the new connection revision. Accepted jobs keep
their selected revision. For aliases of the same machine, the effective soft and hard limits are
the lowest configured values among enabled verified connections; a permissive alias cannot
override a stricter one.

The machine charge counts each physical cache volume, derived image and attempt container once.
Cache bytes are charged once even when multiple workspaces reference them. Workspace `referenced`
bytes describe logical attribution and may sum above the physical machine charge. Image virtual
size is a conservative upper bound because Docker can share layers; container writable-layer bytes
exclude image and volume bytes. The read view reports precision and unknown sizes explicitly and
does not turn missing measurements into zero.

Above the soft limit Relay schedules least-recently-used eviction of eligible cache generations
and application images. It never evicts a pinned cache entry, an image referenced by active or
uncertain work, or an object still mounted by a container. Each exact removal must be observed and
receipted before its charge is released.

The hard limit gates new admission and preparation; it is not a quota on workload writes. Relay
refuses new work when known charge has reached hard, when known charge plus new reservations would
exceed hard, or when a fresh complete observation cannot establish the charge. Admission returns
HTTP 422 with the existing `RESOURCE_REQUIREMENT_UNSATISFIED` code and `validation` stage, with
`recommended_action: "wait"` and, when available, `details.reason` set to `host_storage_limit` or
`host_storage_unknown`. Dispatch leaves queued work held and retries after 30 seconds;
legacy health admission is unchanged. Reservations cover the next cache/image write and attempt
container. Already-prepared and active work may continue under its reservation. A running workload
may grow its writable layer beyond its admission estimate; Relay does not stop it for storage. A
later scan records the higher charge and blocks additional work.

## Read the storage view

`GET /v1/workspaces/{workspace}/connections/{connection}/storage` is read-only and uses a read
token. The connection resolves to the verified machine/engine, so aliases see the same machine
snapshot while the response includes the selected connection revision's configured limits.
The bounded response includes totals by class, workspace references, cache generations and pins,
pending retention deadlines and operation states, orphan summaries, foreign-installation counts,
budget state, observation time, scan state and precision. It never returns raw Docker
IDs, names, labels, credentials, host paths or another workspace's job identities.

`below_soft`, `above_soft`, `hard_refusal` and `unknown` describe the budget state. A stale,
truncated, timed-out or engine-changed scan is incomplete and cannot prove an object absent or
authorize eviction. `observed_absent` records an observation only; it is not a deletion receipt.

## Inventory and orphan cleanup

Relay scans containers, volumes and images with installation-label filters and bounded time, output
and record limits. It inspects only objects returned by the exact installation filter. Other
installation objects are represented only by per-class aggregate counts; unlabelled objects are
outside the inventory. Objects belonging to another installation are never inspected or changed.

An orphan is a labelled object with no matching durable record. Inventory can offer an opaque
orphan ID and eligibility summary, but never its Docker identity. Removing an orphan requires a
separate explicit preview and apply operation with a fresh idempotency key, a current matching
fingerprint, verified management authority and a second exact identity/reference check. The cleanup
worker records a receipt for the exact target. Attempt containers that match a job ledger use the
ordinary attempt-cleanup route instead. Missing ledger objects are recorded as observed absences,
not as completed deletions.

Use a `manage` token for orphan preview and apply. All paths below are relative to
`/v1/workspaces/{workspace}`:

1. Read `GET /connections/{connection}/storage` and select an eligible opaque orphan ID.
2. Preview with `GET /connections/{connection}/storage/orphans/{orphan}/cleanup`.
3. Before the preview expires, send `POST` to that same path with an `Idempotency-Key` and
   `{"fingerprint":"<fingerprint from preview>"}`. A changed scan or target requires a fresh preview.
4. Follow the returned `Location` at `/storage-cleanup-operations/{operation}` with a read token.
   Repeating the original key and request returns the original operation; an unknown deletion
   outcome never grants another automatic removal.

See [remote cleanup](remote-cleanup.md) for ordinary attempt cleanup. The
[storage response schema](../api/schemas/connection-storage.v1alpha1.schema.json) and
[example](../api/examples/connection-storage.example.json) document the bounded fields.
