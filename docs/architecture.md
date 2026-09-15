# Architecture baseline

> **Status:** approved baseline with offline portable-core, SQLite, durable admission,
> scheduling, one-shot dispatch/recovery, durable controls, verified collection and pin-aware
> retention/local sweep/remote cleanup previews. Production composition, artifact HTTP,
> remote cleanup apply and live evidence remain separate gates.

## System intent

Compute Relay is a local, operator-controlled control plane for finite jobs on external compute providers. Applications communicate through a small HTTP/JSON and binary-transfer boundary. The runtime owns durable admission, immutable input preparation, provider-specific submission, observation, recovery, and verified artifact collection.

Kaggle is the first provider to prove, not a domain dependency. A deterministic fake provider must exercise the same generic lifecycle in offline tests.

## Logical layers

```text
Application clients and CLI
          |
          v
HTTP API / authentication / workspace boundary
          |
          v
Application services
admission | objects | jobs | operations | collection | retention
          |
          v
Domain and orchestration
job/attempt state | scheduler | dispatch/reconciliation | policy
          |
          v
Ports
Provider | Store | BlobStore | CredentialResolver | Clock | EventSink
          |
          v
Infrastructure
Kaggle adapter | fake provider | SQLite | filesystem | subprocess boundary
```

The composition root selects concrete implementations. HTTP handlers must not contain provider scheduling logic. Provider adapters must not own the durable queue or mutate arbitrary domain rows.

## Implemented auth and object boundary

M2-05/M2-06 add `auth`, `api`, `objects` and `blobfs`. The HTTP layer authenticates and
bounds requests; the object service verifies authority and coordinates byte publication
with a separate metadata repository. The BlobStore port retains its M2-04 shape through a
shared `domain.ObjectMetadata` value.

Filesystem data and an identity manifest are flushed and atomically published together.
Ownership metadata is committed separately before a successful receipt. Failed or ambiguous
metadata acknowledgement retains complete blobs for reconciliation. Test support is
explicitly nondurable and is not wired into a production `serve` command.

See [ADR-0004](decisions/0004-workspace-auth-and-atomic-objects.md) and
[`auth-and-objects.md`](auth-and-objects.md) for concurrency, permissions, limits, failure
semantics and the local executable smoke test.

## Implemented SQLite foundation

M3-01 supplies concrete SQLite workspace, token-digest and immutable object-metadata
repositories behind those existing interfaces. `internal/statefs` owns the state-root OS
lock and private permissions, independently of the blob-root lock. Embedded checksum-bound
migrations and installation-identity guards reject incompatible or damaged state rather
than silently creating a new runtime identity.

The store uses bounded operations, one private connection, WAL/FULL durability and immediate
transactions. No SQL transaction spans byte transfer or provider work. Database-only
backups use consistent SQLite snapshots with receipt-last publication; offline restore
refuses existing destinations and preserves identity. Blob availability and complete
installation recovery are separate checks.

See [ADR-0008](decisions/0008-sqlite-durability-and-backup.md) and
[`storage.md`](storage.md). Repository availability alone is not a claim of complete
orchestration or production `serve` composition.

## Implemented durable admission

M3-02 adds `internal/admission` and SQLite migration 3. The application service parses the
existing embedded job schema and creates a versioned canonical request identity. The store
rechecks token/workspace authority and commits job, attempt/nonce, frozen profile revision,
object references, accepted event and original idempotency receipt together.

A replay recovers original IDs and resolution before considering current profile mappings
or limits, without bypassing current authorization. Direct HTTPS sources remain explicitly
pending in the durable request; byte inspection/snapshotting belongs to preparation before
dispatch. No admission path downloads data, contacts a provider or invokes the remote runner.

The existing attempt CAS port commits state and its next sequenced event atomically. Three
optional HTTP handlers expose create, no-compute validation and cached status. A nil admission
service does not substitute memory persistence. See [ADR-0009](decisions/0009-durable-idempotent-admission.md)
and [`admission.md`](admission.md) for normalization, limits and failure tests. M3-04 adds
preparation/dispatch separately; local admission idempotency is not exactly-once execution.

## Implemented scheduling and ownership

M3-03 adds a pure FIFO/round-robin selection policy and a SQLite-backed repository for
queue order, account policy, quota observations and fenced local leases. An attempt is
enqueued inside admission. Selection, capacity reservation, claim, preparing state/event
and fairness cursor commit atomically. Missing or ambiguous remote evidence retains account
capacity independently from worker lease expiry; account binding comes from the accepted
profile revision, not a current alias.

Migration 4 starts paused and preserves legacy acceptance order. Reclaim is restricted to
safe local preparation and retains attempt identity. Once a scheduler lease has existed,
the legacy unfenced CAS cannot bypass ownership checks. A fixed local worker pool is
explicitly composed and never runs uploaded commands or calls provider mutations. It does
not mark inputs ready merely because a local callback returned.

The conservative dispatch barrier is not a remote resource/submission-intent ledger.
M3-04 adds the separate fenced preparation/intent/observation transactions described below.
No scheduler administration HTTP surface or production `serve` wiring is introduced.
See [ADR-0010](decisions/0010-fair-scheduling-and-fenced-local-claims.md) and
[`scheduler.md`](scheduler.md) for quota uncertainty, worker limits, shutdown, migration
and local-versus-production test evidence.

## Implemented preparation and one-shot dispatch

M3-04 adds `internal/dispatch`, an explicitly composed engine using the existing scheduler,
blob store and provider-neutral ports. Pending HTTPS roles become immutable job-object pins;
bundle/input verification and provider calls run outside SQLite transactions. Resolved
provider plans contain object identities rather than source URLs and retain the accepted
configuration revision/account. An adapter must verify the effective binding and support
read-only staging reconciliation before it can be registered for this engine.

Migration 5 records bounded journals, exact provider-resource ownership and submission
intents. State/events, journal version and scheduler fence checks commit together. A new
preparation intent grants one Prepare invocation; a new submission intent grants one Submit
invocation. Restarted or uncertain callers observe the prewritten identity instead of
repeating either mutation. A not-found lookup cannot rearm the gate.

Recovery retains possible-activity capacity and can continue while new dispatch is paused.
Repeated unresolved outcomes become needs_attention with a sanitized cached status problem.
A terminal observation opens collection, never automatic job success. Resource records remain
ownership evidence; M3-07 only previews remote cleanup under its pin-aware policy. M3-04 itself
did not add cancellation or compute-retry controls; M3-05 adds those below. M3-06 adds the
separately composed collector; production server wiring remains separate. See
[ADR-0011](decisions/0011-one-shot-mutations-and-recovery.md) and [`dispatch.md`](dispatch.md)
for recovery trade-offs, exact tests and adapter obligations.

## Implemented durable controls

M3-05 adds `internal/operations`, SQLite migration 6 and five optional HTTP handlers. Every
control targets an explicit attempt and requires current workspace authority. Matching
idempotency replay returns an immutable acceptance receipt; current operation reads return
the latest revision. Operation, attempt, event, idempotency and any retry queue insertion
share the appropriate atomic transaction. HTTP handlers contain no provider calls.

Retry verifies the original frozen local bytes outside SQL and rechecks authority, source
eligibility and snapshot identity inside the commit. It creates a distinct attempt/nonce
without changing the immutable job, binding, input pins or historical attempt. Unresolved
remote activity and successful execution with missing artifacts cannot trigger compute retry.

Cancellation prevents new dispatch atomically where possible, including a terminal
`prevented` journal phase that cannot reopen submission. Otherwise it commits one exact-bound,
capability-verified cancellation intent before the adapter call. Ambiguous acknowledgement
is never automatically repeated; cancellation intent does not release possible remote
capacity or prove termination. Completion and cancellation observations remain separate
from immutable operation outcomes. Reconcile only observes existing identity.

Collect creates a durable transfer-only ticket after matching terminal execution evidence.
The M3-06 consumer below performs verification and publication separately; the handler does
neither. A `202` receipt is not result availability, remote termination or hardware release.
The strict public control view excludes reasons, provider references and arbitrary internal
problem details. See [ADR-0012](decisions/0012-attempt-scoped-durable-controls.md),
[`operations.md`](operations.md) and [API contracts](../api/README.md).

## Implemented verified collection

M3-06 adds `internal/collection` with a transfer-only engine and an authenticated internal
result reader. After matching terminal execution evidence, the engine verifies the original
provider binding, enumerates bounded pages and validates the runner manifest against the
frozen job, nonce, input digests and output requirements. An immutable per-attempt snapshot
is committed before output transfers. Remote paths never become host filesystem paths.

A dedicated collector-owned blob root uses the existing create-only filesystem store.
Independent size/hash checks and a successful provider acknowledgement precede transfer EOF.
Every complete blob is reopened and verified before publication. The engine's verification
value is bound to the current lease and snapshot; callers cannot assert verification through
an exported success flag. This is a trusted-process type boundary, not remote attestation.

Migration 7 persists collection-specific generation/fence/expiry/revision leases, immutable
snapshots and publications, and scoped artifact metadata. All file rows, result/attempt
state, sequenced events, safe cached conditions and operation completion commit together.
No SQL transaction spans provider or blob I/O. Uncommitted blobs remain recovery material,
not application-visible artifacts. Authenticated reads require an explicit attempt and
committed artifact ID; no artifact HTTP endpoint is added.

Restart after a durable pin reuses that pin and rehashed complete blobs. Lost publication
acknowledgements do not create a failure mutation or duplicate events. A committed transfer
failure requires another explicit collect request; no path repeats compute or refreshes a
pinned result. Provider wrapper success cannot override payload failure, and cancellation
or hardware release is never inferred from local download completion. Empty required
directories cannot prove presence with manifest v1 and fail closed; see
[ADR-0013](decisions/0013-verified-collection-and-publication.md) and
[`collection.md`](collection.md) for limits, recovery and exact test evidence.

## Implemented retention and cleanup preview

M3-07 adds `internal/retention` and additive migrations 8/9. Eligibility is assessed over all
references with manual, active, ambiguous, held-worker, pending-operation and recovery pins.
A metadata transaction commits irreversible expiry, audit and the rotating scan cursor.
Whole-result expiry updates only result availability, preserving execution and business
outcome, and appends one sequenced `result.expired` event atomically. Input/artifact metadata,
original receipts, publications and ownership history remain retained indefinitely.

The finite local sweeper binds separate persistent input/result blob-root identities before
using deletion tickets. It validates exact workspace/object metadata and actual bytes, then
quarantines and removes only committed tombstone targets. A replaced/swapped store cannot
inherit old authority. SQL acknowledgement follows filesystem removal; lost acknowledgements
recover through the tombstone, not another compute attempt. New admission/retry checks exact
unexpired input inventory before reading bytes and again in the commit; replay stays first.

Remote preview requires current workspace operate authority, an exact ledger entry and the
original verified provider/account binding. Terminal/publication/pin evidence and authority
are rechecked after the read-only provider call before storing its outcome. The wrapper forces
dry-run; no apply method is supplied. Staging records remain pinned with an explicit preview
limitation rather than being passed to the execution cleanup port. See
[ADR-0014](decisions/0014-retention-tombstones-and-cleanup-preview.md) and
[`retention.md`](retention.md) for windows, limits, backup identities and fault evidence.

## Control plane and workload boundary

The local control plane handles credentials, state, transfers, orchestration, and provider calls. It never executes an uploaded business command locally as part of validation or dispatch.

A generic remote runner prepares the provider environment, verifies declared requirements, executes one explicit command, captures bounded logs and metadata, writes a result manifest, and exits. It does not receive runtime bearer tokens or provider account credentials and does not poll the local runtime for additional work.

M2-09 implements this asset under `runner/python`, targeting one explicit Linux attempt.
Its versioned resolved manifest contains frozen identities and staged relative paths, not
provider configuration or source URLs. It verifies M2-07 bundle framing and input digests,
uses a replacement environment, supervises process groups, and writes the existing result
schema plus bounded provenance. No Go production component invokes it locally.

Network enforcement and provider termination remain adapter responsibilities. A runner
GPU check is not a guarantee that arbitrary payload code uses the GPU; a completed result
is not accelerator-release evidence. Synthetic CPU/process tests establish offline behavior,
while actual GPU, managed-package and provider integration require separate verification.
See [ADR-0007](decisions/0007-finite-remote-runner.md) and the
[runner guide](../runner/README.md) for exact contracts and limitations.

## Core entities

- **Workspace:** application namespace and authorization scope for one trusted operator.
- **Job:** immutable requested business work plus frozen inputs and resolved profile snapshot.
- **Attempt:** one explicit compute execution; compute retries create new attempts.
- **Submission intent:** durable proof that a remote side effect may occur or may already have occurred.
- **Provider resource:** connector-owned remote identity tracked for recovery and cleanup.
- **Object:** immutable local input or code bundle, with metadata surviving byte expiry.
- **Artifact:** verified output associated with one attempt and an atomic publication.
- **Operation:** durable cancel, retry, reconcile or collect action with immutable receipt and current status; remote cleanup previews are separate observations, not apply operations.
- **Event:** sequenced state or operational evidence, including irreversible result expiry.

## Non-negotiable invariants

- Admit asynchronous work durably before returning success.
- Freeze job specifications and resolved input bytes before remote dispatch.
- Resolve provider/profile selection explicitly; never use hidden fallback.
- Attribute every remote execution to one durable attempt and prewritten submission identity.
- Preserve `unknown` and ambiguous outcomes rather than guessing success or failure.
- Never automatically resubmit compute after an ambiguous outcome.
- Separate remote execution outcome, artifact availability, cancellation intent, and hardware-release evidence.
- Never report cancellation as confirmed without terminal evidence.
- Apply workspace authorization to every referenced object, attempt, artifact, event, and operation.
- Keep cleanup ownership-ledger based and separate from cancellation.
- Commit expiry before local deletion, preserve recovery pins, and never treat a dry run as remote deletion.
- Keep the reference workflow independent of maintainer-operated infrastructure.

## Initial technology boundaries

The approved defaults are:

- Go control-plane runtime and CLI;
- REST-like HTTP/JSON metadata with streamed binary transfer;
- SQLite for durable metadata and local filesystem storage for blobs/artifacts;
- compiled-in provider modules rather than a dynamic plugin ABI;
- official Kaggle client/CLI behind a narrow adapter boundary, with a small pinned Python bridge only when structured official APIs require it;
- Python and explicit remote Linux shell jobs for MVP; and
- direct installation as the primary path, with Docker optional.

Material decisions and unresolved implementation gates are recorded in the
[`decisions/`](decisions/README.md) index and implementation plan. The HTTP foundation
uses standard-library `net/http`. M3-01 pins the CGo-free modernc SQLite driver and matching
libc; exact versions and durability settings are in the storage guide and Go module files.
M3-02 reuses the already pinned JSON Schema validator for closed, embedded runtime validation.
M3-03 adds no dependency and makes no provider call while selecting or inspecting work.
M3-04 adds no dependency and uses only explicitly registered adapters for provider calls;
the supplied bound adapter is a nonexecuting fixture, not a live Kaggle implementation.
M3-05 through M3-07 add no dependency and preserve explicit provider-neutral composition.
The M2-09 runner and its unit tests use the Python standard library; the optional GPU
probe relies on the separately verified remote environment's PyTorch installation.

## Architecture acceptance

A provider-neutral design is established only when a fake provider completes the generic lifecycle without importing Kaggle code and when adding a provider does not add provider-name branches to HTTP handlers or common state transitions.

A Kaggle adapter is established only after a supported private input → bounded execution → identified terminal result → verified artifact path is demonstrated and recorded. Offline architecture success alone is not live-provider evidence.
