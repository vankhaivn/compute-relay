# Architecture baseline

> **Status:** approved baseline with offline domain, contract, provider/fake,
> authentication, HTTP and filesystem-object components. Production composition and
> durable SQLite orchestration are not implemented yet.

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
admission | objects | jobs | operations | artifacts
          |
          v
Domain and orchestration
job/attempt state | scheduler | reconciliation | policy
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
semantics and the local executable smoke test. SQLite ownership, state locking and the
runtime composition remain later acceptance gates.

## Control plane and workload boundary

The local control plane handles credentials, state, transfers, orchestration, and provider calls. It never executes an uploaded business command locally as part of validation or dispatch.

A generic remote runner prepares the provider environment, verifies declared requirements, executes one explicit command, captures bounded logs and metadata, writes a result manifest, and exits. It does not receive runtime bearer tokens or provider account credentials and does not poll the local runtime for additional work.

## Core entities

- **Workspace:** application namespace and authorization scope for one trusted operator.
- **Job:** immutable requested business work plus frozen inputs and resolved profile snapshot.
- **Attempt:** one explicit compute execution; compute retries create new attempts.
- **Submission intent:** durable proof that a remote side effect may occur or may already have occurred.
- **Provider resource:** connector-owned remote identity tracked for recovery and cleanup.
- **Object:** immutable local input or code bundle.
- **Artifact:** verified output associated with one attempt.
- **Operation:** durable cancel, retry, reconcile, collect, or cleanup action.
- **Event:** sequenced state or operational evidence.

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
uses standard-library `net/http`; no new dependency is introduced by M2-05/M2-06.

## Architecture acceptance

A provider-neutral design is established only when a fake provider completes the generic lifecycle without importing Kaggle code and when adding a provider does not add provider-name branches to HTTP handlers or common state transitions.

A Kaggle adapter is established only after a supported private input → bounded execution → identified terminal result → verified artifact path is demonstrated and recorded. Offline architecture success alone is not live-provider evidence.
