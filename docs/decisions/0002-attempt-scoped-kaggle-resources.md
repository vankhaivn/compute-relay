# ADR-0002: Use attempt-scoped Kaggle execution resources

- **Status:** accepted
- **Date:** 2026-09-13
- **Decision owners:** repository owner and implementation maintainers
- **Related requirements:** JOB-04, JOB-05, DOM-01, DOM-02, DUR-03, PRV-02,
  OPS-04, VER-02
- **Related evidence:** [`../research/kaggle-interface-review.md`](../research/kaggle-interface-review.md)

## Context

The reviewed Kaggle CLI describes kernel status and output in terms of a kernel's latest
run. Logs also resolve the current/latest session. Reusing one kernel slug for unrelated
attempts could let a delayed observer or collector associate stale/newer output with the
wrong local attempt.

Creating a unique resource does not make submission exactly once. A response can still be
lost after the provider accepts a push, and repeating that push could start another version.
The local system must retain uncertainty instead of turning identity isolation into a false
guarantee.

## Decision drivers

- Attribute every remote run and artifact to one durable local attempt.
- Make restart reconciliation possible from data persisted before submission.
- Avoid dependence on mutable human titles or a shared “latest run.”
- Detect manual edits/reruns rather than guessing which execution is authoritative.
- Preserve honest accepted/rejected/unknown submission outcomes.
- Support safe ownership-ledger cleanup.

## Considered options

### Option A — Reuse one shared kernel slug

This reduces remote resource count but makes latest-run observation, manual reruns, delayed
collection, and ambiguous submission unsafe.

### Option B — Unique resource per job

This isolates jobs but explicit retries still create multiple attempts whose latest-run
semantics can conflict.

### Option C — Unique resource per attempt with manifest verification

Each attempt receives a deterministic provider-safe resource identity before any remote
side effect. Retrieved results must carry matching local identity and immutable content
digests.

## Decision

Select **Option C** for the Kaggle MVP.

Before submission, persist:

- runtime installation ID;
- workspace, job, and attempt IDs;
- deterministic provider-safe kernel slug;
- non-secret random attempt nonce;
- bundle and input-manifest digests;
- resolved provider/profile/configuration revision; and
- a durable submission intent.

The remote runner result manifest includes the job ID, attempt ID, nonce, bundle digest,
input-manifest digest, runner version, timing, resource check, exit result, and artifact
digests.

Observation and collection rules:

- retain any stronger provider version/session identifiers as opaque attempt data;
- never accept output until the manifest matches the expected attempt identity;
- treat missing/mismatched identity as an explicit error/attention state;
- do not move a terminal attempt backward because a stale observation arrives;
- if a managed resource was edited or rerun outside the connector, stop automatic
  collection and require operator resolution;
- never automatically repeat a push after a timeout or unknown outcome; reconcile the
  prewritten identity first; and
- advertise durable admission and conservative reconciliation, not exactly-once execution.

Resource deletion is a separate ledger-backed cleanup action after terminal resolution and
safe collection. It is never used as cancellation.

## Consequences

### Positive

- Latest-run APIs are scoped to one intended attempt rather than unrelated work.
- Manifests prevent a stale artifact from satisfying success.
- Restart and ambiguous-submission investigation have a deterministic lookup key.
- Cleanup ownership can be tied to a single attempt.
- Explicit retries remain separate historical attempts.

### Negative or limiting

- The provider account accumulates more resources until explicit cleanup.
- Slug generation and collision handling need deterministic tests and provider length/format
  validation.
- Manual provider-side changes cause `needs_attention` rather than automatic recovery.
- Resource isolation still cannot prove exactly-once submission.

### Follow-up

- M1-04 verifies provider-safe naming and returned identities.
- M1-05 verifies manifest-bound observation and collection.
- M1-07 performs restart and ambiguous-response experiments.
- M3 persists submission/resource ledgers and fault-tests every crash boundary.
- M6 validates ownership-safe cleanup and retention.

## Verification

- Unit tests produce stable valid slugs and reject collisions/invalid account bindings.
- A fake-provider lost-response case never creates a second execution.
- A stale or wrong nonce/digest prevents artifact publication.
- Restart reconciliation locates the same attempt using only persisted pre-submit data.
- Manual rerun fixtures produce identity mismatch/attention, not success.
- Cleanup tests cannot delete a resource lacking an exact ledger identity.

## Rejected alternatives

Option A is rejected because it makes latest-run behavior unsafe. Option B is rejected
because retries are attempt-scoped and must not overwrite or confuse previous execution
history.
