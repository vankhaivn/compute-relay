# ADR-0032: Attempt-scoped ephemeral workload secrets

Status: Accepted

## Decision

Keep application workload secrets separate from provider login credentials and immutable job
inputs. Declare required named slots and their exposure policy in a gated job. Supply values
late through an execute-scoped, write-only API. Preserve only metadata, original receipts and
a keyed replay fingerprint in SQLite; keep the replay key in protected installation state and
unused values in bounded process memory.

Bind supply to the exact workspace, job, attempt and verified readiness revision. Consume its
one-shot authority atomically with the explicit start grant and submission intent. Process loss
before consumption requires explicit replay of the original supply; loss after consumption permits
only observation of the original remote identity. Revocation and cancellation cannot race past
the durable start barrier or restore material afterward.

The SSH adapter delivers once through fixed Docker exec stdin into a private bounded tmpfs.
A trusted finite bootstrap validates the complete packet before any user code runs. No automatic
redelivery follows uncertainty. Require an absolute start deadline no later than material expiry;
remove slot files before result export and let container termination remove the tmpfs.

## Rationale and consequences

Ordinary input bundles, job arguments and environment are retained for reproducibility and
inspection, so they are unsuitable for transient ingest credentials. Keeping a write-only supply
separate preserves those durable contracts and makes restart loss explicit. A keyed fingerprint
supports exact replay without persisting values or an unkeyed dictionary-testable value hash.
The installation key must therefore be restored consistently with the database.

This contract trusts the authorized workload and operator. File-only exposure minimizes routine
argument visibility; explicit argument exposure supports tools without a file interface. Neither
policy isolates user code from its own secret. Streaming log redaction and output scanning guard
against known-value disclosure, not deliberate encoding or exfiltration. Buffer clearing and tmpfs
do not promise physical erasure, immunity to swap, or protection from host/VM administrators.

Provider-neutral contracts advertise a separate capability. Managed SSH implements it first;
unsupported adapters reject it before remote effects. Secret-free jobs retain their existing path.
A new attempt after uncertainty still requires confirmed inactivity and fresh authority.

Protocol and limits: [workload secrets](../../workload-secrets.md),
[preparation gates](../../preparation-gates.md), [deadlines](../../execution-deadlines.md).
Docker's [tmpfs](https://docs.docker.com/engine/storage/tmpfs/) and
[exec](https://docs.docker.com/reference/cli/docker/container/exec/) documentation were reviewed
2026-10-12 for mount lifetime, swap limitations and stdin delivery semantics.
