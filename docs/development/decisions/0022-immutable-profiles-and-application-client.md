# ADR-0022: Immutable profiles and explicit application requests

Status: accepted. Requirements: API-01/03, DX-01; preserves SEC-02 and receipt/binding identity.

## Decision

Provide local profile apply/show and workspace grant/revoke using immutable `(name, revision)`
snapshots, separate enablement and active alias selection. Reapplying identical content is safe;
changed policy/account/instance needs a new revision. Apply never grants access or starts workers.
Admission metadata and credential-reference syntax are not full runtime configuration.

Application CLI uses current private-file tokens and the running literal-loopback HTTP API,
not database access or provider credentials. Separate local schema validation from server contextual
validation. Require explicit keys and attempts for mutation/control; retry has a distinct new attempt.
Use fresh non-reusing transports without redirect/proxy/automatic replay. Upload verifies original
file, actually streamed bytes, receipt and final EOF/Close.

## Reason and consequences

Test-only SQL seeding is not an operator workflow; mutable profiles and implicit replay retarget
accepted work. Explicit commands preserve original receipts despite later remapping. Response loss,
late source failure or stdout error can follow committed effects: retain uncertainty, never claim
rollback or silently generate new keys. Upload lacks an idempotency-key guarantee. See
[application CLI](../../application-cli.md), [admission](../admission.md) and [recovery](../../recovery.md).
