# Durable job admission

Admission validates an application request and commits its original receipt. It does not fetch
URL bytes, inspect the full bundle, call a provider, start workers or allocate compute.
Use [application commands](application-cli.md) for the local workflow.

## Atomic acceptance

The service parses the embedded strict job schema, rejects ambiguous JSON/Unicode, checks path
and budget semantics and computes a versioned canonical request identity. SQLite rechecks current
authority and atomically writes the immutable job, first attempt/nonce, frozen profile revision,
object references, queue entry, sequenced event and original idempotency receipt before 202.

Canonicalization is the project's named integer-only format, not a claim to implement RFC 8785.
Formatting/key order is not the request identity. Changed semantic content using the same key
conflicts. Keys are explicit, workspace-scoped, 8–256 printable non-whitespace ASCII bytes.

## Replay and profile changes

Matching replay returns original IDs and receipt before re-evaluating mutable profile mappings,
limits or input eligibility. It still checks current token/workspace authority; revocation does
not become valid because an old key is known. GET reads current job state rather than rewriting
the original acceptance response.

[Profile administration](application-cli.md) records immutable `(name, revision)` snapshots and
separate enable/grant controls. Future alias remapping cannot retarget accepted work. An admission
profile is local policy, not a verified provider instance or permission to execute remotely.

## Input and state boundaries

Existing input objects must belong to the workspace and be eligible under policy/retention.
Pending HTTPS references stay source records at admission. Preparation later freezes guarded
URL bytes and verifies bundle/input identities before any new provider mutation. Missing/expired
original bytes cannot be replaced by a new download during compute retry.

Attempt-state changes and their next sequenced events commit together with revision/ownership
checks. Scheduler/dispatch fences cannot be bypassed by an older unfenced caller. Metadata errors
or uncertain commit acknowledgement are not permission to create another job/key automatically.

Contextual validation reports remaining verification requirements without admitting work.
Schema-only local validation is narrower still. Normal serve remains admission-only, so a queued
job does not progress merely because all admission checks passed. See [scheduler](scheduler.md),
[dispatch](dispatch.md), [controls](operations.md) and [storage](storage.md).
