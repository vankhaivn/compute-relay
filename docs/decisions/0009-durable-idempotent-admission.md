# ADR-0009: Commit admission identity and its original result atomically

- **Status:** accepted
- **Date:** 2026-09-14
- **Task:** M3-02
- **Requirements:** JOB-01/04, DUR-01/02, API-03
- **Related decisions:** ADR-0004, ADR-0008

## Context

A client can lose the admission response after SQLite has committed. Repeating the request
must recover the same job/attempt, not create new work. Profile remapping, concurrent callers,
workspace authority and later attempt transitions must not rewrite that original result.
Large byte transfers and provider calls cannot be held inside the acceptance transaction.

## Decision

Use one M3-01 bounded immediate transaction to recheck authority, inspect idempotency,
resolve/pin immutable references for a new job, and insert the job, first attempt/nonce,
accepted event and original receipt. Scope idempotency by workspace plus `job.create` plus
the key digest. Matching requests replay original IDs and timestamp with an explicit flag;
changed requests conflict. No automatic transaction replay follows uncertain commit.

Canonicalize against a named integer-only format, `compute-relay/job-request/v1`, rather
than claiming generic JSON canonicalization. Reject duplicate keys, malformed Unicode,
unknown fields, trailing values, excess nesting/size and fractional/out-of-range numbers.
Preserve array order and optional-field presence. Keep the canonical version with each job
and receipt identity so future changes cannot silently reinterpret old keys.

Use the already pinned Draft 2020-12 validator at runtime with only embedded authoritative
schemas and a closed loader. This changes that dependency's usage from development-only
to an admission dependency, not its version or license.

Profile revisions are immutable local non-secret resolution records. A matching replay
precedes current profile remapping and limits but never bypasses token revocation, expiry,
workspace disablement or scope. Preserve historical binding and data for future explicit
attempts; do not use profile changes as an implicit retry/provider switch.

Pin existing object metadata in the admission transaction. Preserve HTTPS source records
as pending in the canonical request; do not download during admission or claim verified
inputs from metadata alone. The future preparation path must freeze bytes before dispatch
and never silently refresh them for an explicit retry.

Implement the existing attempt CAS port with a state-plus-next-event transaction. Keep
operation-linked events deferred until the durable operation model exists. Add only the
create/validate/status HTTP slice; no scheduler, provider call or local workload execution.

## Alternatives rejected

An in-memory key cache fails across process restart. Separately writing idempotency after
job creation leaves a duplicate-admission window. Hashing raw JSON treats formatting as new
work, while decoding through floating-point numbers or last-key-wins parsing creates unsafe
identity ambiguities. Silent default insertion would also blur intentionally different
requests, so omitted fields remain significant in this version.

Resolving the current profile on replay would mutate accepted work. Performing input
transfers inside the transaction would hold the database lock across uncontrolled I/O.
Neither is part of this design.

## Consequences and verification

The system establishes local admission idempotency, not exactly-once provider execution.
`202` is durable queued metadata, not input readiness or compute eligibility. Historical
receipt state remains `queued`; GET reads current attempt state. Retention must preserve
keys at least as long as jobs, and backup restore cannot recover admissions newer than the
backup. Full dispatch/reconciliation remains separate work.

Migration 3 adds immutable profile revisions, jobs, attempts, pinned object references,
sequenced events and scoped idempotency; existing migration checksums are unchanged.
SQL constraints plus tests cover rollback at every insert, concurrent keys, foreign owners,
remapping, revocation, disk-full rollback, CAS/event atomicity, killed-process recovery and
lost HTTP receipt replay. Existing offline CI checks exact production dependencies and native
platforms. Limited local harness evidence is labeled separately in the [admission guide](../admission.md).
