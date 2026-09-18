# ADR-0009: Immutable idempotent admission

Status: accepted. Requirements: JOB-01/04, DUR-01/02, API-03.

## Decision

Validate the strict job contract and compute the named canonical request identity. Atomically
commit original job/attempt/nonce, frozen profile revision and input references, queue/event and
idempotency receipt before returning 202. Do not perform provider, URL or workload I/O in admission.
Pending URLs remain explicit source records until preparation freezes them.

After current authority checks, matching replay returns the original receipt before mutable
profile/input eligibility checks; changed content under the same key conflicts. Current GET
state is not the old receipt. Profile remapping cannot retarget accepted work, and missing/expired
original bytes cannot justify refreshing source data for retry.

## Reason and consequences

A lost response must not create another job or reinterpret an accepted request using today's
profile. Canonicalization avoids treating harmless encoding differences as new content while
retaining semantic conflicts. Admission is durable local acceptance, not exactly-once compute,
provider readiness or bundle execution. State/events still require transactional revision and
ownership checks. See [admission](../admission.md) and [application CLI](../../application-cli.md).
