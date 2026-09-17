# ADR-0021: Local installation and admission-only HTTP lifecycle

Status: accepted. Requirements: API-01/03, DX-01; preserves SEC-02 and durable ownership.

## Decision

Expose local initialization, workspace/token administration and schema validation through the
main binary. Bind SQLite and separate input/result root identities in a private installation marker.
Only init creates new state; other commands reject missing/corrupt/replaced markers instead of
silently reinitializing. Local commands and serve hold exclusive store/root ownership.

Deliver expiring scoped application tokens once to new private files, never stdout. Database
insertion and file delivery are separate effects; failed delivery attempts independent-context
revocation and reports retained uncertainty. No provider credential is involved.

Compose guarded literal-loopback HTTP services with durable stores, without provider/scheduler/
collector/sweeper workers. Report local-admission-only readiness explicitly. Shutdown stops new
handlers, then joins admitted handlers before store closure and lock release, even after forced
connection close. A local schema check neither admits nor checks provider availability.

## Reason and consequences

Silent state repair, memory fallbacks, leaked token output and early lock release misrepresent
safety. The explicit local surface is useful before full worker integration but cannot execute
queued jobs. Stop serve for administration; callbacks must cooperate. See
[local runtime](../local-runtime.md), [storage](../storage.md) and [current status](../status.md).
