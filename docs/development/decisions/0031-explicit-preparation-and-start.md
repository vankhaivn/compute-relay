# ADR-0031: Separate preparation and start authority

Status: accepted (2026-10-12). Requirements: JOB-03, JOB-05, DUR-03, OPS-01, OPS-03, SSH-03.

## Decision

Offer immutable `execution_control=prepare_then_start` for managed providers advertising the
separate capability, initially SSH. Preserve combined authorization for existing clients.
Store distinct preparation and start authorities with immutable original receipts and one-shot
consumption. Readiness is a dated, nonexecuting observation bound to staged bytes, runtime,
manifest, container identity and policy. A public readiness read performs no remote work.

Bound preparation calls, ready lifetime, outstanding resources and staged bytes. Ready holds
release worker slots while retaining account capacity and remote storage reservations. Require
explicit cancellation and exact cleanup for unused resources.

Reverify readiness before the durable start barrier. Consume start authority and record intent
atomically, then write separate non-secret start control and start the original container.
The runner validates control against the original manifest and tightens bounds in memory.
Recovery after a possible write/start is observation-only and cannot repeat the command.

## Reason and alternatives

Time-sensitive applications need verified bytes before selecting their final execution window.
A combined staging/compute grant cannot express that boundary. An upload receipt cannot attest
remote readiness. Rewriting the job at start would discard immutable reconciliation evidence.
Separate controls retain the finite-job model and make abandoned storage accountable without
adding a persistent interactive session or background workload.

## Consequences

- Full readback before readiness and again before start adds bounded transfer and latency. It
  verifies actual bytes without executing code.
- A consumed start claim can remain uncertain after a crash before the command. Automatic retry
  would risk duplicate execution, so recovery retains the original identity.
- Expiry prevents future start but cannot prove possible earlier execution stopped.
- Missing or changed preparations require explicit replacement work. Exact cleanup can reclaim
  never-started containers without fabricating a result.

See [preparation gates](../../preparation-gates.md) for API shapes, limits and recovery semantics.
