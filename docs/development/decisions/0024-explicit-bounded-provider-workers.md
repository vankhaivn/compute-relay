# ADR-0024: Explicit bounded provider workers in the normal runtime

**Status:** accepted

## Context

The repository already had durable scheduling, fenced dispatch, one-shot provider intents,
restart reconciliation, verified collection and a live-qualified fixed Kaggle acceptance path.
The normal `compute-relay serve` composition deliberately omitted provider workers, which made the
main application surface admission-only even though the underlying components existed.

Making provider dispatch implicit would weaken the project's safety model. An admission profile is
not by itself authorization to consume provider quota, GPU capacity or create remote resources.
Ambiguous submission also must not become permission to submit again after restart.

## Decision

Keep ordinary `serve` local-admission-only unless the operator supplies the complete provider
configuration and explicit mutation authorization at process startup.

For the first integrated Kaggle path:

- resolve one immutable admission profile to one exact provider instance/revision/account/credential
  snapshot; never fall back to another current alias, account, provider, CPU or paid capacity;
- require explicit private-staging and GPU flags plus a finite per-process maximum number of
  distinct attempts that may receive new provider mutation authority;
- perform read-only credential/account verification before enabling workers;
- require known positive free GPU quota before a new GPU dispatch;
- compose the existing durable scheduler/dispatch and collection engines with one worker each;
- write durable preparation/submission intent before every remote mutation and recover by observing
  the original attempt after restart or response uncertainty;
- stop claiming new queued jobs after the finite process budget is consumed while preserving
  recovery/observation and result collection for attempts already started;
- keep local process shutdown separate from provider cancellation and remote cleanup;
- leave remote cleanup apply unavailable until it has its own reviewed exact-target procedure.

The active HTTP mode reports whether the process is local-admission-only or running configured
Kaggle workers. Installation state itself does not persist a permanent “dispatch enabled” switch.

## Consequences

A normal application can now use one runtime/API path from immutable upload and admission through
Kaggle execution to verified local artifact publication. The safe default remains effect-free with
respect to providers.

The integrated runtime reuses the same provider components exercised by the fixed acceptance path,
but implementation and offline/native CI do not automatically transfer the fixed experiment's live
evidence to arbitrary accounts or workloads. Provider/toolchain/environment changes still require
scoped re-qualification.

The first composition intentionally remains narrow: direct HTTPS input fetching, public provider
quota/log endpoints, remote cleanup apply, strict runtime TOML, doctor, broader packaging and
release hardening remain separate work. Finite process authorization is an operator budget, not a
provider quota reservation or exactly-once guarantee.
