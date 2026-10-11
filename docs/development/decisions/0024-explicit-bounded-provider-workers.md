# ADR-0024: Explicit bounded provider workers in the normal runtime

Status: accepted. Refines ADR-0021.

## Decision

Keep ordinary `serve` local-admission-only unless the operator supplies the complete provider
configuration and explicit mutation authorization at process startup. In that explicit mode:

- resolve one immutable admission profile to one exact provider instance/revision/account/credential
  snapshot; never fall back to another alias, account, provider, CPU or paid capacity;
- require explicit private-staging and GPU flags plus a finite per-process number of distinct
  attempts that may receive new provider mutation authority;
- verify the credential and account read-only before enabling workers, and require known free GPU
  quota before a new GPU dispatch;
- compose the existing durable dispatch and collection engines with one worker each, writing
  preparation/submission intent before every remote mutation and recovering by observation;
- once the budget is consumed, stop claiming queued jobs while still observing and collecting
  attempts already started.

The HTTP mode reports whether the process runs workers. Installation state does not persist a
permanent dispatch switch.

## Reason and consequences

An admission profile alone is not authorization to consume quota, GPU capacity or create remote
resources, and ambiguous submission must not become permission to submit again after restart.
This mode gives one runtime/API path from upload and admission to verified artifact publication
while the default stays effect-free toward providers.

The process budget is an operator limit, not a quota reservation or exactly-once guarantee. Local
shutdown is not provider cancellation, and remote cleanup apply stays unavailable until it has its
own exact-target procedure. Offline and native tests do not transfer live evidence to other
accounts or workloads.
