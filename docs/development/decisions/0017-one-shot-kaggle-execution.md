# ADR-0017: One-shot execution and exact-source observation

Status: accepted. Requirements: PRV-02, DUR-03, DOM-02/03.

## Decision

Compose an immutable per-attempt Executor from the original plan, private ready staging and
frozen policy/account. Package exactly the five locked runner modules as inert source, binding
nonce, plan/input/staging and effective GPU policy. Use a stable intent-derived name rather than
content-derived replacement names. Never execute generated workload source on the control host.

A successful new M3 BeginSubmission commit is the sole durable mutation authority. Recheck original
staging; permit one private SaveKernel after genuine absence, observe exact existing resources
without updating them, and retain uncertainty after a possible effect. Recovery never resaves.
Validate raw numeric ID/version/source/account/privacy/resources around explicit-version status
reads. Missing/new status stays unknown; cancellation acknowledgements do not establish termination.
Terminal provider facts open collection, not final result success; release remains unobservable.

## Reason and consequences

SaveKernel is an upsert without exposed atomic create-only/CAS, so an external creation race or
unchanged same-version rerun is not ruled out. Postchecks cannot undo remote effects. Dataset
attachment is not an immutable mount. Point-in-time free-only checks are not reservations.
Source reconstruction depends on original binary/bootstrap/runner/configuration: retain them for
in-flight recovery, fail closed on change, and do not claim transparent cross-version recovery.

See [execution](../providers/kaggle-execution.md), [runner](../../../runner/README.md) and
[recovery](../../recovery.md). Fixed isolated helpers retain explicit transport/lifetime limits.
