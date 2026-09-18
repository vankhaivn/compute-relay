# ADR-0016: One-shot private staging and separate readiness

Status: accepted. Requirements: DAT-04, OPS-05, SEC-03; preserves DUR-03.

## Decision

Use the existing provider Plan and M3 preparation/ownership journal, not a second staging
subsystem. Only a new BeginPreparation commit grants one creation invocation. Derive resource
names from stable attempt/operation identity and bind contents in a separate marker; changed
contents conflict under the same name instead of selecting a new resource.

Verify original local bytes/EOF/Close before credential access and again during upload. Require
an explicit successful-source trailer before private dataset creation. Only genuine exact-resource
HTTP 404 can enter new creation; no upload/create/version retry after ambiguity. Preserve license
rights and explicit privacy, never public fallback.

Readiness requires exact ID/ref/version/marker/license, explicit private READY state, complete
pagination and marker-first/all-file hash verification followed by metadata/state recheck. Pin the
first discovered identity and reject replacement. Reconciliation is read-only, including not found.

## Reason and consequences

A create receipt cannot establish readiness or original bytes. Pipe EOF can follow a producer
error; names alone cannot prove ownership. Conservative recovery can retain unresolved staging
and partial upload storage. No automatic cleanup or provider reclamation guarantee is supplied.
Readiness is not an atomic provider mount/lock or compute permit. See
[staging](../providers/kaggle-staging.md) and [execution](../providers/kaggle-execution.md).
