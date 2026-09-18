# Provider contract

`internal/provider` defines provider-neutral finite-batch ports and explicit instance/snapshot
registries. `internal/ports` holds consumer-sized storage, blob, credential, clock and event seams.
Kaggle code does not enter the domain/HTTP contract. The fake provider supplies controlled offline
observations, not an executable local provider or evidence of live capability.

## Required and optional surfaces

The Provider interface has Describe, Check, Validate, Prepare, Submit, Observe,
ReconcileSubmission, ListArtifacts, FetchArtifact and Cleanup. Cancellation, logs and quota are
optional interfaces. Descriptors must match actual methods and independent evidence/conditions;
registries reject duplicate/invalid/nil bindings rather than choose an implicit fallback.

A ResolvedJob is a post-admission snapshot with original identities, frozen inputs/profile,
required capabilities and finite budget. Plans copy mutable data; preparation binds the complete
plan digest and operation identity. Preparation success is not private readiness. Adapters must
validate outcomes against the expected identity, not consume zero values as success.

| Submission outcome | Meaning |
|---|---|
| accepted | Validated exact remote reference; observe it. |
| rejected | Proven non-acceptance of this request with a structured reason. |
| unknown | Compute may have started; reconcile, never automatically resubmit. |

Not found is a lookup result, not proof of rejection. Only new durable orchestration intent
permits mutation; no port/SDK retry loop may bypass it. See [dispatch](../dispatch.md).

## Observation, artifacts and cleanup

References bind installation/workspace/job/attempt/instance/intent, resource key, nonce and
frozen digests. Observation must preserve exact resource/version identity and truthful state.
Absent optional values remain unknown/unavailable/manual-required; unsupported GPU cannot
become successful CPU fallback. Cancellation acknowledgement is not termination.

Artifact pages/transfers are exact-attempt scoped and bounded. Copy into unpublished destinations;
verify length/digest and successful EOF/acknowledgement before exposing bytes. Identity-only
manifest validation does not replace full schema, required-output, phase and GPU checks in
[collection](../collection.md). A fake's manifest/GPU claims are simulation data.

Cleanup is not cancellation. Port requests need an explicit mode, exact resource/creation
operation and actual verified ownership/retention evidence. A syntactically valid ledger ID
or prefix is insufficient. Current production-facing [retention](../retention.md) previews are
dry-run-only; the acceptance adapter cannot apply cleanup. Never infer deletion from a preview.

## Infrastructure and verification

Store transitions atomically compare revision/fence and sequence events. Event delivery is
post-commit notification, not a second authority. Blob interfaces accept workspace/object IDs,
not arbitrary paths. CredentialResolver scopes explicit transient values to a callback; no
provider secret belongs in a job or diagnostic. Callbacks must cooperate with cancellation.

`internal/provider/providertest` and fake fixtures exercise contract/fault behavior offline.
`go run ./cmd/provider-smoke` runs one finite synthetic lifecycle with no workload/provider network.
Use [validation](../validation-checklist.md) for live evidence, never point fake/contract
fixtures at an operator account. Current component composition is in [architecture](../architecture.md).
