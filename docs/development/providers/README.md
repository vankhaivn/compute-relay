# Provider references

Adapters translate provider-neutral ports into supported provider operations. They do not own
application authorization, the durable queue or permission to retry ambiguous compute.

| Reference | Purpose |
|---|---|
| [Provider contract](contract.md) | Required/optional ports, identity and outcome semantics. |
| [Kaggle preflight](kaggle-preflight.md) | Locked environment, explicit credentials and local/read-only checks. |
| [Private staging](kaggle-staging.md) | One-shot preparation, exact bytes and separately observed readiness. |
| [Execution](kaggle-execution.md) | Frozen source, submission authority and exact-version observation. |
| [Operational mappings](kaggle-operations.md) | Quota, log snapshots, cancellation and timeout evidence. |
| [Artifacts](kaggle-artifacts.md) | Versioned selected-file reads and immutable collection recovery. |
| [Acceptance runbook](kaggle-acceptance.md) | The fixed, explicitly authorized GPU experiment. |

Use the [operator checklist](../validation-checklist.md) for re-qualification.
Keep full operator run records private and use focused issues/PRs for sanitized failures or
support-changing evidence. Source/fixture evidence is not live account support. The normal server
remains [admission-only](../../status.md).
