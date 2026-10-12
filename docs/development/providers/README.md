# Provider references

Adapters translate provider-neutral ports into supported provider operations. They do not own
application authorization, the durable queue or permission to retry ambiguous compute.

| Reference | Purpose |
|---|---|
| [Provider contract](contract.md) | Required/optional ports, identity and outcome semantics. |
| [Kaggle client configuration](kaggle-preflight.md) | Locked environment, explicit credentials and the read-only account check. |
| [Private staging](kaggle-staging.md) | One-shot preparation, exact bytes and separately observed readiness. |
| [Execution](kaggle-execution.md) | Frozen source, submission authority and exact-version observation. |
| [Operational mappings](kaggle-operations.md) | Quota, bounded log replay, cancellation and timeout evidence. |
| [Artifacts](kaggle-artifacts.md) | Versioned selected-file reads and immutable collection recovery. |
| [SSH host](ssh-host.md) | Operator-owned Docker host: pinned login, per-attempt containers and exact cleanup. |

The explicit and managed serve modes compose the Kaggle components behind durable dispatch and
collection workers; see the [Kaggle runtime](../../kaggle-runtime.md) and
[ADR-0024](../decisions/0024-explicit-bounded-provider-workers.md). Source and fixture evidence is
not live account support; use the [validation checklist](../validation-checklist.md) to re-qualify.
Managed serve also composes the SSH host adapter; see [SSH hosts](../../ssh-hosts.md).
