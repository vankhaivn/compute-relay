# Architecture decisions

These records preserve accepted design choices, their reasons and consequences. They are not
implementation diaries or live-run reports. Use [current status](../../status.md) for current support
claims and focused issues/PRs for sanitized evidence that changes those claims.

All decisions below are accepted. Editing for clarity does not change their outcome; a material
new choice requires a superseding ADR. Historical wording and review discussion remain in Git.

| ADR | Decision |
|---|---|
| [0001](0001-official-kaggle-client-boundary.md) | Isolated pinned official client, narrow bridge and no hidden retries. |
| [0002](0002-attempt-scoped-kaggle-resources.md) | Attempt-scoped resources and manifest identity, without exactly-once claims. |
| [0003](0003-go-and-sqlite-baseline.md) | Go control plane and CGo-free SQLite. |
| [0004](0004-workspace-auth-and-atomic-objects.md) | Current authority, atomic blob bytes and separate ownership commit. |
| [0005](0005-bundle-format-and-rooted-import.md) | Strict bundle format and rooted explicit input selection. |
| [0006](0006-public-https-ingestion.md) | Per-hop public HTTPS/DNS/peer checks before immutable input publication. |
| [0007](0007-finite-remote-runner.md) | One finite remote runner, frozen inputs and bounded results. |
| [0008](0008-sqlite-durability-and-backup.md) | State identity, checksummed migrations and database-only backup. |
| [0009](0009-durable-idempotent-admission.md) | Original immutable admission receipts and frozen resolution. |
| [0010](0010-fair-scheduling-and-fenced-local-claims.md) | Durable fairness, shared-account capacity and fenced local claims. |
| [0011](0011-one-shot-mutations-and-recovery.md) | New write-ahead intent for each one-shot mutation; observational recovery. |
| [0012](0012-attempt-scoped-durable-controls.md) | Explicit attempts and original control receipts versus current state. |
| [0013](0013-verified-collection-and-publication.md) | Immutable result pins and independently verified atomic publication. |
| [0014](0014-retention-tombstones-and-cleanup-preview.md) | Pin-aware expiry before exact bound-store deletion; remote preview only. |
| [0015](0015-read-only-kaggle-preflight.md) | Local/read-only opt-in, explicit token scope and bounded SDK transport. |
| [0016](0016-private-staging-and-readiness.md) | One-shot private staging and complete separately observed readiness. |
| [0017](0017-one-shot-kaggle-execution.md) | Locked source and exact-identity execution under durable intent. |
| [0018](0018-kaggle-operational-evidence.md) | Conservative quota, bounded log snapshots and manual cancellation. |
| [0019](0019-version-scoped-kaggle-artifacts.md) | Explicit version/file reads and original-pin collection recovery. |
| [0020](0020-explicit-durable-gpu-acceptance.md) | Fixed authorized GPU experiment with a separate resume process. |
| [0021](0021-local-runtime-lifecycle.md) | Private local operator commands and admission-only HTTP lifecycle. |
| [0022](0022-immutable-profiles-and-application-client.md) | Immutable admission profiles and non-replaying application CLI. |
| [0023](0023-verified-artifact-delivery.md) | Current-authority downloads with final acknowledgement and create-only files. |
| [0024](0024-explicit-bounded-provider-workers.md) | Explicit finite provider authorization and durable workers in normal serve. |

## Add or revise a decision

Use `NNNN-short-name.md`, never reuse numbers, and start from [template](template.md).
Statuses are proposed, accepted, superseded by ADR-NNNN, deprecated or rejected. State the
material decision, alternatives/reason and actual consequences. Link the current detailed guide
rather than duplicating it. Keep tests/run outcomes in the relevant PR or issue, not a growing ADR audit. Do not create an ADR for routine formatting, naming or documentation cleanup.
