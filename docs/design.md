# Design

Compute Relay is a finite batch control plane for one trusted operator's applications.
The application owns workload meaning and external sessions; Relay owns durable orchestration,
provider boundaries and verified result bytes. Go serves local HTTP/JSON; SQLite holds metadata;
private filesystem roots hold input/result bytes; the embedded Python runner executes remotely.

## Invariants

1. Admission commits local identity, not compute consent. Each attempt needs finite explicit authority.
2. Commit a one-shot mutation intent before a provider call. After uncertainty, observe the original
   resource only; never infer permission to repeat from a missing response or a not-found read.
3. Preserve original jobs, attempts, requests, receipts and resource evidence. An explicit retry
   creates a new attempt and requires new authority; it does not rewrite the earlier one.
4. Freeze provider, account, selection, policy, resource and input identities. Credential rotation
   must verify the same account. No provider/account/CPU fallback or automatic paid capacity.
5. Fence every worker claim and renew its lease. Stale owners cannot mutate durable progress;
   lease expiry alone does not prove a provider call or callback stopped.
6. Reserve shared account/machine capacity conservatively, including prepared holds and uncertain
   activity. Confirmed inactivity is independent of cancellation intent, local expiry and results.
7. Verify a complete bounded artifact catalog and exact manifest, bytes, EOF and source completion.
   Publish the whole result set atomically; complete temporary bytes alone are not results.
8. Credentials are write-only and isolated from jobs, logs, fixtures, SQLite and Git. Preserve the
   selected vault and native identity; access failure never creates a backend fallback.
9. Host keys, native machine/Docker identity and frozen container/image policy must match. Remote
   cleanup is exact-target, explicit and separately fenced; no prefix/age deletion of remote data.
10. Unknown remains unknown. Execution, cancellation, result availability, cleanup and hardware
    release are distinct facts. A 202 is only a committed local receipt.
11. Tombstones expire inventory without erasing history. Delete local bytes only against exact
    committed tombstones and original root bindings. Preserve ambiguous recovery material.
12. No automatic submission, restaging, start, cleanup, quota evasion or billing after ambiguity.
    CI remains offline; live effects need exact authorization and finite budgets.

## Domain

| Entity | Purpose |
|---|---|
| Installation | One exclusive owner of state, identity markers, blob roots and selected credential vault. |
| Workspace / token | Application namespace and current scoped authority under the trusted operator. |
| Connection | Managed provider identity, credential reference, revision and verified account/machine facts. |
| Selection / profile | Immutable policy and accelerator offered for admission; later configuration creates a new selection. |
| Object / bundle | Immutable workspace-owned input bytes, with bounded size/digest and safe logical paths. |
| Job | Accepted canonical specification, input/output declarations and original admission receipt. |
| Attempt | One explicit execution history with frozen binding, nonce, finite budgets and remote evidence. |
| Authorization | Consume-once execution, preparation, start or workload-secret grant for an exact attempt. |
| Operation / receipt | Durable requested action and immutable replay identity; cleanup is an operation kind. |
| Intent / resource | One-shot external mutation barrier and original staging/execution/deletion identity. |
| Lease | Renewable owner/generation/fence for dispatch, collection and cleanup work. |
| Publication | Atomically committed verified artifact set for one attempt and immutable result pin. |
| Tombstone | Irreversible inventory expiry and exact local-byte deletion authority; metadata remains. |

Public enums preserve independent orchestration, execution, collection and release evidence.
Provider-specific identity interpretation belongs inside adapters. Workspaces separate applications,
not mutually hostile tenants. Same-user processes, workload code and host administrators are trusted.

## Durable boundaries

SQLite uses short immediate transactions and owns local transaction time. True provider/progress
observation timestamps remain evidence. No transaction spans network or blob I/O. Idempotent
receipts, consume-once grants and fenced renewable leases each use one shared mechanism.

The ordinary path is admission → finite grant → staging intent → readiness observation → submission
intent → execution observation → collection pin → verified publication. SSH can split preparation
and start with a readiness revision; unused secret material is memory-only and consumed with start.
After a committed mutation barrier, recovery never recreates resources or redelivers secrets.

Identical request/key replay returns the original receipt under current authority. A changed request
conflicts. An exact replay is not a new authorization or proof of current remote success.
Workers retry transient local errors with backoff; corruption/invariant errors stop the runtime.
They join admitted callbacks at shutdown rather than claiming completion while effects may continue.

Provider adapters implement one 14-method Provider contract. One Start path accepts optional
start authority and workload-secret material; ListArtifacts returns a complete bounded catalog;
one Fetch path supports an optional progress callback. Exact observation/control and cleanup stay
inside the same port. Typed capabilities preserve supported/unsupported/unknown semantics;
an unsupported requirement fails before mutation. SSH fakehost contract tests exercise a real adapter.

## Decisions and consequences

- SQLite plus separate blob roots keeps local commit boundaries small. Whole-installation recovery
  must preserve both stores, markers and credentials; a database snapshot alone is insufficient.
- One managed serve composition removes parallel policy/configuration paths. An empty installation
  performs no provider calls. Existing explicit rows are retired by sequential migration, preserving
  remote evidence and requiring attention instead of initiating remote actions.
- The Kaggle adapter uses native Go HTTPS. Python is only the embedded remote runner/development
  toolchain. Historical SDK transport evidence does not qualify the native implementation.
- New installations default to an unencrypted private file vault. Existing macOS Keychain entries
  keep their exact native identity. A changed executable may require macOS access permission;
  continuity does not mean silent ACL migration or permission to export/recreate credentials.
- SSH pins native host and Docker engine identities, because two network paths can reach the same
  capacity and a Docker endpoint can change independently of the SSH address.
- Kaggle deletion is name-based despite immutable identity checks. Explicit cleanup authorization
  cannot eliminate the check/delete replacement race; uncertainty never permits repeating the RPC.
- HTTP artifact completion uses a final verification trailer. Consumers must preserve and verify
  it; received bytes alone cannot establish successful delivery.

## Fault cases

The [JSON catalog](../internal/devtool/fault-matrix.json) owns exact scenario/test identities.
Catalog validation checks exact test references; the full Go suite runs the referenced cases.
These qualify offline failure behavior, not live-provider or host power-loss guarantees.
HTTPS ingestion is removed, so its redirect scenario is covered by rejecting HTTPS admission.
The CLI-effect case also applies to SSH Docker commands; native Kaggle uses direct HTTPS.

| Case | Fault |
|---|---|
| FM01 | Client loses the admission response and repeats its idempotency key. |
| FM02 | Same key is reused with a different specification. |
| FM03 | Runtime crashes immediately before/after writing submission intent. |
| FM04 | Provider accepts a submission but the response is lost. |
| FM05 | An ambiguous resource cannot be rediscovered conclusively. |
| FM06 | Provider state is unknown or changes format. |
| FM07 | A stale poll arrives after a terminal observation. |
| FM08 | Runtime restarts while a remote attempt is active. |
| FM09 | Output collection fails halfway through a large file. |
| FM10 | A required output is missing or has the wrong digest. |
| FM11 | A complete bounded output catalog contains multiple entries. |
| FM12 | A cancellation request races with completion. |
| FM13 | Cancellation is unsupported or lacks a usable session identity. |
| FM14 | Local deadline expires while remote activity is unknown. |
| FM15 | Quota is missing, stale, exhausted, or consumed by external activity. |
| FM16 | Input staging succeeds but readiness is delayed. |
| FM17 | A URL redirects into a private network destination. |
| FM18 | A bundle contains traversal, links, duplicates, or excessive expanded size. |
| FM19 | A workspace references another workspace's object/artifact. |
| FM20 | The state directory is opened by a second runtime process. |
| FM21 | Disk runs out during upload, collection, or database commit. |
| FM22 | Provider credentials rotate or the configured account changes. |
| FM23 | A managed remote resource is manually modified outside the runtime. |
| FM24 | Cleanup is retried after the remote resource is already absent. |
| FM25 | A CLI exits nonzero after a possible remote side effect. |

Do not weaken a scenario or assertion to obtain a pass. Changed test locations must update the
catalog and retain the behavior. Run `./scripts/check.sh`; see [development](development.md).
Use [recovery](recovery.md), [artifact delivery](artifact-delivery.md) and
[remote cleanup](remote-cleanup.md) for operator procedures, not ad-hoc resets or new submissions.
