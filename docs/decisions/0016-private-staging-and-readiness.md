# ADR-0016: One-shot private staging and separately verified readiness

- Status: proposed; offline implementation in PR #20, pending owner review/merge
- Date: 2026-09-16
- Task: M4-02
- Requirements: DAT-04, OPS-05, SEC-03; preserves DUR-03
- Extends: ADR-0001, ADR-0002, ADR-0011 and ADR-0015

## Context

M3 already owns write-ahead preparation intent, frozen inputs, local fences and immutable
resource references. M4-01 adds credential-scoped isolated SDK reads. A Kaggle dataset-create
receipt alone cannot establish private visibility, processing readiness or intact input bytes.
An ambiguous upload/create must not be repeated merely because a later lookup is inconclusive.

The owner requested M4-02 offline implementation without changing the separate M1 live gate.
The previously exported checkpoint used a different provisional planner and is not the active
integration model. There must be one staging implementation on PR #20, not parallel packages.

## Decision

Implement a preparation-only Stager in the existing Kaggle package, using provider.Plan and
PreparationObserver directly. Keep the creation enablement flag as explicit operator policy,
not proof of a committed intent. The existing M3 orchestration invocation must obtain a NEW
BeginPreparation commit before Prepare; all restart/reconciliation uses observe only. Do not
add a production full-Provider stub, a second journal, migration or state-machine fallback.

Derive a stable opaque dataset name from the prewritten installation/workspace/instance/job/
attempt/preparation identities. Do not include mutable payload contents in that name. Bind
exact contents and logical mappings in a separately hashed marker; changed contents under an
existing intent conflict rather than selecting a new resource. Stage code/input bytes under
fixed opaque filenames, without copying commands, environment, credentials or source URLs.
Use rights-preserving metadata labels, never automatic CC0/public fallback.

Verify local originals before credential access, including exact length/hash/EOF and Close
acknowledgement. Stream and hash the actual upload again. Use public operations in the pinned
official SDK, guarded by the explicit version-bound transport seam. Permit only the reviewed
account, staging and read operations; one upload session per file and one private dataset
create per authorized helper invocation. Require a genuine exact-resource HTTP 404 before
fresh creation, not an error payload or failed authentication. Never update/version/adopt an
existing conflicting dataset or repeat an ambiguous mutation.

Separate pending/private observation from ready. Require exact dataset ID/ref/version 1,
marker/license, private metadata, READY processing state, complete bounded pagination, then
independent marker-first and all-payload byte verification. Recheck metadata/state after reads.
Preserve public identity as private-false attention evidence; do not dispatch or auto-publish.
Persist the first discovered ID/ref/version/marker via the existing M3 ledger and reject later
retargeting, including replacement at the same slug.

Use fixed isolated leaf processes, stdin-only secrets/payloads, bounded RPC/transfer output,
no redirects/retries/proxy fallback, verified TLS and an explicit signed-storage host allowlist.
A sanitized SDK redirect response must be safely closable without consuming its original
body. Parent deadlines and an independent helper watchdog bound cooperative work; no general
workload runner or hard-real-time kernel guarantee is implied.

## Alternatives rejected

- Name resources from the entire content marker: changed content could create another resource
  underneath the same durable intent and orphan earlier side effects.
- Mark ready from the create receipt or listing alone: neither verifies original private bytes.
- Retry create/upload after response loss or not-found: duplicates untracked mutations.
- Trust a configured account or name prefix as ownership: requires actual account/identity proof.
- Register an optimistic batch adapter to reuse the dispatch engine: invents unimplemented compute.
- Merge the obsolete checkpoint beside current code: creates conflicting types and semantics.

## Consequences and limits

Recovery favors safety over availability. A committed intent whose request never reached the
provider may remain unresolved rather than create again. Partial upload tickets can leave
provider storage; they are transient and have no independently persisted cleanup authority in
this component. No automatic deletion, public fallback or provider reclamation claim is made.

Only version-1 attempt datasets and the explicitly allowlisted storage host are supported by
this path. Readiness is a point-in-time observation; later submission/runner input verification
must still bind the exact version. Staging policy and frozen configuration must be preserved
across recovery. Credentials remain env-token-only under ADR-0015. No mutation CLI, production
serve, GPU submit or integrated live acceptance is added.

## Verification

Test-only composition delegates the real dispatch preparation phase to Stager while retaining
real admission, blobs, SQLite journal/ledger and no-compute assertions. Tests inject failed and
lost intent/create/observation acknowledgements, reopen state, remap current profiles and reject
resource replacement/public visibility. Python tests use real pinned SDK types with mocked
HTTP for creation/readiness, pagination, identity/privacy, byte faults and no hidden retries.
Go process tests exercise isolated stdin framing, finite streams, output limits and cancellation.
These layers are disclosed separately; no live account or provider mutation was used.

Exact-head CI and local limitations are recorded in PR #20 and the
[staging guide](../providers/kaggle-staging.md). Earlier accepted ADRs, proposal, dependency
pins, migrations and public contracts remain unchanged. Stop for owner review/merge; M4-03
and M1 live evidence remain separate work.
