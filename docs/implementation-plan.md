# Dependency-aware implementation plan

> **Status:** active; M-0, M1-01, M2 and M3 are merged at their offline gates.
> M4-01/M4-02 offline components are merged; M4-03 is in review in PR #21.
> M1/live batch acceptance remains blocked on separate evidence.
>
> **Planning date:** 2026-09-13; execution record updated 2026-09-16.
>
> **Scheduling rule:** milestones are acceptance gates, not calendar promises.

This plan converts the approved proposal and
[`scope-and-requirements.md`](scope-and-requirements.md) into executable tasks. It keeps
provider research, deterministic offline implementation, and authorized live verification
separate.

## Current execution record

M-0 was merged in PR #1, M2-01 in PR #2, M1-01 in PR #3, M2-02 in PR #4,
M2-03 in PR #5, M2-04 in PR #6, and M2-05/M2-06 together in PR #7.
PR #8 merged [safe packaging and allowlisted local import](packaging-and-import.md)
for M2-07. PR #9 merged [bounded public HTTPS ingestion](https-ingestion.md) for M2-08.
PR #10 merged the [finite remote runner](../runner/README.md) for M2-09, closing M2's
offline portable-core tasks without closing the live M1 gate.
PR #11 merged the [SQLite metadata foundation](storage.md) for M3-01.
PR #12 merged [durable idempotent admission](admission.md) for M3-02.
PR #13 merged [fair scheduling and fenced local ownership](scheduler.md) for M3-03.
PR #14 merged [durable preparation and one-shot dispatch recovery](dispatch.md) for M3-04.
PR #15 merged [durable attempt-scoped controls](operations.md) for M3-05.
PR #16 merged [verified artifact collection and recovery](collection.md) for M3-06.
PR #17 merged [retention, local sweep and remote cleanup preview](retention.md) for M3-07.
PR #18 merged [executable fault qualification](fault-matrix.md) and
[recovery/state semantics](recovery.md) for M3-08 at `cc44f50` on 2026-09-16, closing M3's
implemented offline gate. It did not close M1 or establish a production runtime.

PR #19 merged M4-01's [local/read-only preflight foundation](providers/kaggle-preflight.md)
at `52b1655` on 2026-09-16. PR #20 merged M4-02's
[private staging and readiness](providers/kaggle-staging.md) at `91fa6be` on 2026-09-16.
PR #21 implements M4-03 [one-shot execution and exact-version observation](providers/kaggle-execution.md)
and remains in review until owner merge. Proposal section 23.6 permits offline work without
waiving M1-08 go for integrated batch activation. No live-provider work or M4-04 has started.
See ADR-0015/0016/0017. The retained `checkpoint/m4-02-offline-staging` branch is obsolete,
not an integration prerequisite; its conflicting package is not part of PR #20 or PR #21.

[Authentication and object storage](auth-and-objects.md) have offline components,
real-loopback tests, atomic blob publication and an explicit ownership-commit seam.
M2-07 adds named-root import and bundle validation; M2-08 adds guarded public HTTPS
snapshots; M2-09 adds an explicit finite Linux runner asset, never invoked by local admission.
M3-01 supplies persistent installation/workspace/token/object repositories, OS locking,
ordered migrations and database-only backup/restore. M3-02 adds canonical job identity,
frozen profile/object references, atomic job/attempt/event/idempotency, state/event CAS and
optional create/validate/status HTTP. Pending HTTPS inputs are source records at admission.
M3-03 adds durable FIFO/round-robin claims, shared-account capacity, bounded local workers
and explicit quota policy; a claim alone is not a remote submission intent.
M3-04 freezes/verifies inputs, commits staging/submission ledgers before one-shot mutations,
recovers the same attempt through observation and exposes sanitized cached conditions.
Terminal observation opens collection without claiming verified results. M3-05 adds durable
operation records and immutable receipts, one-shot cancellation, frozen-input compute retry,
observation-only reconcile, transfer-only collection tickets and five optional HTTP handlers.
M3-06 consumes those tickets with strict result/identity verification, bounded streaming,
immutable result pins, fenced leases, atomic publication and authenticated internal reads.
Recovery reuses the same pin and verified blobs without rerunning compute. M3-07 adds named
retention holds, irreversible expiry with sequenced events, exact bound-store local deletion,
expired-input admission/retry guards and authenticated ledger-based remote dry runs.
Metadata is retained; remote apply and staging preview are not supplied.

M3-08 maps all 25 numbered proposal scenarios to 34 distinct named tests, adds missing
observation/transport/large-transfer/policy regressions and makes `devtool fault-test` part
of `check`. Qualification verifies exact proposal section/order/text and source identities,
then requires fresh uncached run/pass evidence and complete package outcomes. Missing or
skipped roots/subtests cannot qualify. Recovery guidance keeps immutable receipts, current
state, remote uncertainty, result availability and expiry distinct. Local-deadline, dry-run
cleanup and synthetic CLI/account evidence limits are explicit in the matrix; none is
relabeled as a production deadline service, remote apply or live-provider test.

M4-01 preparation supplies explicit environment-reference resolution, immutable non-secret
preflight configuration, local version checks and an opt-in read-only operator utility.
A fixed isolated helper checks server account identity before quota-data availability through
two bounded official-SDK requests. It is not a Provider or dispatch registration and always
reports `batch_ready=false`. Numeric quota mapping, durable provider configuration and unified
TOML/serve composition are not supplied by this foundation. Offline SDK fixtures do not
qualify K-01/K-12 live behavior or waive M1's private execution/result requirements.

M4-02 adds intent-derived staging names, frozen marker/file identities, pre-credential local
byte checks, one-shot private SDK creation and separately observed, paginated, byte-verified
readiness. Recovery is read-only and never creates again after a committed/ambiguous intent.
Real SQLite integration tests demonstrate write-ahead ownership, lost acknowledgements,
profile remapping and immutable discovered references through a test-only provider wrapper.
The real pinned SDK has mocked-transport tests in a separate tier. No production full-Provider
stub, new migration, mutation CLI or live staging acceptance is implied.

M4-03 constructs locked runner source from the original plan/staging identity and supplies a
per-attempt Executor. Newly committed M3 submission authority permits one bounded private SDK
save; reconciliation reads the original identity without another save. Exact source, numeric
kernel ID, version, account and privacy checks precede status use. Missing SDK fields/statuses
remain unknown, cancellation acknowledgements do not prove termination, and release evidence
stays not observable. Real SQLite tests preserve confirmed attempt state even when the latest
journal observation is unknown. SDK/leaf-process/bootstrap tests are offline, with documented
upsert, same-version rerun, dataset-mount and cross-binary recovery limits. Full Provider
registration, optional capabilities and output retrieval are not supplied by this component.

Operator credentials and live probes belong to the operator's own runtime, not GitHub
Actions. Actions are offline code-quality/build/test only. Use the available local Linux
runtime for executable smoke checks; do not add deployment or real Kaggle/GPU workflows.
At the owner's request, finish one task/PR, report its evidence, and stop for owner merge
before beginning another task. PR #21 implements M4-03's offline execution boundary, not
complete integrated Kaggle batch. Artifact HTTP/CLI, production `serve`, remote cleanup and
live Kaggle remain separate. Commit and push reviewable checkpoints during implementation;
the disposable local runtime must not be the only copy of ongoing work.

## Status vocabulary

- `proposed`: scoped but dependencies are not ready;
- `ready`: dependencies are satisfied and work can begin;
- `in-progress`: active work exists;
- `blocked`: a named prerequisite prevents safe progress;
- `in-review`: implementation/artifacts exist on a branch or PR;
- `complete`: merged and acceptance evidence is recorded.

## Delivery principles

1. Prove the smallest private bounded Kaggle path before building a broad adapter around
   assumptions.
2. Build provider-neutral schemas/contracts and the deterministic fake provider in parallel
   after M-0; do not wait idly for credentials.
3. Never present fixture/offline success as live-provider evidence.
4. No compute-creating mutation is automatically retried after ambiguity.
5. Every task cites requirement IDs and names whether it needs external credentials or
   compute.
6. Material decisions use ADRs; routine reversible choices are made without another product
   discovery round.
7. A task is complete only after its validation and documentation disclosure are recorded.

## Dependency graph

```text
M0 evidence/traceability/ADRs/plan
        |
        +--------------------------+
        |                          |
        v                          v
M1 thin live-provider proof   M2 portable offline core
        |                          |
        +-------------+------------+
                      v
              M3 durable orchestration
                      |
                      v
              M4 integrated Kaggle batch
                      |
                      v
              M5 usable developer product
                      |
                      v
              M6 release hardening
```

M2 may begin after M-0 while credentials are unavailable. Integrated M4 batch activation
requires M1 go and M3's durable semantics. The owner's requested M4 offline preparation
under proposal section 23.6 does not remove that dependency or grant live mutation authority.
ADR-0015/0016/0017 record the distinction; live acceptance rows remain blocked.

## M-0 — Evidence and scope

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M0-01 | documentation / `complete` | Establish repository policy, approved proposal, architecture baseline, roadmap scaffold, evidence vocabulary, and commit convention. | PRD-01–PRD-10, VER-01 | None | No / No | Root bootstrap commit and passing commit-convention workflow. |
| M0-02 | documentation / `complete` | Create stable requirement IDs and map product behavior to components, tests, and milestones. | All matrix rows | M0-01 | No / No | [`scope-and-requirements.md`](scope-and-requirements.md) has no required behavior without target evidence. |
| M0-03 | research / `complete` | Review the current stable official Kaggle CLI/client surfaces, versions, identity, outputs, logs, quota, timeout, staging, and cancellation gaps. | PRD-04, DAT-04, PRV-02/03, OPS-02/04, VER-03 | M0-01 | No / No | Dated primary-source review at an exact release/tag; no live claims. |
| M0-04 | research / `complete` | Populate K-01…K-16, risk responses, compatibility target matrix, and live checklist. | PRD-04, PRV-02/03, OPS-02/04, SEC-03, VER-03, DX-01 | M0-03 | No / No | Every gate has evidence level, fallback, and M-1 acceptance procedure. |
| M0-05 | ADR / `complete` | Record official-client boundary, attempt-scoped provider identity, and Go/SQLite baseline. | PRD-03/05, DUR-01/03/04, PRV-01/02, SEC-01 | M0-03 | No / No | ADR-0001…0003 accepted with alternatives, consequences, and verification. |
| M0-06 | planning / `complete` | Replace the template with this dependency-aware backlog and explicit live/offline boundaries. | All | M0-02–M0-05 | No / No | Every implementation task states dependencies, external effects, and acceptance evidence. |
| M0-07 | review / `complete` | Review and merge the M-0 documentation set; close M-0 without upgrading any live capability. | VER-01/03 | M0-02–M0-06 | No / No | PR checks pass, reviewer findings resolved, documents merged on `main`. |

### M-0 exit decision

M-0 is closed after PR #1. Its completion allows M1 and M2 work; it does not authorize
credentials, GPU allocation, or destructive provider cleanup by itself.

## M-1 — Thin real-provider proof

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M1-01 | implementation / `complete` | Build an isolated pinned Python 3.11 Kaggle client environment and narrow probe harness; inventory CLI/bridge operations and exact SDK lock. | PRD-04, PRV-01, SEC-01, VER-03 | M0-07, ADR-0001 | No / No | Reproducible install/version output, command fixtures, subprocess safety tests, dependency/license record. |
| M1-02 | live verification / `blocked` | Run read-only authentication and quota probes with actionable redacted diagnostics. | OPS-02, SEC-01, VER-03 | M1-01, explicit credential authorization | Yes / No GPU | K-01/K-12 sanitized fixtures; missing/invalid credential negative case; no secret disclosure. |
| M1-03 | live verification / `blocked` | Create a unique synthetic private staging dataset, verify uploaded bytes/privacy/readiness, and record ownership. | DAT-04, SEC-03, OPS-05, VER-03 | M1-02, explicit provider-side-effect authorization | Yes / No GPU; provider storage/API | K-03/K-04 checksums, private visibility, readiness transitions, cleanup dry run. |
| M1-04 | implementation + live / `blocked` | Generate an attempt-scoped private kernel and runner manifest; execute a <=120-second real GPU smoke computation. | JOB-03/05, PRV-02, OPS-03, VER-03 | M1-03, ADR-0002, finite GPU authorization | Yes / Finite GPU | K-02/K-05/K-09/K-13: actual GPU calculation, environment/result manifest, persisted identities. |
| M1-05 | live verification / `blocked` | Observe raw lifecycle, logs, terminal state, multi-file/paginated outputs, and identity/digest-safe collection. | DOM-02, PRV-02/03, VER-03 | M1-04 | Yes / Covered by bounded run | K-06/K-07/K-08/K-10 fixtures; all pages; matching nonce/digests; log availability classification. |
| M1-06 | live verification / `blocked` | Test one small timeout and investigate supported cancellation/target identity without using delete as cancel. | OPS-03/04, VER-03 | M1-04, separate explicit mutation authorization | Yes / Finite GPU only when required | K-09/K-11 conclusion: proven supported path or explicit unsupported/manual-required behavior. |
| M1-07 | fault/live verification / `blocked` | Induce a lost/ambiguous response and restart the local observer; reconcile the same attempt without another push. | DUR-03, PRV-02, VER-02/03 | M1-04/M1-05, safe fault procedure | Yes / Finite GPU | K-05/K-15 evidence and command/audit count proving no automatic duplicate execution. |
| M1-08 | ADR + research / `proposed` | Decide go/no-go for Kaggle batch and finalize CLI-versus-bridge operation mapping and capability matrix. | PRD-04, PRV-01/03, VER-03 | M1-02–M1-07 or precise blockers | No new compute | Accepted/superseding ADR, updated feasibility report, provider doc, and explicit go/blocked decision. |

### M-1 authorization boundary

Tasks marked `blocked` require the operator to make authorized credentials available to the
execution environment and explicitly approve the stated finite side effect/budget. Secrets
must not be pasted into chat, committed, or captured in fixtures.

If authorization is unavailable, M1-01 and all safe fixture/harness work continue, while
live tasks remain `blocked-environment`.

## M-2 — Portable provider-neutral core

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M2-01 | implementation / `complete` | Create the Go module, pin Go/dependencies, license inventory, cross-platform developer command wrappers, and base CI. | PRD-05, VER-01, DX-01 | M0-07, ADR-0003 | No / No | Format/vet/static/unit commands on CI; exact module pins; no Docker/CGo required for baseline build. |
| M2-02 | implementation / `complete` | Define opaque IDs, stable error taxonomy, job/attempt/operation/event models, capability states, and monotonic transition rules. | JOB-01, DOM-01/02/03, PRV-03, API-03 | M2-01 | No / No | Table-driven domain tests, unknown states, cancellation/result separation, no provider imports. |
| M2-03 | implementation / `complete` | Create versioned job/result/config JSON schemas and OpenAPI skeleton with strict validation. | JOB-01/02/03, API-01/03 | M2-02 | No / No | Schema/example validation; unknown fields/path/enums/limits rejected; generated contract diff checked. |
| M2-04 | implementation / `complete` | Define narrow provider/store/blob/credential/clock/event ports and registry; implement deterministic fake provider. | PRD-03, PRV-01/03, VER-01 | M2-02 | No / No | Contract suite completes generic lifecycle with no Kaggle import and with missing cancel/log/quota variants. |
| M2-05 | implementation / `complete` | Implement workspace tokens/digests, authorization service, loopback HTTP middleware, request IDs, body/rate limits. | PRD-07, API-01/02, SEC-02 | M2-02/M2-03 | No / No | Merged PR #7: cross-workspace/profile/scope/revocation matrix, Host/Origin/default exposure checks, real-loopback bounded HTTP tests; persistent token repository supplied by M3-01. |
| M2-06 | implementation / `complete` | Implement streaming object upload and filesystem blob lifecycle with immutable digests and atomic publication. | DAT-01, DUR-01, API-01, SEC-02 | M2-02/M2-03 | No / No | Merged PR #7: streamed checksum/length validation, real process-kill recovery, disk/commit-ambiguity tests, quota/permissions and local smoke; incomplete bytes never usable; SQLite ownership repository supplied by M3-01. |
| M2-07 | implementation / `complete` | Implement safe `.tar.gz` packaging/inspection and allowlisted local import. | DAT-01/02, SEC-02 | M2-06 | No / No | Merged PR #8: deterministic bundle/manifest checks, archive traversal/link/collision/bomb corpus, rooted snapshot races, strict workspace import HTTP, local race/repeat/fuzz/smoke and native CI. See ADR-0005 and packaging-and-import.md. |
| M2-08 | implementation / `complete` | Implement bounded public HTTPS ingestion with SSRF and redirect protection. | DAT-01/03, SEC-02 | M2-06 | No / No | Merged PR #9: public-only DNS/peer/TLS checks per hop, blocked private/mixed/metadata targets, bounded streaming/timeouts, verified EOF, workspace/commit/redaction tests, request schema/OpenAPI, local race/repeat/fuzz/TLS smoke and native offline CI. See ADR-0006 and https-ingestion.md. |
| M2-09 | implementation / `complete` | Implement generic remote-runner contract and local deterministic runner tests without executing admitted workload code on the host. | JOB-02/03/05, SEC-02 | M2-02/M2-03 | No / No | Merged PR #10: strict frozen-input manifest, bounded extraction/setup/process groups/logs/results; actual CPU/shell/timeout/SIGTERM fixtures, synthetic GPU and pip-plan checks, generated result JSON/schema and source-lock verification. See ADR-0007 and runner/README.md. No local admission execution or live GPU claim. |

## M-3 — Durable orchestration

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M3-01 | implementation / `complete` | Implement SQLite migrations/store, state-directory lock, transaction policy, and consistent backup/restore baseline. | DUR-01/04, DX-01 | M2-01/M2-02, ADR-0003 | No / No | Merged PR #11: concrete workspace/token/object repositories, embedded migration checksums, OS locking, restart/concurrency/disk-full/backup tests, native production-driver CI and disclosed local system-SQLite smoke. See storage.md and ADR-0008. |
| M3-02 | implementation / `complete` | Implement durable workspace/object/job/attempt admission with idempotency key and canonical request hash. | JOB-01/04, DUR-01/02, API-03 | M2-03/M2-05/M2-06, M3-01 | No / No | Merged PR #12: one transaction persists canonical request, immutable profile/object pins, job/attempt, accepted event and original receipt; lost response/restart/concurrent replay preserves IDs; changed request conflicts; state/event CAS, rollback/disk-full/HTTP tests, source-only pending HTTPS and disclosed local harness. See admission.md and ADR-0009. |
| M3-03 | implementation / `complete` | Implement bounded FIFO/round-robin scheduler, account capacity, transactional dispatch claims, and backpressure. | OPS-01/02/03 | M2-04, M3-01/M3-02 | No / No | Merged PR #13: transactional FIFO/round-robin cursor, shared-account/worker reservations, generation/fence leases, conservative dispatch barrier, pause/disable/backpressure, quota precision/exhaustion latch, restart/concurrency/rollback/disk-full/process-kill tests and finite metadata-only smoke. See scheduler.md and ADR-0010. Claim ownership does not authorize provider mutations. |
| M3-04 | implementation / `complete` | Persist preparation resources and submission intent before side effects; implement accepted/rejected/unknown and reconciliation. | DUR-03, PRV-02, VER-02 | M3-01/M3-03 | No / No | Merged PR #14: immutable URL-role freeze and byte verification; exact provider binding; staging/submission ownership journals and one-shot gates; fenced restart reconciliation; cached safe status conditions; lost-response/commit-ack, real process-kill, disk-full, stale-owner and twenty-way gate tests; real-blob fake-provider smoke. See dispatch.md and ADR-0011. Terminal observation opens collection only. |
| M3-05 | implementation / `complete` | Implement durable cancel/retry/reconcile/collect operations with race-safe transition rules. | DOM-02/03, API-03, OPS-04 | M3-02/M3-04 | No / No | Merged PR #15: migration 6; explicit attempt targets, immutable receipts/current status, one-shot cancellation/manual-required, cancellation/completion and staging races, frozen-input concurrent retry, revocation and event rollback, transfer-only collection tickets, authenticated HTTP and actual-response/schema checks. See operations.md and ADR-0012. Collection consumer is supplied by M3-06. |
| M3-06 | implementation / `complete` | Implement attempt-scoped artifact collection, verification, atomic publication, and collection-only recovery. | JOB-05, PRV-02, VER-02 | M2-06, M3-04/M3-05 | No / No | Merged PR #16: migration 7; immutable result pins, fenced collection leases, strict manifest/frozen requirement checks, bounded pagination/transfers, independently rehashed blobs, atomic publication/state/events and scoped internal reads. Tests cover wrong identity/digest/missing output, partial/late-error transfer, stale/concurrent owners, cancellation race, SQL disk-full, schema-6 upgrade, lost acknowledgements and real process kill without another compute submission. See collection.md and ADR-0013 for directory and composition limits. |
| M3-07 | implementation / `complete` | Implement ownership ledger, retention pins, local sweep, and remote cleanup dry-run plan. | OPS-05, DUR-01, VER-02 | M3-01/M3-04/M3-06 | No / No | Merged PR #17: migrations 8/9; complete reference/pin checks, named holds, immutable expiry/audit, atomic result.expired event, expired-input preflight/commit guards, exact bound-root deletion and acknowledgement recovery. Authenticated exact-ledger dry runs recheck pins/binding/authority and keep owned absence idempotent. Tests cover upgrade, wrong bytes/root/identity, rollback/disk-full, lost acknowledgements and hold/revocation races. See retention.md and ADR-0014. Metadata is preserved; remote apply and staging preview are not supplied. |
| M3-08 | test/documentation / `complete` | Execute the proposal fault matrix and document recovery/operational state semantics. | VER-02, all DUR/DOM/OPS | M3-01–M3-07 | No / No | Merged PR #18: exact 25-scenario/34-test catalog, fresh uncached named-test qualification in devtool check, missing/skipped/incomplete evidence rejection, observation/identity/deadline/account/CLI/16 MiB transfer/quota/cancellation regressions, recovery guide and requirement links. See fault-matrix.md for exact executed evidence and limits: local invocation deadline, cleanup dry-run and synthetic provider/helper effects are not production/live qualification. Final-head CI is recorded in the PR. |

## M-4 — Integrated Kaggle batch

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M4-01 | offline foundation / `complete`; live acceptance / `blocked` | Implement Kaggle instance configuration, credential resolver integration, version checks, and read-only preflight. | PRD-04/08, SEC-01, PRV-03 | M1-08 go for integrated batch; M2-04/M2-05, M3-01 | Offline No / No; live verification Yes / No GPU | Merged PR #19: explicit env references, immutable non-secret configuration, local pin checks before token access, bounded isolated SDK account/quota reads, opt-in operator command, negative/transport/process and real pinned-SDK fixture tests. No Provider registration, durable config composition or batch readiness. See providers/kaggle-preflight.md and ADR-0015; K-01/K-12 live evidence remains blocked. |
| M4-02 | offline implementation / `complete`; live acceptance / `blocked` | Implement private attempt staging/readiness and resource-ledger recording. | DAT-04, OPS-05, SEC-03 | M1-08, M3-04/M3-07, M4-01 | Offline No / No; live verification Yes / provider storage | Merged PR #20: stable attempt/preparation naming, exact marker/input identity, pre-credential byte/EOF/Close checks, bounded no-retry private SDK creation, paginated marker-first/all-file verification and read-only recovery. Real SQLite integration tests cover ownership-before-helper, lost acknowledgements, reopen/remap and pinned resource replacement rejection. Explicit source-completion trailer prevents pipe errors from authorizing dataset creation. See providers/kaggle-staging.md and ADR-0016; no production Provider or live K-03/K-04 claim. |
| M4-03 | offline component / `in-review`; live acceptance / `blocked` | Implement attempt-scoped submit, observation, raw-state mapping, and ambiguity reconciliation. | PRV-02, DUR-03, DOM-02/03 | M1-08, M3-04, M4-02 | Offline No / No; live verification Yes / Finite GPU | PR #21: locked remote source and stable intent-derived slug; per-attempt Executor under M3 authority; bounded one-shot private SDK save, exact ID/version/source/account reads and raw-field status mapping. Unit/real-process/SQLite fault tests preserve original intent and confirmed state across lost acknowledgements, restart/remap and stale/replaced observations; actual pinned SDK uses mocked HTTP. See providers/kaggle-execution.md and ADR-0017 for upsert, same-version identity, mount/upgrade and composition limits. No blind resubmit or live acceptance. |
| M4-04 | implementation / `proposed` | Implement logs, quota, timeout, capability, and cancellation/manual-required mappings. | PRV-03, OPS-02/03/04 | M1-08, M3-05, M4-03 | Live verification Yes / bounded where needed | Optional capabilities exactly match evidence; missing values remain unknown/unsupported. |
| M4-05 | implementation / `proposed` | Implement paginated output retrieval and manifest-bound artifact collection. | JOB-05, PRV-02, VER-02 | M3-06, M4-03 | Live verification Yes / existing completed run | K-07 fixtures/live proof; all required outputs verified against the exact attempt. |
| M4-06 | integration / `proposed` | Run sanitized end-to-end private GPU acceptance through the durable runtime, including restart recovery. | PRD-04, VER-03 | M4-01–M4-05 | Yes / Explicit finite GPU | Proposal live checklist passes; gaps remain explicitly unobservable/unsupported. |

## M-5 — Usable developer product

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M5-01 | implementation / `proposed` | Build unified runtime/CLI commands for init, serve, workspace/token, validate, submit/status/logs/artifacts, explicit operations, and cleanup. | API-01/03, DX-01 | M3 complete, M4 interfaces stable | No for offline commands; provider commands vary | CLI tests use same application services/API and never create compute from read-only commands. |
| M5-02 | implementation / `proposed` | Finalize strict TOML config, example config, precedence, path resolution, validation, and safe defaults. | PRD-08, JOB-03, SEC-01, DX-01 | M4-01/M5-01 | No / No | Examples validate; unknown/contradictory keys fail; effective config excludes secret values. |
| M5-03 | implementation / `proposed` | Implement doctor modes separating local, read-only provider, and explicitly compute-consuming checks. | VER-03, DX-01 | M4-01/M5-02 | Optional credentials; GPU only explicit | Default doctor consumes no GPU; explicit smoke flag shows budget and evidence tier. |
| M5-04 | example/live / `proposed` | Ship real GPU smoke and small pinned open-access LLM batch examples outside core. | DX-02, VER-03 | M4-06, verified environment/model card | Yes / Explicit finite GPU for live acceptance | Actual GPU use, bounded prompts/output, model revision/license, manifests, and verified artifacts. |
| M5-05 | implementation / `proposed` | Ship thin Node.js, Python, and Go HTTP clients for one common job/artifact model. | PRD-02/03, DX-02 | M5-01/M5-04 | No in CI; live example optional | All clients handle idempotency, attention/unknown, polling, errors, and digest-checked downloads. |
| M5-06 | packaging/docs / `proposed` | Provide direct-install/build path, optional container, cross-platform dev wrappers, and first-run guide. | PRD-05/08, DX-01 | M5-01–M5-05 | No / No | Clean-environment procedures; Docker optional; no privileged/socket mount or paid service. |

## M-6 — Release hardening

| ID | Type / status | Intended behavior and components | Requirements | Dependencies | External credentials / compute | Acceptance evidence |
|---|---|---|---|---|---|---|
| M6-01 | security/test / `proposed` | Complete archive/SSRF/auth/secret/limits/threat-model tests and redacted diagnostics. | DAT-02/03, SEC-01/02/03, VER-02 | M5 complete | No / No | Security corpus and canary checks pass; diagnostics exclude private bytes/secrets by default. |
| M6-02 | implementation/test / `proposed` | Harden retention, disk limits, backup/migrations, ownership-safe cleanup, and operator recovery docs. | DUR-01/04, OPS-05, VER-02 | M3/M4/M5 | Live cleanup uses explicit auth, no new compute | Disk/crash/backup/cleanup tests and documented restore/manual resolution. |
| M6-03 | compatibility / `proposed` | Run native platform matrix and publish exact supported runtime/provider versions. | DX-01, VER-01/03 | M5-06, M6-01/M6-02 | Provider verification as scoped | Every claimed platform has native evidence; untested targets remain targets, not support claims. |
| M6-04 | release / `proposed` | Build reproducible signed/checksummed artifacts, SBOM/license notices, changelog, support/security version policy. | PRD-01/05/08, VER-01 | M6-01–M6-03 | No / No | Release checklist and artifact verification pass from a clean environment. |
| M6-05 | acceptance / `proposed` | Audit final definition of done against requirements, evidence, risks, and explicit exclusions. | All | M6-01–M6-04 | No new compute unless a stale claim needs recheck | Traceability has no orphan requirement/test; provider claims cite current sanitized evidence. |

## Task completion record

A completed task must add a concise record to its PR or durable document:

```text
Task:
Requirements:
Behavior delivered:
Files/components changed:
Offline validation and exact commands:
Live provider behavior observed:
Credentials/compute/storage side effects:
Checks not run and why:
Evidence/docs updated:
Residual risks or unknowns:
```

“Tests pass” without the test tier and command is insufficient. “Kaggle supported” without
the tested client/account/path is insufficient.

## Next work after M4-03 review

Stop after reporting PR #21 for owner review/merge. Do not begin M4-04 in this task.
The per-attempt Executor requires NEW M3 submission authority; a reconstructed executor,
preflight success or missing resource cannot authorize another save. Preserve original source,
configuration/binary and remote identity when investigating uncertainty. Provider terminal
observation opens collection, not job success or a claim of retained verified artifacts.

M1-02 read-only evidence and M1-03–M1-07/M1-08 retain their own authorization requirements.
Provider mutation, GPU allocation, remote cleanup apply and live compatibility require explicit
authorization and evidence. Optional capability/cancellation/quota mapping, output retrieval,
full Provider registration, artifact HTTP/CLI, durable configuration and production `serve`
remain separate work. No remote exactly-once guarantee, same-version session attestation,
immutable dataset mount or transparent cross-binary recovery is implied by offline tests.
