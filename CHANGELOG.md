# Changelog

All notable changes to this project will be documented in this file.

The format follows the principles of Keep a Changelog, and released versions will use
Semantic Versioning once a public compatibility surface exists.

## [Unreleased]

### Added

- Initial open-source repository policy and documentation system.
- Approved project proposal and provider-neutral architecture baseline.
- Apache-2.0 licensing and OSS governance/security/contribution policies.
- Repository-wide instructions for human and automated contributors.
- Conventional Commits documentation, local hooks, commit template, and CI validation.
- Requirement-to-component-to-test traceability for the approved MVP boundaries.
- Dated Kaggle CLI `v2.2.4` interface review and populated K-01 through K-16 feasibility
  ledger.
- Initial ADRs for the official-client boundary, attempt-scoped provider identity, and
  Go/SQLite baseline.
- Dependency-aware M-0 through M-6 implementation plan with explicit credential/compute
  authorization boundaries.
- Risk register, compatibility target matrix, and live-verification checklist.
- Pinned credential-free Kaggle client probe environment with cross-platform offline
  safety tests and dependency/license inventory.
- Go module, pre-release executable, cross-platform developer commands, and offline CI
  foundation.
- Provider-neutral Go domain model with typed opaque identities, capability/evidence
  semantics, stable structured errors, explicit entities, independent attempt-state
  dimensions, and monotonic transition tests.
- Strict Draft 2020-12 JSON Schemas, OpenAPI 3.1 skeleton, positive/negative fixtures,
  domain-enum drift checks, and a reviewable contract-content lock.
- Provider-neutral execution and infrastructure ports, explicit instance registry, optional
  capability handling, and a deterministic fixture-only provider with reusable contracts.
- Local provider smoke command with identity/digest-checked artifacts, fault/race tests,
  and an explicit operator-runtime versus offline-CI boundary.
- Workspace token issuance/revocation, digest-only repository contracts, scope/profile and
  resource authorization, guarded loopback HTTP and bounded request handling.
- Streamed object upload, immutable filesystem blobs, OS locking, private Unix/Windows
  permissions, crash/fault tests and an explicit ownership-commit boundary.
- Object metadata schema, local request error codes, implemented-handler OpenAPI status,
  and a finite local upload/isolation/revocation/restart smoke command.
- ADR-0004 documenting separate workspace authority, byte publication and durable admission.
- M2-07 manifest-bound `.tar.gz` bundle preview/create/inspect commands, explicit selection,
  bounded `.computeignore` rules, credential-pattern checks and strict archive validation.
- Rooted, workspace-allowlisted local file/bundle import through the existing verified
  upload/ownership path, plus HTTP/schema tests and a finite local packaging smoke command.
- ADR-0005 recording the strict USTAR subset, source snapshot semantics and import boundary.
- M2-08 opt-in public HTTPS input snapshots with per-hop DNS/peer/TLS checks, bounded
  redirects/time/bytes, no ambient credentials/proxies, and verified immutable publication.
- Strict HTTPS ingestion request schema/OpenAPI, service/API fault tests, a finite local
  TLS smoke and ADR-0006 documenting the public-address and retry/privacy boundaries.
- M2-09 finite Python/Linux remote runner with frozen-input identity, strict bundle
  extraction, CC_* environment, bounded setup/process groups/logs and verified outputs.
- Runner manifest and source locks, real CPU/shell fault tests, generated result-schema
  checks, and ADR-0007 separating runner outcomes from provider terminal/release evidence.
- M3-01 CGo-free SQLite metadata foundation with persistent installation/workspace/token
  digests/object ownership, embedded checksum-bound migrations and OS-level state locks.
- Consistent database-only backup/restore, identity-loss guards, rollback/crash/disk-full
  tests and a finite local store smoke command.
- ADR-0008 and storage guidance documenting transaction, permission, backup and evidence
  boundaries without claiming durable job admission or a production server.
- M3-02 strict embedded-schema job validation, versioned canonical request hashing and
  atomic SQLite job/attempt/object-pin/event/idempotency admission with original receipt replay.
- Immutable profile revisions, current-authority transaction checks, bounded outstanding
  jobs and state/event CAS persistence without provider calls or workload execution.
- Authenticated create/validate/status handlers, process-crash/lost-response/rollback tests,
  metadata-only admission smoke and ADR-0009 documenting pending preparation and replay limits.
- M3-03 durable FIFO/round-robin queue ownership, shared-account capacity and fenced local
  leases, with paused initialization and a conservative no-redispatch barrier.
- Bounded local worker pool, explicit quota uncertainty/strict/exhaustion policy, scheduler
  events and migration/restart/concurrency/disk-full/process-kill tests.
- Finite metadata-only scheduler smoke and ADR-0010 distinguishing local claims from remote
  preparation/submission intent; no provider execution is enabled by a claim.
- M3-04 immutable input preparation, frozen provider/account snapshots, private staging
  observation and SQLite resource/submission-intent journals with one-shot mutation gates.
- Fenced same-attempt reconciliation, safe cached status conditions, quota-rejection latch,
  process-kill/commit-loss/rollback tests and a real-blob, nonexecuting fake dispatch smoke.
- ADR-0011 documenting conservative recovery and the separate collection/control-operation
  gates; no production server or live provider capability is claimed.
- M3-05 migration 6 with attempt-scoped durable operation records, immutable idempotency
  receipts, per-attempt cancellation/retry uniqueness and operation-linked events.
- Dispatch-preventing or one-shot capability-verified cancellation, conservative unresolved
  outcomes, frozen-input explicit new attempts and observation-only reconciliation.
- Transfer-only collection tickets after terminal execution evidence; M3-06 supplies their
  separately composed consumer, result verifier and artifact publication.
- Authenticated cancel/retry/reconcile/collect and current-operation HTTP handlers, strict
  request/receipt schemas, fixture/lock updates and actual-response contract tests.
- Concurrent retry, cancellation/completion, staging, revocation, event rollback and
  lost-acknowledgement tests; serializer/record truth-table checks cover 880 combinations.
- Operations guidance and ADR-0012 documenting immutable receipt versus current-state
  reads, cancellation evidence, original-input retry and the collection handoff.
- M3-06 bounded transfer-only collection engine, strict manifest/frozen-requirement checks,
  immutable result pins, independent blob hashing and authenticated internal artifact reads.
- Migration 7 with fenced collection leases and atomic artifact/result/state/event publication;
  restart and explicit transfer retry preserve the pinned result without rerunning compute.
- Real SQLite/blob tests for pagination, partial/late-error transfers, wrong identity/digest,
  missing outputs, stale/concurrent ownership, cancellation races, revocation, schema upgrade,
  actual disk-full rollback, lost acknowledgements and process kill after durable pin.
- Collection guidance and ADR-0013 documenting publication/recovery, dedicated result storage,
  cooperative shutdown, empty-required-directory limitations and separate HTTP/CLI gates.
- M3-07 additive migrations 8/9: inventory, named holds, irreversible expiry/audit, rotating
  scans, latest remote previews and separate persistent input/result blob-store bindings.
- Pin-aware finite local sweep with exact byte/metadata verification, quarantine rename,
  already-absent recovery and independent deletion acknowledgement; metadata is preserved.
- Atomic `result.expired` state/event/tombstone transactions and expired-input checks before
  admission/retry reads and at commit, while original idempotency receipts remain replayable.
- Authenticated exact-ledger remote cleanup dry runs with frozen binding and current pin/
  authority rechecks. No remote apply; staging preview remains explicitly unavailable.
- Retention tests for shared/recovery/worker/manual pins, root swap/replacement, upgrade,
  exact deletion/quarantine reopen, lost acknowledgements, event/audit rollback, actual
  SQLite disk-full, receipt preservation and preview hold/revocation races.
- Retention guidance and ADR-0014 separating metadata expiry, local byte removal and remote
  preview, with conservative defaults, backup identity and metadata-pruning limitations.
- M3-08 versioned fault catalog mapping all 25 numbered proposal scenarios to 34 distinct
  named tests, with fresh uncached qualification through `devtool fault-test`.
- Exact section/order/text and source identity checks; bounded Go test-event consumption
  rejects missing/skipped/failed roots or descendants, incomplete packages and invalid output.
- Regressions for unknown/modified observations, stale terminal polls, active restart,
  local observation deadlines, frozen-account recovery, quota rejection, missing cancellation
  identity and a nonzero helper exit after synthetic provider acceptance.
- A 16 MiB halfway-transfer failure/reopen/explicit-recovery test preserving the original
  result pin, exact final bytes and one original compute submission.
- Executable evidence and recovery guides linking requirements to tests while separating
  immutable receipts, current state, uncertainty, cancellation, collection and retention.
  Local deadlines, cleanup dry runs and synthetic helper/account effects remain explicitly
  distinct from production overall-job deadlines, remote apply and live-provider acceptance.
- M4-01 offline/read-only foundation: explicitly allowlisted environment credential references,
  fresh scoped access and callback-buffer clearing without ambient source fallback.
- Immutable non-secret instance preflight configuration, local Python/client/SDK metadata
  checks before credential access, and server-account verification before quota-data reads.
- A fixed isolated SDK helper with stdin-only tokens, ordered HTTPS request allowlist,
  redirect/retry/proxy restrictions, bounded strict responses and an independent watchdog.
- Finite `cmd/kagglepreflight` utility with local default, explicit authenticated-read opt-in,
  interrupt/SIGTERM handling and sanitized dated reports that always keep batch readiness false.
- Configuration, credential lifetime, process isolation, deadline, redaction and real pinned-SDK
  fixture tests; preflight guide and ADR-0015 disclose the private transport seam and live gates.
- M4-02 preparation-only Stager using frozen provider plans, intent-derived resource names,
  exact marker/catalog identities and pre-credential byte/hash/EOF/Close checks.
- Bounded one-shot private dataset creation through the pinned SDK, with separate private/
  processing/catalog and marker-first/all-payload verification before readiness.
- Read-only staging recovery with no reupload/create/version/delete fallback; the existing
  M3 journal pins discovered resource IDs and retains ambiguous or public-resource evidence.
- Real SQLite/admission/blob integration tests for ownership-before-helper, failed/lost
  acknowledgements, reopen/profile remapping and resource-replacement rejection; separate
  SDK HTTP fixtures and isolated leaf-process framing/stream/deadline tests.
- Staging guide and ADR-0016 documenting component authority, signed-storage restrictions,
  transient upload-ticket limits and unchanged live/production batch gates.
- M4-03 per-attempt Executor with locked remote source, stable intent-derived kernel names,
  original ready/private staging rechecks and one SDK save under NEW M3 submission authority.
- Exact numeric kernel ID/version/source/account/privacy observation and read-only recovery;
  raw-field status checks avoid fabricated SDK defaults and retain unknown cancellation/release.
- A remote-only marker-gated bootstrap for the unchanged finite runner, plus explicit Go asset
  embedding/digest checks excluding bytecode caches and unrelated files.
- Execution tests for concurrent calls, ambiguous save responses, real helper framing/deadlines,
  actual pinned-SDK mocked HTTP, bootstrap wiring and generated runner-manifest validation.
- Real SQLite/blob/dispatch intent-before-helper, lost acknowledgement, restart/remapping and
  stale/replacement observation tests. Latest raw UNKNOWN and stronger confirmed attempt state
  are tested separately without changing the existing M3 merge semantics.
- Execution guide and ADR-0017 documenting upsert/CAS, same-version identity, dataset mount,
  free-only quota, cross-binary source recovery and separate full-Provider/live acceptance limits.
- M4-04 serialized read-only Monitor with explicit account verification and conservative GPU
  quota normalization from raw nanosecond durations, including used and reserved capacity.
- Quota freshness and missing/unavailable distinctions without inferred reset time or numerical
  defaults; existing SQLite exhaustion survives unknown/stale reads and restart.
- Bounded version-scoped provider log snapshots with exact identity checks before/after reads,
  token-literal redaction, UTF-8 limits, explicit truncation and reference/snapshot-bound cursors.
- Independent capability evidence, frozen timeout-layer reporting and reference-validated manual
  cancellation without guessing session IDs, invoking provider mutations or claiming termination.
- Operational tests for real isolated helper protocols, pinned-SDK mocked HTTP, precision and
  missing fields, log identity races, pagination and persistent quota/control receipt semantics.
- Operational guide and ADR-0018 documenting delayed snapshots, non-reservation quota, unsupported
  batch cancellation and unverified provider timeout/live support separately from implementation.
- M4-05 read-only ArtifactReader with complete version-scoped listing, manifest-first original
  attempt/output selection and snapshot-bound pagination without following listing URLs.
- Explicit file/version SDK downloads with bounded credential-free signed storage, independent
  byte checks and final identity/terminal/process acknowledgement before successful transfer.
- M3 collection integration preserving immutable pins, original receipts and atomic publication;
  tests cover scoped reads, 16 MiB partial/late/wrong bytes, same-pin restart and lost pin or
  publication acknowledgements without another compute attempt or submission.
- Artifact protocol/process and real pinned-SDK HTTP fixtures for path/cursor/budget rejection,
  full pagination, final status loss after complete bytes and selected failed-payload evidence.
- Artifact guide and ADR-0019 separating candidate metadata, verified local bytes and published
  results, with temporary-sink, repeated-read, range-resume and live/runtime limitations.
- M4-06 finite acceptance adapter and CLI composing the real component ports for one original
  fixed GPU job, with separate local, authorized submit and read-only resume/collect modes.
- Two-file CUDA matrix example with immutable challenge/input identity, exact arithmetic and
  bounded result/hardware evidence; no CPU fallback, package installation or remote internet.
- Original executable/state binding, create-only submission/resume process records and a new
  current-operate-authorized journal inspection API returning no mutation permit.
- Real durable acceptance fixtures for lost responses, one-attempt restart, explicit late-transfer
  recovery and marker failures; actual child processes separately test local CLI reopen and nonce
  freshness. Synthetic tensor/record/CLI negatives cannot claim actual live GPU acceptance.
- Acceptance guide and ADR-0020 documenting operator authorization, report semantics, retained
  resources, original-binary recovery and the explicit not-run/blocked live M4-06/M1 ledger.

### Changed

- Increased the commit-subject and PR-title ceiling from 72 to 80 characters at the owner's
  request, preserving existing history and all other format rules. Added 16 regression checks
  to the existing convention workflow; synchronized the canonical policy and commit template.
- Synchronized living documentation for merged M4-01 through M4-05 and M4-06's PR #24 harness
  review gate, without declaring live acceptance or importing the obsolete staging checkpoint.
  ADR-0015 through ADR-0019 record owner acceptance; historical decision/evidence text is retained.
- Updated OpenAPI collection descriptions and their content lock without changing wire
  schemas, the fifteen-handler inventory or the production-runtime implementation gate.
- Extended the shared event enum and contract lock for `result.expired`; no cleanup or
  event HTTP route is introduced. Contributor guidance records pushed feature-branch
  checkpoints rather than relying on disposable local runtime state.
- Included fault qualification in the existing developer `check` task without changing
  CI workflows, dependencies, migration bytes, public contracts or production runtime logic.
- Extended only the existing Kaggle-client workflow's path filters to include the embedded
  preflight helper package. Tests remain offline, permissions stay read-only and dependency
  pins, lockfiles, runner assets, public contracts and database migrations are unchanged.
- Corrected the process-isolation test to compare cwd/home filesystem identity rather than
  textual path spelling, preserving the isolation requirement across canonical path aliases.
- Fixed safely closable sanitized staging redirects and rejected local input Close failures
  before credential lookup. M4-02 staging changes no dependency or migration; the separately
  approved convention adjustment adds only a validator regression step to its existing workflow.
- M4-03 adds no dependency, workflow, public contract or migration changes; original Python
  runner asset/lock bytes and existing durable state-machine implementations remain unchanged.
- M4-04 likewise preserves dependency/workflow/public-contract/migration and original execution/
  runner sources. New optional ports reuse the existing identity, quota and durable-control rules.
- M4-05 retains those dependency/workflow/contract/migration/source boundaries and reuses M3
  collection logic unchanged. Unknown SDK status decoding now fails artifact qualification
  explicitly; a default enum cannot authorize collection before or after transfer.
- M4-06 adds no dependency, migration, workflow or public HTTP/schema change. Existing M3 mutation
  state machines and original provider helper/runner sources remain intact; journal inspection
  is additive and cannot grant a new dispatch permit.
- Added the explicit formatting-only `style` subject category with five additional regressions
  (21 total), preserving the 80-character ceiling and all other syntax checks. This avoids rewriting
  a pushed formatting checkpoint; the canonical policy is synchronized in a separate docs commit.

[Unreleased]: https://github.com/vankhaivn/compute-relay/commits/main
