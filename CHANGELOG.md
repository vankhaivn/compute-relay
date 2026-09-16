# Changelog

All notable changes to this project will be documented in this file.

The format follows the principles of Keep a Changelog, and released versions will use
Semantic Versioning once a public compatibility surface exists.

## [Unreleased]

### Added

- Initial open-source repository policy and documentation system.
- Approved project proposal and provider-neutral architecture baseline.
- Apache-2.0 license, security policy, governance, support, and contribution guidance.
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

### Changed

- Synchronized living README, roadmap, plan and documentation/ADR indexes with merged M3
  and M4-01's in-review offline/read-only foundation. Preserved the approved proposal and
  dated provider evidence. No M1 live acceptance or integrated batch activation is claimed.
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

[Unreleased]: https://github.com/vankhaivn/compute-relay/commits/main
