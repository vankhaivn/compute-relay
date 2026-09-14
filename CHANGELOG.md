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
- Initial ADRs for the official-client boundary, attempt-scoped Kaggle identity, and
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

[Unreleased]: https://github.com/vankhaivn/compute-relay/commits/main
