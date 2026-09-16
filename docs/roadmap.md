# Outcome roadmap

> **Status:** acceptance-based roadmap active; synchronized 2026-09-16.
>
> **Current milestone:** M-3 — durable orchestration; M3-01 through M3-07 merged,
> M3-08 fault qualification in review in PR #18. M-1 live acceptance remains blocked.
>
> Dates and sprint estimates are intentionally not assigned without an execution context.

This roadmap follows acceptance gates rather than a calendar. Research and provider-neutral
core work may overlap after M-0, but extensive abstraction or product polish must not outrun
proof of the first real compute path.

## Milestone status

| Milestone | Status | Gate |
|---|---|---|
| M-0 — Evidence and scope | `complete` | PR #1 merged the traceability, evidence, ADR, risk, compatibility and dependency-aware plan set. |
| M-1 — Thin real-provider proof | `blocked` for live acceptance | M1-01 offline harness merged in PR #3; authorized private bounded GPU path with identity-safe verified outputs is still required. |
| M-2 — Portable core | `complete` offline | M2-01 through M2-09 merged through PR #10; provider-neutral API/domain/objects/auth/fake-provider and finite runner evidence, not a live-provider claim. |
| M-3 — Durable orchestration | `in-review` for offline acceptance | M3-01 through M3-07 merged in PRs #11–#17; PR #18 supplies executable 25-scenario qualification and recovery documentation. Exact-head checks and owner merge close this offline gate only. |
| M-4 — Integrated Kaggle batch | `not-started` | Evidence-matching Kaggle adapter inside the durable core. |
| M-5 — Usable developer product | `not-started` | CLI, doctor, install, examples, and three thin clients. |
| M-6 — Release hardening | `not-started` | Security, compatibility, cleanup, diagnostics, and reproducible release proof. |

Detailed task dependencies and authorization boundaries are in
[`implementation-plan.md`](implementation-plan.md). PR #18 stops for owner merge. Passing
offline qualification does not automatically start M4 or authorize provider side effects.
Production `serve`, artifact HTTP/CLI, remote cleanup apply and staging preview remain
separate gates; the matrix does not claim those behaviors exist.

## M-0 — Evidence and scope

**Outcome:** repository instructions are active; approved requirements are traceable;
current official Kaggle surfaces have been reviewed; feasibility risks and candidate
transport/default decisions are recorded.

**Artifacts:**

- [`scope-and-requirements.md`](scope-and-requirements.md)
- [`research/kaggle-interface-review.md`](research/kaggle-interface-review.md)
- [`research/kaggle-feasibility.md`](research/kaggle-feasibility.md)
- [`risk-register.md`](risk-register.md)
- [`compatibility.md`](compatibility.md)
- ADR-0001 through ADR-0003 under [`decisions/`](decisions/README.md)
- [`implementation-plan.md`](implementation-plan.md)

**Exit evidence:**

- proposal requirements mapped to components, tests, and milestones;
- Kaggle feasibility report populated with exact source versions and evidence levels;
- initial material decisions recorded with alternatives and verification;
- dependency-aware task plan separates offline work from credential/compute tasks;
- no live capability presented as verified without an authorized test; and
- the M-0 PR is merged with required checks passing.

**Current conclusion:** the offline M-0 package is merged in PR #1. No live capability was
upgraded by that merge. M-2's offline tasks have subsequently merged; M-1 live tasks remain
blocked until explicit authorization and recorded provider evidence.

## M-1 — Thin real-provider proof

**Outcome:** prove the smallest supported private GPU job path before building a broad
adapter around assumptions.

**Exit evidence:**

- reproducible pinned official-client environment and sanitized command/response fixtures;
- authorized private input and multi-file code reach one bounded execution;
- actual GPU computation is demonstrated, not only device listing;
- terminal provider evidence and correct-attempt output checksums are recorded;
- status, logs, quota, timeout, cancellation, and release-observability conclusions match
  the tested environment;
- local restart/ambiguous response does not create a second execution;
- test-owned resource cleanup is identity-safe; and
- an ADR records the adapter go/blocked decision and final transport mapping.

M-1 does not require cancellation or live logs to exist. It requires their absence or limits
to be represented honestly. M1-01's merged credential-free harness does not close this gate.

## M-2 — Portable core

**Outcome:** a versioned provider-neutral job model and application boundary exist
independently of Kaggle details.

**Exit evidence:**

- pinned Go module/toolchain and offline CI;
- strict job/result/config schemas and OpenAPI baseline;
- domain states, structured errors, and provider ports;
- workspace authorization and loopback-authenticated API;
- immutable streamed object ingestion, safe bundles/local import, and protected HTTPS
  ingestion;
- generic runner contract; and
- fake-provider contract tests, including providers with no cancellation/live logs/quota.

No Kaggle package or provider-name branch appears in HTTP handlers or common state
transitions. M2-09 in PR #10 closed this offline milestone without closing live M-1 acceptance.

## M-3 — Durable orchestration

**Outcome:** queueing, attempts, idempotency, submission intent, recovery, operations, and
artifact-only retries survive local failures.

**Exit evidence:**

- SQLite-backed durable metadata, migrations, state lock, backup baseline, and filesystem
  blob lifecycle;
- idempotent admission and immutable job/attempt history;
- bounded fair scheduler and conservative account capacity;
- write-ahead submission intent and accepted/rejected/unknown handling;
- explicit cancel/retry/reconcile/collect operations;
- attempt-scoped verified artifact publication;
- ownership ledger and dry-run cleanup; and
- required crash/ambiguity/disk/stale-observation fault matrix passes.

No automatic compute rerun or provider fallback exists. [M3-05 controls](operations.md) add
immutable receipts, explicit attempt targets, one-shot cancellation and transfer-only tickets.
[M3-06 collection](collection.md) verifies pinned manifests and blobs before atomic artifact
publication. An accepted ticket still does not imply available results. [M3-07 retention](retention.md)
adds conservative pins, bound-store local byte removal and exact-ledger remote dry runs.
Metadata and unresolved recovery material remain retained; a preview never applies remote
deletion.

[M3-08 qualification](fault-matrix.md) maps the 25 numbered proposal scenarios to 34 distinct
root tests. The developer checker compares exact proposal/source identities, executes fresh
uncached tests and rejects missing/skipped/failed evidence. [Recovery semantics](recovery.md)
explains safe actions without inventing production commands. PR #18's final-head CI and
owner merge close the implemented **offline** gate; this is not a full-MVP completion claim.

The evidence remains scoped: local invocation deadlines are not a production overall-job
deadline service; cleanup coverage is dry-run-only; synthetic account/CLI/resource faults
are not live-provider observations. The full test/race suites and later M6 acceptance remain
necessary beyond the nominated matrix roots.

## M-4 — Integrated Kaggle batch

**Outcome:** the M-1-validated Kaggle lifecycle is implemented inside the durable
provider-neutral core.

**Exit evidence:**

- client/config/credential preflight matches the pinned evidence;
- private staging and readiness are safe and explicit;
- submission identity and ambiguity use the common durable semantics;
- capability, raw-state, error, quota, log, timeout, and cancellation mappings match
  fixtures/live evidence;
- paginated collection verifies the correct attempt; and
- a sanitized durable end-to-end acceptance run survives local restart.

Unsupported or unknown capabilities remain visible. M3's offline qualification does not
replace the M1-08 provider go decision or authorize M4 to bypass its prerequisites.

## M-5 — Usable developer product

**Outcome:** an operator can install, configure, diagnose, run, and integrate the connector
without provider-specific application code.

**Exit evidence:**

- unified runtime/CLI, strict configuration, and local/read-only/compute doctor modes;
- direct-install path and optional unprivileged container path;
- cross-platform developer commands;
- bounded GPU smoke test and small pinned open-access LLM batch example;
- thin Node.js, Python, and Go HTTP clients using one contract; and
- first-run documentation from install through verified artifacts.

## M-6 — Release hardening

**Outcome:** security, retention, cleanup, compatibility, evidence, and artifacts support an
honest first release.

**Exit evidence:**

- security and failure-case tests, redacted diagnostics, ownership-safe cleanup,
  backup/migration guidance, and disk/retention behavior;
- native evidence for every claimed host platform;
- current sanitized live-provider acceptance record;
- reproducible artifacts, checksums, SBOM/license notices, support/security policy; and
- final traceability audit distinguishing implemented, verified, unknown, and out-of-scope
  behavior.

## Stop/go rules

- Proceed with the production Kaggle batch adapter only after a supported private input →
  bounded execution → identified terminal result → verified artifact path is demonstrated.
- Ship missing optional capabilities as explicitly unsupported or unknown rather than
  faking cancellation, logs, quota, or hardware release evidence.
- If live credentials are unavailable, continue offline core, fixtures, harnesses, and
  procedures while marking live acceptance `blocked-environment`.
- If private staging, identity-safe collection, or bounded termination fails, record the
  precise block; do not silently redesign the project into a hosted service, paid fallback,
  public-data workaround, browser automation system, or always-on notebook worker.
