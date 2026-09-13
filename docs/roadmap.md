# Outcome roadmap

> **Status:** approved outcome scaffold. Dates, sprint estimates, and detailed tasks have not been assigned.

This roadmap follows acceptance gates rather than a calendar. Some research and core scaffolding may proceed in parallel, but extensive abstraction or product polish must not outrun proof of the first real compute path.

## M-0 — Evidence and scope

**Outcome:** repository instructions are active; approved requirements are traceable; current official Kaggle surfaces have been reviewed; feasibility risks and candidate transport decisions are recorded.

**Exit evidence:**

- proposal requirements mapped to components and tests;
- Kaggle feasibility report populated with source versions and evidence levels;
- initial ADRs for material implementation choices; and
- no live capability presented as verified without an authorized test.

## M-1 — Thin real-provider proof

**Outcome:** the smallest supported private GPU job path is proven before building a broad abstraction around assumptions.

**Exit evidence:**

- authorized private input and multi-file code reach one bounded execution;
- actual GPU computation is demonstrated, not only device listing;
- terminal provider evidence and correct-attempt output checksums are recorded;
- local restart does not create a second execution; and
- no hidden paid or maintainer-operated dependency participates.

## M-2 — Portable core

**Outcome:** a versioned provider-neutral job model and application boundary exist independently of Kaggle details.

**Exit evidence:**

- workspace authorization, immutable object ingestion, generic job validation, and fake provider;
- contract tests for providers with missing cancellation, logs, or quota; and
- no Kaggle branches in common HTTP or domain state logic.

## M-3 — Durable orchestration

**Outcome:** queueing, attempts, idempotency, submission intent, recovery, and artifact-only retries survive local failures.

**Exit evidence:**

- SQLite-backed durable state and filesystem blob lifecycle;
- restart and ambiguous-submission fault tests;
- no automatic compute rerun or provider fallback; and
- verified attempt-scoped artifact publication.

## M-4 — Integrated Kaggle batch

**Outcome:** the validated Kaggle lifecycle is implemented inside the durable provider-neutral core.

**Exit evidence:**

- capability, identity, state, error, quota, log, and cancellation behavior matches recorded evidence;
- private staging and readiness are safe and explicit;
- unsupported or unknown capabilities remain visible; and
- collection and recovery preserve attempt identity.

## M-5 — Usable developer product

**Outcome:** an operator can install, configure, diagnose, run, and integrate the connector without provider-specific application code.

**Exit evidence:**

- CLI, doctor/preflight, configuration, direct-install path, and optional container path;
- bounded GPU smoke test and small open-access LLM batch example;
- thin Node.js, Python, and Go HTTP clients; and
- documented cross-platform developer commands and validated compatibility claims.

## M-6 — Release hardening

**Outcome:** security, retention, cleanup, compatibility, evidence, and release artifacts support an honest first release.

**Exit evidence:**

- security and failure-case tests, ownership-safe cleanup, backup/migration guidance, and support diagnostics;
- sanitized live-provider acceptance record;
- reproducible release artifacts and checksums; and
- release documentation that distinguishes implemented, verified, unknown, and out-of-scope behavior.

## Stop/go rules

- Proceed with the Kaggle batch adapter only when private staging, bounded execution, identity-safe observation, and result collection are supportable through authorized interfaces.
- Ship missing optional capabilities as explicitly unsupported or unknown rather than faking cancellation, logs, or quota.
- If live credentials are unavailable, continue offline architecture, tests, fixtures, and the executable live-test procedure while marking live acceptance `blocked-environment`.
- Do not silently redesign the project into a hosted service, paid fallback, browser automation system, or always-on notebook worker to avoid a feasibility limitation.
