# Scope and requirement traceability

> **Status:** M-0 traceability baseline.
>
> **Authority:** requirements are derived from the approved
> [`proposal.md`](proposal.md). This document does not replace the proposal and does not
> claim that the runtime or provider path has been implemented.

## Approved product in one page

Compute Relay is an open-source, self-hosted control-plane runtime that runs beside an
operator's applications. Applications submit finite Python or explicit Linux shell jobs
through a provider-neutral HTTP/JSON contract. The runtime owns durable admission,
immutable input preparation, provider dispatch, observation, recovery, and verified
artifact collection.

Kaggle is the first provider to prove, not a core-domain dependency. Operators use their
own accounts and credentials. The reference workflow has no maintainer-hosted control
plane, shared quota pool, mandatory paid infrastructure, hidden billed fallback, automatic
compute retry, provider switch, or silent CPU downgrade.

The MVP is finite batch execution. It is not remote VRAM, a transparent CUDA proxy, an
always-on model server, a remote desktop, a hosted SaaS, a public multi-tenant scheduler,
or an account/quota circumvention system.

## Traceability rules

- The IDs below are stable project references for plans, ADRs, tests, issues, and PRs.
- The proposal remains authoritative when a summary below omits detail.
- “Acceptance evidence” describes the minimum proof; it is not a claim that the proof
  already exists.
- Provider-related acceptance must state whether evidence is offline, upstream
  documentation, or an authorized live observation.
- A material change to an owner-approved requirement needs explicit owner approval.
  Implementation defaults may change through an ADR without changing approved scope.

## Requirement-to-component-to-test matrix

| ID | Proposal source | Requirement | Primary components or documents | Acceptance evidence | Target |
|---|---|---|---|---|---|
| PRD-01 | O-01, §§3.1–3.3 | Distribute an OSS runtime that operators host and operate themselves. | packaging, release, install docs | Fresh-machine install and use without a maintainer service. | M-5/M-6 |
| PRD-02 | O-02, §§5.1, 15 | Run as a sidecar process with a language-neutral HTTP/JSON boundary. | `cmd`, API, auth, clients | Node.js, Python, and Go clients use the same API and artifact model. | M-2/M-5 |
| PRD-03 | O-03/O-04, §§6–7 | Keep applications and the common domain independent of Kaggle concepts and credentials. | domain, app services, provider ports | Fake provider completes the lifecycle; no provider-name branches in handlers or common transitions. | M-2 |
| PRD-04 | O-05 | Prove one complete Kaggle path before adding another production provider. | Kaggle adapter, feasibility report | Sanitized private-input → bounded GPU execution → verified output evidence. | M-1/M-4 |
| PRD-05 | O-06, §§6.2, 21 | Implement the control plane in Go; direct install is primary and Docker is optional. | Go module, CLI/runtime, packaging | Native builds and developer commands work on claimed targets without Docker. | M-2/M-5 |
| PRD-06 | O-07/O-12, §§4, 9 | MVP supports finite Python and explicit Linux shell jobs. | job schema, runner, validation | Contract tests and live smoke jobs for supported kinds; no retained worker loop. | M-2/M-4 |
| PRD-07 | O-09, §§18.4–18.6 | One trusted operator may isolate several applications by workspace. | auth, workspace services, store | Cross-workspace object/job/artifact/operation access is denied. | M-2 |
| PRD-08 | O-15/O-16, §§3.3, 18 | Require no paid connector service; operators supply provider accounts and credentials. | config, credentials, docs | Reference path uses only local runtime/storage and the operator account; warnings are visible. | M-5/M-6 |
| PRD-09 | O-10/O-11, §§12–14 | Preserve jobs and identity across restart; never silently rerun compute or switch provider. | store, scheduler, reconciliation | Crash/fault tests show no second dispatch after ambiguity and no hidden fallback. | M-3 |
| PRD-10 | §§4.3–4.4, 28 | Exclude hosted SaaS, dashboards, workflow DAGs, transparent CUDA, anti-idle, account rotation, and retained sessions from MVP. | roadmap, review policy | Scope review shows no mandatory implementation or compatibility promise for excluded features. | All |
| JOB-01 | §§9.1–9.3 | Accept a versioned, provider-neutral, immutable job specification and resolved profile snapshot. | schemas, domain, API | Schema tests reject unknown/invalid fields; accepted jobs cannot mutate code, inputs, or provider binding. | M-2/M-3 |
| JOB-02 | §§9.3–9.5 | Execute a non-empty argument vector without implicit shell evaluation; paths remain relative and bounded. | validation, runner | Unit tests cover shell-injection strings, path traversal, reserved variables, and working-directory containment. | M-2 |
| JOB-03 | §§9.6–9.7 | Treat resource/network/timeout requirements as hard requirements and never silently enable internet or downgrade GPU to CPU. | profiles, validation, runner | Unsupported jobs fail before dispatch where knowable; post-start mismatch yields a structured failure. | M-2/M-4 |
| JOB-04 | §§10.5, 13.2 | Freeze bundle and input bytes before dispatch and reuse the exact snapshot for an explicit retry. | object store, job/attempt model | Mutable-URL and retention tests prove retries cannot fetch replacement bytes silently. | M-2/M-3 |
| JOB-05 | §§11.1–11.4 | Run one finite attempt through a generic versioned runner and produce an identity-bearing result manifest. | remote runner, manifest schema | Runner tests cover setup, payload, timeout, failure, missing manifest, and required output rules. | M-1/M-2 |
| DAT-01 | O-08, §10.1 | Support streamed upload, allowlisted local import, and bounded public HTTPS ingestion before compute. | API, transfer, object store | Component tests prove immutable publication and reject incomplete, oversized, or unauthorized sources. | M-2 |
| DAT-02 | §§10.2, 19.3 | Package immutable `.tar.gz` bundles safely; reject links, traversal, duplicates, bombs, and unsafe paths. | packaging, archive validation | Malicious archive corpus passes containment and size/count tests on supported host path semantics. | M-2/M-6 |
| DAT-03 | §§10.4, 19.5 | Protect URL ingestion against SSRF, unsafe redirects, credential leakage, and unbounded transfer. | transfer, policy | Tests cover DNS/IP changes, private/link-local/metadata targets, redirect revalidation, and size/time budgets. | M-2/M-6 |
| DAT-04 | §§8.5, 10.6 | Stage only complete private provider inputs and distinguish upload completion from provider readiness. | provider adapter, resource ledger | Live K-03/K-04 evidence confirms privacy, checksums, and readiness before GPU submission. | M-1/M-4 |
| DOM-01 | §§5, 12–13 | Model immutable jobs, explicit attempts, submission intents, provider resources, operations, events, objects, and artifacts. | domain, persistence | Domain tests preserve attempt history and separate compute retry from reconcile/collect. | M-2/M-3 |
| DOM-02 | §12 | Keep orchestration, execution observation, result availability, cancellation intent, and remote activity/release evidence distinct. | domain state, API responses | Transition tests retain `unknown`, `blocked`, `reconciling`, and `needs_attention` without false terminal states. | M-2/M-3 |
| DOM-03 | §12.5 | Centralize monotonic state transitions and write state plus sequenced event atomically. | domain, store | Tests reject illegal/backward transitions and accept terminal observations that skip intermediate states. | M-3 |
| DUR-01 | §§13.1, 20 | Persist metadata in SQLite and immutable bytes in local filesystem storage. | SQLite store, blob store, migrations | Restart/component tests preserve all durable entities and recover staged file publication. | M-3 |
| DUR-02 | §13.3 | Bind compute-creating admission to a workspace-scoped idempotency key and canonical request hash. | API, app service, store | Same key/request returns original identifiers; changed request returns `IDEMPOTENCY_CONFLICT`. | M-3 |
| DUR-03 | §§13.4–13.6 | Persist submission identity and intent before remote side effects; reconcile uncertainty instead of resubmitting. | scheduler, provider port, reconciliation | Fault injection around every submission boundary produces no automatic duplicate execution. | M-3/M-4 |
| DUR-04 | §13.7 | Allow one runtime process per state directory and avoid database transactions across network/file transfers. | process lock, store, workers | A second process fails clearly; concurrency and crash tests leave recoverable state. | M-3 |
| API-01 | §§15.1–15.2 | Provide `/v1` REST-like metadata endpoints and streamed binary transfer with loopback default. | HTTP server, OpenAPI | OpenAPI/schema checks and component tests cover required endpoint inventory and limits. | M-2 |
| API-02 | §§15.1, 18.4 | Require bearer authentication and authorize every workspace-scoped resource reference. | middleware, workspace auth | Cross-workspace matrix covers profiles, jobs, attempts, events, objects, logs, artifacts, and operations. | M-2 |
| API-03 | §§15.4–15.9, 17 | Return durable asynchronous acceptance, explicit operations, stable error codes, and no compute side effects from `GET`. | API, app services | HTTP tests cover `202`, `409`, `422`, `429`, `503`, request IDs, and safe recommended actions. | M-2/M-3 |
| PRV-01 | §7.1 | Define a provider-neutral contract for check, validate, prepare, submit, observe, reconcile, artifacts, and cleanup. | provider interfaces, registry | Fake-provider contract suite runs without importing Kaggle code. | M-2 |
| PRV-02 | §§7.2, 8.4 | Represent submission as accepted/rejected/unknown and bind each attempt to a prewritten unique remote identity. | provider contract, Kaggle adapter | Ambiguous-submit test enters reconciliation; retrieved manifest proves the correct attempt. | M-1/M-3/M-4 |
| PRV-03 | §§7.3–7.6 | Advertise capabilities as supported/unsupported/unknown with conditions and evidence. | descriptors, profiles, provider docs | Contract tests cover providers lacking cancel, live logs, or quota without successful-looking stubs. | M-2/M-4 |
| OPS-01 | §§14.1–14.2 | Use a durable bounded queue and conservatively count possibly active attempts against account capacity. | scheduler, leases, policy | Fairness/backpressure tests and ambiguous-state capacity tests pass. | M-3 |
| OPS-02 | §§14.3–14.4 | Model quota as known/unknown/stale/unavailable with units, source, and observation time. | provider quota, policy, API | Missing data is never interpreted as zero or unlimited; strict/default policies are tested. | M-3/M-4 |
| OPS-03 | §§14.5–14.6 | Apply separate bounded timeouts and zero automatic compute retries/provider fallbacks. | config, scheduler, runner | Timeout-layer tests prove a poll timeout cannot cause resubmission and limits are never silently enlarged. | M-3/M-4 |
| OPS-04 | §14.7 | Persist cancellation intent; report cancellation only with evidence; never delete resources as cancellation. | operations, adapter capability | Race/unsupported tests produce `too_late`, `manual_required`, or unresolved states honestly. | M-3/M-4 |
| OPS-05 | §§20.5–20.6 | Clean only ledger-owned, identity-checked, resolved resources; remote dry-run is the default. | resource ledger, cleanup service | Idempotent cleanup tests preserve unknown/active resources and tolerate already-absent owned resources. | M-3/M-6 |
| SEC-01 | §§18.3, 19.2, 19.6, 19.8 | Keep provider credentials out of jobs, SQLite, bundles, commands, logs, artifacts, and diagnostics. | credentials, subprocess boundary, logging | Secret canary tests and diagnostic export checks find no credential material. | M-2/M-6 |
| SEC-02 | §§19.2–19.5 | Bind to loopback by default, authenticate, bound expensive requests, and never execute workload code locally. | server, middleware, packaging, fake provider | Security tests cover exposure defaults, request limits, command construction, archive safety, and SSRF. | M-2/M-6 |
| SEC-03 | §§19.7, 19.10 | Keep provider resources private and reject quota evasion, hidden browser APIs, anti-idle, and account rotation. | Kaggle adapter, policy, docs | Generated metadata and code review preserve privacy and supported official interfaces only. | M-1/M-4/M-6 |
| VER-01 | §§23.1, 23.5 | Keep default CI offline using unit, component, fake-provider, fixture, and fault tests. | CI, test harnesses | Default CI uses no credentials or provider quota and labels evidence tier explicitly. | M-2/M-3 |
| VER-02 | §§23.2–23.3 | Cover the enumerated failure scenarios and critical no-duplicate/no-false-status invariants. | fault tests, state tests | Fault matrix is executable and linked to each relevant requirement ID. | M-3/M-6 |
| VER-03 | §§23.4–23.6 | Gate live read/compute tests behind explicit authorization and finite budgets; never ask for secrets in chat. | live harness, feasibility report | Sanitized evidence records versions, procedures, results, costs/limits, and unobservable gaps. | M-1/M-4/M-6 |
| DX-01 | §21 | Provide runtime/CLI, init, doctor, config validation, direct install, optional Docker, and cross-platform developer commands. | CLI, config, scripts, release | Claimed platform matrix passes native build/test and official-client prerequisite checks. | M-5/M-6 |
| DX-02 | §22 | Keep GPU smoke, small LLM batch, video follow-up, and thin clients outside core business logic. | examples, client samples | Smoke and LLM examples produce verified outputs; three clients exercise one common API contract. | M-5 |

## Coverage review

Every implementation task in [`implementation-plan.md`](implementation-plan.md) must cite at
least one requirement ID. A requirement may have several tests, but no required behavior
may depend solely on a live provider test when deterministic offline validation is possible.

The initial matrix intentionally does not assign optional retained sessions, dashboards,
SDK generation, a second production provider, or workflow DAGs to an MVP milestone.
