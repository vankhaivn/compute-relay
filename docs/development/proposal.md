# Compute Connector — Project Proposal and Agent Handoff

> **Working name:** Compute Connector  
> **Document version:** 0.1  
> **Prepared:** 2026-09-13  
> **Document language:** English  
> **Audience:** An autonomous engineering agent responsible for researching, planning, and implementing the repository  
> **Status:** Approved product direction, with implementation defaults and explicit feasibility gates. This document is not a claim that the software or its proposed commands already exist.

## How to use this document

Read this document completely before preparing the roadmap. It contains the product decisions already agreed with the project owner, default answers to otherwise blocking design questions, and the evidence required before advertising provider capabilities.

The next agent is expected to create its own research notes, architecture decision records, roadmap, and implementation plan from this proposal. **Do not restart product discovery or ask the owner to choose between routine technical alternatives.** Use the defaults here, inspect the target repository, and document any justified deviations.

A distinction applies throughout:

- **Owner-approved requirement:** A product decision already accepted in the discussion; preserve it.
- **Implementation default:** A concrete recommendation supplied by this handoff to eliminate unnecessary questions. The agent may improve it through an architecture decision record without changing the approved scope.
- **Provider fact:** A narrow statement supported by the references at the end of this document. It may change over time.
- **Verification gate:** A behavior that requires evidence from the selected provider/client version and, where relevant, a real account. Do not promote an assumption into a capability claim.

Unless a paragraph explicitly describes a cited provider fact, the architecture, schemas, endpoints, limits, and behaviors below are **project requirements or design proposals**, not descriptions of existing Kaggle functionality.

---

## Contents

1. [Executive summary](#1-executive-summary)
2. [Owner-approved decisions](#2-owner-approved-decisions)
3. [Product positioning, responsibility, and boundaries](#3-product-positioning-responsibility-and-boundaries)
4. [Goals, non-goals, and MVP scope](#4-goals-non-goals-and-mvp-scope)
5. [Terminology and integration model](#5-terminology-and-integration-model)
6. [Architecture and dependency boundaries](#6-architecture-and-dependency-boundaries)
7. [Provider abstraction and capability negotiation](#7-provider-abstraction-and-capability-negotiation)
8. [Kaggle evidence baseline and feasibility gates](#8-kaggle-evidence-baseline-and-feasibility-gates)
9. [Job contract and portable execution environment](#9-job-contract-and-portable-execution-environment)
10. [Packaging, inputs, and data movement](#10-packaging-inputs-and-data-movement)
11. [Remote runner and execution lifecycle](#11-remote-runner-and-execution-lifecycle)
12. [Job state, attempts, and truthful status](#12-job-state-attempts-and-truthful-status)
13. [Persistence, idempotency, and crash recovery](#13-persistence-idempotency-and-crash-recovery)
14. [Scheduling, quota, timeout, and cancellation policy](#14-scheduling-quota-timeout-and-cancellation-policy)
15. [HTTP API and application integration](#15-http-api-and-application-integration)
16. [Artifacts, logs, progress, and event delivery](#16-artifacts-logs-progress-and-event-delivery)
17. [Error model and recovery semantics](#17-error-model-and-recovery-semantics)
18. [Configuration, credentials, and workspaces](#18-configuration-credentials-and-workspaces)
19. [Security, privacy, and abuse boundaries](#19-security-privacy-and-abuse-boundaries)
20. [Local storage, retention, and cleanup](#20-local-storage-retention-and-cleanup)
21. [CLI, installation, platform support, and developer experience](#21-cli-installation-platform-support-and-developer-experience)
22. [Example workloads and application clients](#22-example-workloads-and-application-clients)
23. [Testing, verification, and acceptance criteria](#23-testing-verification-and-acceptance-criteria)
24. [Observability and support diagnostics](#24-observability-and-support-diagnostics)
25. [Repository structure and engineering standards](#25-repository-structure-and-engineering-standards)
26. [Milestone outcomes and planning instructions](#26-milestone-outcomes-and-planning-instructions)
27. [Risks and predetermined responses](#27-risks-and-predetermined-responses)
28. [Future extensions without premature implementation](#28-future-extensions-without-premature-implementation)
29. [Decision register and default answers](#29-decision-register-and-default-answers)
30. [Final definition of done](#30-final-definition-of-done)
31. [Instructions to the implementation agent](#31-instructions-to-the-implementation-agent)
32. [Sources and evidence limitations](#32-sources-and-evidence-limitations)

---

## 1. Executive summary

Build an open-source, self-hosted **compute connector runtime** that runs alongside an application and executes finite jobs on external compute providers.

An application written in Node.js, Python, Go, or another language connects through a small, provider-neutral HTTP/JSON API. The application submits code, an execution command, dependency declarations, input references, and desired outputs. The runtime handles durable admission, input preparation, provider-specific submission, execution tracking, recovery, and artifact collection.

**Kaggle is the first compute provider, not the identity of the product and not a dependency of its core domain.** The architecture must support adding a different legitimate compute source later without rewriting application integrations or putting provider-specific concepts into the common job API.

The owner will distribute source code. The owner will not host a connector service, provision compute accounts for users, hold their credentials, sell quota, or operate a shared GPU backend. Each operator runs their own runtime and uses their own provider account.

The default workflow is:

```text
No queued work
    -> no connector-created persistent GPU session

A job arrives
    -> validate and persist
    -> snapshot/stage code and inputs
    -> submit a bounded remote execution
    -> observe its progress/status
    -> retrieve and verify results
    -> observe remote termination
    -> retain local results according to policy
```

This is not remote VRAM, a transparent CUDA device, a general-purpose remote desktop, or a promise of instant GPU startup. Applications move work through explicit job boundaries; they do not obtain a drop-in replacement for local GPU memory or local process calls.

The first end-to-end workload is a small LLM batch job. A small hardware smoke test precedes it. Video rendering is a later example using the same generic job contract, not a separate branch of business logic in the core.

**Primary product promise:** One local integration contract for submitting, observing, and retrieving finite compute jobs, with honest provider limitations and no mandatory paid infrastructure in the reference workflow.

## 2. Owner-approved decisions

The following decisions were explicitly accepted in the discussion.

| ID | Decision | Consequence |
|---|---|---|
| O-01 | Publish an OSS repository that users download and run themselves. | No maintainer-hosted control plane, user account service, or shared quota pool. |
| O-02 | The connector is a runtime process beside the application, not an embedded library. | HTTP/JSON is the primary application boundary. |
| O-03 | Applications should depend on the connector as little as practical. | No application-side Kaggle SDK, notebook metadata, or credential handling is required. |
| O-04 | Support multiple compute providers architecturally. | Kaggle-specific code stays in its adapter. |
| O-05 | Kaggle is the first real provider. | Verify one complete Kaggle path before adding another production adapter. |
| O-06 | Use Go for the runtime; direct execution is primary, Docker optional. | Do not require Docker merely to submit a job. |
| O-07 | The initial remote job languages are Python and shell. | Node.js/Go applications can submit jobs, but remote Node.js/Go jobs are not an MVP promise. |
| O-08 | Support local files/directories and URL-based inputs. | Provide explicit upload/import and URL ingestion, without required paid object storage. |
| O-09 | One runtime may serve several applications belonging to one operator. | Separate them by workspace; this is not hostile multi-tenant SaaS. |
| O-10 | Persist queue and job state locally. | Restarting the runtime must not erase jobs or automatically create duplicate executions. |
| O-11 | Do not silently rerun jobs or switch providers after failure. | Compute re-execution is an explicit action; safe observational/transfer retries are separate. |
| O-12 | Implement finite batch execution first. | Always-on model servers and streaming inference are outside MVP. |
| O-13 | A future retained-session mode is acceptable as an opt-in feature with quota warnings and limits. | Preserve an extension path, but do not claim this is already technically or contractually possible on Kaggle. |
| O-14 | Use a small LLM batch example as the first meaningful demonstration. | Keep the model and workload in examples, outside the runtime domain. |
| O-15 | The basic workflow must not require paid services. | No required cloud database, broker, tunnel subscription, paid model API, or billing account. |
| O-16 | Users supply their own provider credentials and are responsible for their use. | Include clear terms/privacy warnings and operator documentation. |

The owner's stated motivation is unused Kaggle GPU allowance, described by the owner as approximately **30 hours per week**. Treat that as motivation and an account-specific expectation, **not a hard-coded entitlement or a promise to every user**.

Windows was raised during the technology discussion, but an exact support matrix was not independently specified. The implementation default in this document is cross-platform control-plane support from the first usable release; do not present that detail as a separate owner-selected platform matrix.

## 3. Product positioning, responsibility, and boundaries

### 3.1 What the repository supplies

The repository supplies a runtime, a public job API, provider modules, a generic remote runner, command-line utilities, configuration examples, test fixtures, and application integration examples.

An operator installs it on a machine they already control. That machine may be a personal computer or an operator-managed server. The runtime does not need a local GPU to orchestrate remote work.

### 3.2 What the operator supplies

The operator supplies the host machine, connectivity, provider account, credentials, account verification when required, permission to upload data and code, and lawful rights to execute the workload. The operator also supplies storage capacity for staged inputs and downloaded outputs.

Account creation, credential issuance, acceptance of provider terms, and ownership of input data cannot be fabricated by an engineering agent. These are installation prerequisites, not unresolved product-design questions.

### 3.3 Meaning of “free”

Use this wording in product documentation:

> The reference workflow requires no paid connector service or mandatory paid infrastructure. Operators use their own accounts and available provider allowances. Provider availability, eligibility, quotas, terms, and pricing may change. The project does not guarantee unlimited resources or zero incidental costs for the operator's machine, network, or arbitrary job code.

The runtime must not silently select a billed provider, enable paid capacity, purchase credits, or call a paid model API as a fallback.

The connector's free-only dispatch policy is not a sandbox that can prove arbitrary uploaded code will never contact a paid third-party API. The reference examples must not do so, and the distinction must be documented.

### 3.4 Terms and OSS licensing are different concerns

Kaggle's terms include restrictions concerning personal/non-commercial use and use for third parties. A repository disclaimer is not permission to ignore provider terms. [S-01]

The project should be positioned as a tool for permitted operator-controlled workflows. It must not include account farming, quota evasion, simulated user activity, session-limit bypasses, hidden web automation, or anti-idle mechanisms intended to circumvent restrictions.

The connector's source-code license governs this repository. It does not relicense uploaded data, model weights, third-party dependencies, provider services, or generated results.

**Default source-code license:** Apache-2.0, unless the target repository already has an explicit compatible licensing decision. This is a handoff default, not a license already chosen by the owner in conversation. Preserve third-party notices and original asset/model licenses.

### 3.5 Honest operational language

Say “a bounded remote execution is submitted” rather than “a GPU is guaranteed immediately.” Say “the provider reported a terminal execution state” rather than “billing definitely stopped at this exact millisecond.”

Distinguish ending the application command, ending the remote wrapper, the provider reporting execution completion, and the provider releasing/accounting for hardware. The connector can expose evidence; it must not claim deeper observability than it has.

## 4. Goals, non-goals, and MVP scope

### 4.1 Goals

1. Keep application integration language-neutral and independent of provider internals.
2. Execute useful bounded GPU work through the first real adapter.
3. Preserve jobs and execution identity across local failures.
4. Prevent accidental duplication and silent semantic changes.
5. Make inputs, artifacts, logs, and errors usable without provider-specific application code.
6. Make missing capabilities explicit before dispatch whenever possible.
7. Remain operable with a local process, local storage, and the operator's provider account.
8. Make new providers additive modules with a reusable contract-test suite.

### 4.2 MVP deliverables

The minimum usable release includes:

| Area | Required outcome |
|---|---|
| Runtime | Go process exposing authenticated loopback HTTP API, health endpoints, and graceful shutdown. |
| Persistence | SQLite-backed queue, jobs, attempts, events, resources, and recovery state; filesystem-backed blobs/artifacts. |
| Workspace support | More than one application workspace, with scoped credentials and namespace isolation. |
| Job submission | Versioned generic job specification; Python/shell command; code bundle; local/URL input paths. |
| Kaggle integration | Official supported client path for private staging, bounded execution, status reconciliation, and result collection. |
| Provider abstraction | A real Kaggle adapter plus a deterministic fake provider for contract tests. |
| Results | Verified artifacts, execution manifest, exit/error details, and logs that are actually available. |
| Recovery | Durable submission intent, ambiguous-submit handling, restart reconciliation, and artifact-only retry. |
| Guardrails | No automatic compute rerun, no provider fallback, finite time limits, safe defaults, and clear unknown states. |
| UX | CLI, configuration example, doctor/preflight, packaging utility, and explicit manual retry/cancel/reconcile controls. |
| Examples | GPU smoke test, small LLM batch workload, and thin Node.js/Python/Go HTTP clients. |
| Quality | Tests, compatibility evidence, developer commands, installation guide, troubleshooting, and OSS warnings. |

Best-effort provider log/quota integrations belong in the adapter when validated. They must not prevent the core batch path from existing when a provider cannot supply them; the API must return an explicit unsupported, unknown, or unavailable result.

### 4.3 Non-goals for MVP

Do not build a hosted SaaS, billing system, Kubernetes operator, public multi-tenant scheduler, distributed cluster, web dashboard, marketplace, workflow DAG engine, GUI video studio, LLM chat product, OpenAI-compatible inference server, transparent CUDA proxy, or general remote development environment.

Do not require a VPN, reverse tunnel, inbound connection into the local machine, or always-on relay for batch execution. Do not implement browser-based Kaggle automation as a substitute for supported API behavior.

Do not implement checkpoint/resume semantics for arbitrary user code, cross-provider migration of running processes, automatic multi-account rotation, GPU reservation guarantees, or an unlimited remote storage service.

Do not make arbitrary OCI images a mandatory part of the common execution contract. A provider may execute code inside its managed environment without accepting a custom container.

### 4.4 Extensibility is not feature completeness

A second production provider is not required to release MVP. The fake provider and boundary tests must demonstrate that the architecture is not a Kaggle wrapper disguised by renaming classes.

Retained sessions should remain a documented future resource model. They must not force the MVP to implement queues inside remote notebooks or a permanent remote worker.

## 5. Terminology and integration model

| Term | Meaning |
|---|---|
| Operator | The person or organization controlling one runtime installation and its provider accounts. |
| Application | A consumer of the runtime API, such as an LLM batch tool or video-rendering application. |
| Workspace | A namespace for an application's jobs, inputs, artifacts, and API access. It is not a VM or a hostile-tenant security boundary. |
| Runtime | The local Go control-plane process. |
| Provider | An adapter type, such as `kaggle`, implementing a defined compute lifecycle. |
| Provider instance | One configured adapter with credential references, policies, and account binding. |
| Compute profile | A provider-neutral alias exposed to apps, resolving to a configured provider instance and resource requirements. |
| Job | An immutable requested unit of business work with a durable identity. |
| Attempt | One explicit execution attempt for a job. Compute retries create new attempts. |
| Bundle | An immutable uploaded archive containing execution code and declared files. |
| Input object | An immutable runtime-managed input blob, imported/uploaded/fetched before dispatch. |
| Artifact | An output file produced by an attempt and verified into local runtime storage. |
| Runner | Generic code executed remotely to prepare the environment, run the command, and write result metadata. |
| Submission intent | A durable record that remote submission may occur or may already have occurred. |
| Reconciliation | Observation used to resolve local state against provider state, without blindly resubmitting work. |
| Capability | A typed provider behavior such as batch execution, cancellation, or quota reporting. |

### 5.1 Typical deployment

```text
Operator-controlled machine
┌───────────────────────────────────────────────────────────────┐
│ App A: Node.js video tool                                      │
│ App B: Python batch-LLM client                                 │
│ App C: Go application                                         │
│              │                                                │
│              │ HTTP + JSON + binary uploads/downloads          │
│              ▼                                                │
│       Compute Connector runtime                               │
│       ├── API/auth/workspaces                                 │
│       ├── job queue and reconciliation                         │
│       ├── local SQLite and artifact store                      │
│       └── provider adapter                                    │
└───────────────────────────┬───────────────────────────────────┘
                            │ Outbound provider/API traffic
                            ▼
                    Kaggle, initially
                    ├── private staged inputs
                    ├── bounded execution + generic runner
                    └── completed outputs/status
```

The runtime and an application may share a machine without sharing a filesystem namespace. Uploads must therefore work even when one component runs in a container.

### 5.2 Minimal application responsibilities

An application needs the runtime base URL, a workspace-scoped API token, a bundle/input preparation step, a job specification, and code to poll state and download results.

It must not need a Kaggle username, a notebook slug, the Kaggle CLI, direct provider credentials, or knowledge of provider filesystem paths. Optional diagnostic details may expose provider names, but normal control flow must use generic fields.

Official SDKs are optional future conveniences. The reference clients should prove the API is usable without SDKs.

## 6. Architecture and dependency boundaries

### 6.1 Suggested layers

```text
Public adapters
    HTTP API / local CLI
            │
Application services
    admission / workspaces / job operations / artifact access
            │
Domain and orchestration
    job model / attempt model / state rules / scheduler / reconciliation
            │
Ports
    Provider / Store / BlobStore / CredentialResolver / Clock / EventSink
            │
Infrastructure
    Kaggle adapter / fake provider / SQLite / filesystem / process runner
```

The composition root wires concrete implementations. The core domain must not import Kaggle packages, notebook JSON types, shell command builders, or the Python client bridge.

HTTP handlers must not contain provider scheduling logic. The Kaggle adapter must not own the durable queue or write arbitrary domain database rows. The provider may return normalized observations and an opaque provider-owned reference; orchestration persists and interprets them through the common contract.

### 6.2 Recommended local components

Use a single Go binary for the API and operational CLI. Use SQLite rather than an external broker/database for the first release. Use the local filesystem for large blobs rather than storing large byte arrays in SQLite.

The Kaggle adapter may need a Python environment and official Kaggle client. **“Go runtime” does not imply that the entire Kaggle integration is a dependency-free Go binary.** Make this prerequisite visible and keep it out of consuming applications.

Start with compiled-in provider modules rather than a dynamic plugin ABI. A provider registry allows new modules to register capabilities and configuration schemas. Avoid reflection-heavy registries or generic extension frameworks unless a demonstrated need appears.

### 6.3 Control plane versus workload code

The control plane handles credentials, state, transfer, orchestration, and provider calls. It never executes uploaded business commands locally as part of admission or validation.

The remote runner executes business code on the selected provider. It does not need the runtime's HTTP token or the provider account credential. Batch execution must not rely on the runner calling the local runtime back.

### 6.4 Core architectural invariants

- API requests are admitted durably before a successful asynchronous response is sent.
- Job specifications and resolved input bytes become immutable before remote dispatch.
- Provider selection is explicit through a profile; there is no hidden fallback.
- Every remote execution is attributable to one durable attempt and submission intent.
- Unknown execution status is represented as unknown, not guessed as failure or success.
- Workspace checks apply to all referenced resources, not just the top-level job endpoint.
- Artifact availability is separate from remote command success.
- Cancellation intent is separate from confirmed remote termination.
- Cleanup never doubles as cancellation.
- The reference workflow has no maintainer-operated dependency.

## 7. Provider abstraction and capability negotiation

### 7.1 Provider-neutral responsibilities

A provider must offer enough behavior to validate a job against its environment, prepare provider-owned resources, submit a bounded execution, inspect an identified attempt, enumerate/retrieve completed artifacts, and describe its limitations.

Optional capabilities include remote cancellation, live logs, quota reporting, stronger execution identity, or retained sessions. Their absence must not be hidden behind a successful-looking stub.

Suggested interface shape, intentionally architectural rather than copy-and-paste Go code:

```text
Provider
    Describe() -> ProviderDescriptor
    Check(ctx, instanceConfig) -> DiagnosticReport
    Validate(ctx, resolvedJob) -> ExecutionPlan | Rejection
    Prepare(ctx, plan, operationIdentity) -> PreparedResources
    Submit(ctx, preparedResources, submissionIdentity) -> SubmissionOutcome
    Observe(ctx, remoteReference) -> Observation
    ReconcileSubmission(ctx, submissionIdentity) -> ReconciliationOutcome
    ListArtifacts(ctx, remoteReference, cursor) -> ArtifactPage
    FetchArtifact(ctx, remoteReference, artifactReference, destination) -> TransferResult
    Cleanup(ctx, ownedResourceReference) -> CleanupOutcome

Optional interfaces
    Cancel(ctx, remoteReference) -> CancellationOutcome
    ReadLogs(ctx, remoteReference, cursor) -> LogPage
    ReadQuota(ctx, accountScope) -> QuotaObservation
```

The agent may split these into smaller Go interfaces. It must preserve the semantics, especially explicit ambiguity in `Submit` and conditional support for `Cancel`.

### 7.2 Submission outcomes

A provider submission does not return only “success or error.” It must distinguish:

| Outcome | Meaning | Required orchestration behavior |
|---|---|---|
| `accepted` | Evidence identifies a submitted execution or uniquely managed execution resource. | Persist the remote reference and observe it. |
| `rejected` | Evidence proves that the provider did not accept execution. | Record the cause; classify as terminal failure or a pre-execution blocked condition. |
| `unknown` | The request may have reached the provider, but the response did not establish its outcome. | Enter reconciliation; never blindly repeat the submission. |

A network timeout or CLI process termination after sending a request is not automatically a rejection.

### 7.3 Capability data

Expose capability support as `supported`, `unsupported`, or `unknown`, accompanied where useful by conditions and evidence. Do not use a single unconditional boolean when account setup, client version, or execution mode affects support.

```json
{
  "provider_type": "kaggle",
  "instance_id": "personal-kaggle",
  "capabilities": {
    "batch": {"support": "supported"},
    "python": {"support": "supported"},
    "shell": {"support": "unknown", "reason": "Requires remote runner verification"},
    "private_input_staging": {"support": "unknown"},
    "remote_cancel": {"support": "unknown"},
    "logs_after_completion": {"support": "unknown"},
    "logs_while_running": {"support": "unknown"},
    "quota_reporting": {"support": "unknown"},
    "custom_container": {"support": "unsupported"},
    "retained_sessions": {"support": "unsupported"}
  },
  "evidence": {
    "client_version": null,
    "checked_at": null,
    "account_checked": false
  }
}
```

This is a sample pre-verification descriptor, not the final capability report. The existence of a documented feature permits implementing its probe; it does not establish successful access for the configured account.

A request requiring a capability known to be unsupported must fail before dispatch with a structured error. A requirement that can only be verified after allocation must be explicitly marked in the execution plan and checked before the business command starts.

### 7.4 Capability categories

Cover execution modes, languages, GPU visibility, resource-selection granularity, outbound network control, filesystem constraints, execution timeout, private staging, secret injection, cancellation, output retrieval, output limits, live/completed logs, quota reporting, and execution identity.

A provider may support observing available VRAM without supporting a reservation for a requested minimum VRAM. Represent those as different facts.

### 7.5 Profiles hide provider-specific choices

An application selects a generic profile such as `default-gpu`. Runtime configuration maps it to an instance, accelerator preference, and policy:

```text
Application: profile = default-gpu
Runtime:     default-gpu -> personal-kaggle -> verified accelerator choice
```

An operator may later remap this profile to another provider for **future submissions**. The already-resolved provider/configuration snapshot of an accepted job must not change beneath it. A retry of the same immutable job uses the original provider binding by default; changing provider is a new job/explicit clone operation, not an invisible retry variation.

Do not put Kaggle notebook slugs or dataset source syntax into the normal application contract. Provider-specific extensions, when eventually necessary, must be namespaced, optional, validated, and clearly marked non-portable. MVP should not need them in application examples.

### 7.6 Extensibility test

The fake provider must be able to complete the same generic job lifecycle using predetermined fixtures without importing the Kaggle module. Tests must also cover a provider with no cancellation, no live logs, and unknown quota.

Adding an adapter may require adding a provider-specific package, configuration block, capability descriptor, and contract tests. It should not require adding `if provider == "kaggle"` branches to HTTP handlers or domain state transitions.

## 8. Kaggle evidence baseline and feasibility gates

### 8.1 Narrow evidence available when this proposal was written

The following are documentation/source observations as of 2026-09-13, not results of running jobs with the owner's account.

| Evidence | Project implication |
|---|---|
| Official kernel documentation describes push, status, output, accelerator selection, and a maximum-runtime option. Some status/output operations concern the latest run. [S-03] | A batch adapter is plausible; preserve exact attempt identity and test timeout behavior. |
| Kernel metadata describes script/notebook execution, private visibility, GPU/network options, and attached sources. [S-04] | Generate metadata inside the adapter; never require app authors to supply it. |
| Dataset commands support creation/versioning and private creation by default. [S-05] | Private input staging is a candidate transport, subject to account, limits, and readiness checks. |
| Dataset metadata requires a license selection and supports copyright/other-license labels. [S-06] | Never stamp arbitrary user data with a permissive license merely to make uploads succeed. |
| Authentication documentation includes OAuth, an API-token environment variable/token file, and legacy credentials. [S-02] | Use a verified official authentication path; do not hard-code legacy credentials as the only method. |
| The official changelog records additions for kernel logs and GPU/TPU quota. [S-07] | Investigate these existing features instead of assuming they do not exist. |
| Current output-format docs enumerate JSON support for certain commands, including quota, but not every kernel operation. [S-08] | Do not append a guessed `--json` or `--format json` flag to every command. |
| The source includes quota retrieval and formatting; account data and client behavior still need a live test. [S-09] | Expose observed quota with timestamps and uncertainty, not fabricated values. |
| The reviewed CLI command list does not expose a kernel-cancel command; an upstream proposal discusses session-identity obstacles. [S-10] [S-11] | Treat reliable remote cancellation as unproven for MVP, not as a trivial wrapper. |
| Kaggle publishes its managed Python CPU/GPU environment source. [S-12] | Check actual runtime compatibility rather than assuming a custom image can be uploaded. |

These observations do not establish a service-level guarantee, exact startup latency, universal GPU availability, account entitlement, or an approved use case. Notebook documentation is an important current reference, but its dynamic page was not fully extractable during preparation; recheck the actual page during implementation. [S-13]

### 8.2 Official integration path: default decision

Use the official Kaggle Python client/CLI as the initial adapter transport, isolated behind a small Go-owned boundary.

Start with a pinned CLI invoked using argument arrays, explicit environment, bounded output capture, and controlled working directories. Where CLI output is not machine-readable or cannot expose sufficient identity, use a **small pinned Python bridge invoking public official client APIs** and returning structured JSON on stdout. Keep diagnostics on stderr.

This bridge is an internal adapter detail, not another network service and not a general Python dependency exposed to consuming applications. Select CLI-only or the bridge through an architecture decision after the first feasibility spike. Do not rewrite undocumented browser APIs in Go simply to preserve a marketing claim of a single binary.

Audit any automatic retries performed inside the selected client/SDK for side-effecting calls. Disabling retries in the Go scheduler is insufficient if an underlying transport can replay an ambiguous compute submission. Configure or isolate mutation calls so their retry behavior is explicit and tested; do not claim duplicate-dispatch safety until this is understood. Likewise, validate structured results rather than treating every zero CLI exit code as proof of a successful remote operation.

Record the tested Kaggle package version, transitive SDK version, Python version, and upstream commit/tag used for evidence. Pin released versions for reproducibility rather than following mutable `main` in production.

### 8.3 Required feasibility report

Create `docs/development/research/kaggle-feasibility.md` before freezing the adapter contract. Every probe should record the question, source/client version, exact test procedure, sanitized result, capability conclusion, and fallback.

| Gate | Question to verify | Minimum evidence / predetermined response |
|---|---|---|
| K-01 Authentication | Can the configured official credential access the operator's required resources? | Read-only authenticated probe; missing credentials become actionable setup diagnostics. |
| K-02 GPU eligibility | Can this account submit a finite GPU-enabled job? | Real smoke test proving CUDA computation; no CPU substitution. |
| K-03 Code and private inputs | Can a multi-file bundle and private input reach the same execution correctly? | Synthetic files, remote checksum verification, and confirmed private visibility. |
| K-04 Readiness | How are upload completion and dataset processing readiness detected? | Readiness polling test; never start GPU work while inputs are known unready. |
| K-05 Identity | What does submit return, and how is an attempt rediscovered after a lost response? | Unique-resource test, version/run metadata if available, and an ambiguous-submission experiment. |
| K-06 Status | Which raw states exist and what do they mean? | Captured fixtures plus one observed lifecycle; unrecognized values map to unknown. |
| K-07 Artifacts | Can outputs be retrieved completely, including multiple files/pages? | Manifest check, pagination test, digest validation, and private output access. |
| K-08 Logs | Are logs available during execution, only afterward, or inconsistently? | Poll a deliberately slow synthetic job; do not infer streaming from a logs command name. |
| K-09 Timeout | Does the requested provider-side maximum runtime actually bound a run? | Small bounded experiment with runner fallback; never test by consuming hours unnecessarily. |
| K-10 Termination | What evidence remains after the wrapper exits? | Provider terminal observation plus any available release/accounting evidence; document gaps. |
| K-11 Cancellation | Is there a supported cancellation operation and a reliably obtainable target identity? | Both must be proven; otherwise advertise unsupported/unknown and provide manual guidance. |
| K-12 Quota | What are the returned units, reset time, age, and account scope? | Tested official quota query; failed/missing data is unknown, not zero or unlimited. |
| K-13 Environment | Which Python, shell, framework, CUDA, GPU, and filesystem properties exist? | Environment manifest and execution tests; no assumption of package/image parity. |
| K-14 Offline operation | Does a staged job work without remote internet? | Run a small job from attached inputs with no remote network requirement. |
| K-15 Restart | Can another runtime process continue tracking an existing attempt? | Kill/restart local runtime after submit; no second compute submission. |
| K-16 Cleanup | Can only connector-owned completed staging resources be removed safely? | Ownership ledger, dry-run plan, and idempotent cleanup test. |

### 8.4 Identity policy for Kaggle MVP

Default to **one uniquely named remote execution resource per attempt**, with a deterministic provider-safe identifier derived from the runtime installation ID and attempt ID. Persist that identifier before submission.

Never repeatedly push unrelated jobs to a shared notebook slug and then retrieve “latest output.” An isolated resource reduces ambiguity, but does not make the upstream operation exactly-once: blindly repeating a push to the same slug can still create a new execution/version.

Include job ID, attempt ID, bundle/input digests, and a random non-secret attempt nonce in the remote execution manifest. Check them on retrieval. Do not allow a stale artifact from another run to satisfy success criteria.

Where stronger version/session identifiers are supported, persist them as opaque adapter data and use them. If the operator manually edits/reruns a managed resource, detect identity mismatch and stop automatic collection rather than guessing which run is authoritative.

### 8.5 Private staging policy

Preferred MVP data transport is private provider-managed staging of immutable code/input bundles, followed by attachment to a bounded execution. The exact Kaggle mechanism must pass K-03 and K-04.

Default metadata should preserve ownership and existing license information. A candidate Kaggle label is `copyright-authors`, with a description that existing licenses remain applicable; verify correctness for the staged contents and current platform behavior. [S-06] Do not use `CC0` as a universal upload workaround.

If a private transport cannot be made reliable through supported interfaces, document the block and do not silently publish data or require a paid storage service.

### 8.6 Do not conflate unrelated Kaggle products

The first provider executes notebook/script workloads. It is not an integration with a hosted model-proxy or benchmark inference credit system. Do not confuse accelerator allowance with any other model/API quota exposed by Kaggle.

## 9. Job contract and portable execution environment

### 9.1 Contract principles

The job specification must describe business work independently of the provider. It is immutable after acceptance, except for separately modeled operational actions such as cancellation or explicit retry.

Use a versioned JSON schema and generated/validated OpenAPI contract. Suggested specification version is `compute-connector/v1alpha1`; the HTTP API is `/v1`. These are working names, not previously published compatibility guarantees.

### 9.2 Proposed job specification

The following is a complete illustrative request for already-uploaded objects. IDs are examples rather than live references.

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "small-llm-batch",
  "profile": "default-gpu",
  "labels": {
    "application": "llm-client",
    "purpose": "example"
  },
  "bundle": {
    "object_id": "obj_example_code_bundle"
  },
  "execution": {
    "kind": "python",
    "command": ["python", "main.py", "--max-new-tokens", "32"],
    "working_directory": ".",
    "environment": {
      "BATCH_SIZE": "1"
    },
    "dependencies": {
      "python_requirements": "requirements.txt"
    }
  },
  "inputs": [
    {
      "name": "prompts",
      "source": {"kind": "object", "object_id": "obj_example_prompts"},
      "target": "prompts.jsonl"
    }
  ],
  "outputs": [
    {"path": "responses.jsonl", "required": true},
    {"path": "metrics.json", "required": true}
  ],
  "resources": {
    "accelerator": "gpu",
    "minimum_gpu_count": 1
  },
  "network": {
    "remote_internet": "required"
  },
  "timeouts": {
    "remote_wall_seconds": 900,
    "setup_seconds": 300,
    "finalization_grace_seconds": 60
  }
}
```

The example's internet requirement is explicit because a selected model or dependencies may be downloaded remotely. A fully staged example should set it to `disabled` only when the provider can satisfy that request. Default jobs have remote internet disabled; validation rejects incompatible setup instead of silently enabling it.

### 9.3 Field semantics

| Field | Contract |
|---|---|
| `name` | Human-readable label; not a unique ID and not a provider slug. |
| `profile` | Workspace-allowed compute profile; resolved and snapshotted before dispatch. |
| `labels` | Bounded non-secret string metadata; never executed or used as filesystem paths. |
| `bundle.object_id` | Immutable code archive owned by the workspace. |
| `execution.kind` | `python` or `shell` for MVP; both run through the generic wrapper. |
| `execution.command` | Non-empty argument vector; no implicit shell evaluation. |
| `working_directory` | Relative directory within the extracted code root; `.` is default. |
| `environment` | Non-secret string variables; reject reserved runner/control-plane names. |
| `python_requirements` | Relative path inside the bundle; installed remotely, never on the runtime host. |
| `inputs[].source` | An uploaded/imported object or a permitted HTTPS URL to be ingested locally. |
| `inputs[].target` | Unique relative path beneath the remote input root. |
| `outputs[].path` | Relative file path, or an explicitly declared directory type, beneath the output root. |
| `outputs[].required` | Missing required output prevents success; optional output may be absent. |
| `resources` | Hard requirements when specified; no silent CPU fallback or VRAM promise. |
| `network` | Explicit requirement, not a guarantee that every arbitrary egress rule is enforceable. |
| `timeouts` | Positive bounded durations; rejected if unsupported or outside policy, not silently enlarged. |

Do not accept unknown top-level fields silently. Validate sizes, counts, encoding, path lengths, and enum values. Server responses may add compatible informational fields; client examples should ignore unknown response fields.

### 9.4 Remote directory contract

Applications use these generic environment variables:

```text
CC_JOB_ID
CC_ATTEMPT_ID
CC_CODE_DIR
CC_INPUT_DIR
CC_OUTPUT_DIR
CC_SCRATCH_DIR
CC_EXECUTION_MANIFEST_PATH
```

Suggested logical layout:

```text
remote-job-root/
├── code/       extracted bundle, working-directory base
├── inputs/     staged immutable inputs
├── outputs/    declared application outputs
├── scratch/    disposable intermediate data
└── control/    runner metadata/logs/result manifest
```

Physical paths may differ by provider. The adapter supplies the mapping; the application must not use `/kaggle/...` directly.

Do not promise read-only input filesystem enforcement if the provider/runner cannot enforce it. Input immutability means the submitted snapshot is immutable; remote copies may be mutable within the workload environment.

### 9.5 Commands and shell behavior

Python jobs use an argument vector such as `["python", "main.py"]`. Shell jobs use an explicit interpreter such as `["bash", "render.sh"]`. Do not concatenate untrusted values into a local shell string.

MVP shell jobs target the supported remote Linux shell, not Windows PowerShell. Windows support concerns the local runtime and CLI; it does not imply Windows execution on Kaggle.

Argument vectors are not shell-expanded. Application code should read the `CC_*` environment variables directly. Document this to avoid examples that pass literal `$CC_INPUT_DIR` as though it will automatically expand.

### 9.6 Dependencies and environment reproducibility

Dependencies belong to the job bundle, not to the application integration or runtime host. Prefer pinned requirements and a verified remote installation strategy. Record resolved package versions, Python version, accelerator information, and relevant framework/CUDA versions in the execution manifest.

The agent must investigate whether a remote virtual environment can reuse provider GPU packages safely, whether isolated installs are feasible, and how setup time affects quota. Do not blindly reinstall an incompatible GPU framework over the managed environment.

For MVP, support Python requirements and explicit shell setup commands only within the remote environment. OS package installation/root privileges are not guaranteed. Fail clearly when a job requires unavailable system dependencies.

A pinned job does not guarantee bit-for-bit reproducibility across changing provider images or nondeterministic GPU operations. Record provenance rather than promising more than the system controls.

### 9.7 Resource requirements

Require a GPU when the job declares one. The runner verifies actual device visibility and the requested capabilities before starting business logic. A successful CPU execution must not count as a successful GPU test.

Keep exact accelerator model selection in runtime profile configuration unless the application truly needs it. Avoid encoding a list of supposedly free GPU models in the common API.

When a provider cannot guarantee a minimum resource before allocation, validation reports `verify_after_start`. A mismatch becomes `RESOURCE_REQUIREMENT_UNSATISFIED`, with setup/allocated time recorded. Never downgrade silently.

## 10. Packaging, inputs, and data movement

### 10.1 Three input paths

Support these paths in MVP:

1. **Upload:** The client streams a file/archive to the runtime and receives an immutable object ID. This is the portable default.
2. **Local import:** The runtime snapshots an allowed local path under an operator-configured import root. This is an opt-in convenience for shared-filesystem deployments.
3. **HTTPS ingestion:** The runtime downloads an allowed HTTPS resource before remote submission, verifies it, and stores an immutable input object.

The HTTP job request must not force clients to embed large files as base64 JSON.

### 10.2 Bundles

Default bundle format is `.tar.gz`, with a versioned bundle manifest describing files, sizes, digests, and execution metadata where appropriate. The agent may choose ZIP if cross-platform tooling materially improves, but must preserve archive-safety requirements and document the choice.

The packaging utility should select explicitly included files from a project root and respect a `.computeignore` file. Default exclusions should include VCS directories, environment files, credential files, dependency caches, build outputs, editor caches, and the runtime's own state.

A default ignore file is not a secret scanner and cannot guarantee that no secrets are bundled. Provide a file-list/digest preview and bounded secret-pattern checks for obvious credential material. Reject symlinks for MVP rather than trying to preserve complex host filesystem semantics.

Never automatically package an entire home directory or source repository merely because the caller supplied its parent path.

### 10.3 Local import semantics

Local import is disabled unless an import root is configured. The request selects a named root and a relative path, not an unrestricted absolute host path.

Snapshot bytes into runtime storage before treating the input as ready. A file changing mid-read must produce a clear failure or a verified consistent snapshot; do not silently race against the application's writes.

A successful import means the runtime owns the copied bytes. It does not mean future edits to the original file will appear in an accepted job.

### 10.4 URL semantics

MVP supports public HTTPS inputs without embedded credentials. An optional expected SHA-256 digest should be accepted. Store the resolved digest and final byte count even when none was supplied.

Download URL inputs on the runtime host before GPU allocation, then stage the resulting snapshot. This keeps input transport independent of the remote notebook's network mode and avoids exposing local runtime addresses to remote compute.

Enforce SSRF protections, redirect limits, connect/read/total timeouts, byte limits, DNS/IP checks, and no redirects to local/private/link-local/metadata destinations. Validate each redirect target and the actual connection endpoint. Reject credentials in URLs; signed/private URLs and arbitrary request headers can be deferred until a secure secret-reference design exists.

Do not store sensitive query strings in ordinary logs. An expired URL must not cause the runtime to invent another source or silently change the job's input bytes.

### 10.5 Snapshot identity and retries

All attempts of the same job must use the same frozen bundle and input bytes. If an input URL is mutable, freeze its bytes before the first dispatch. A later retry must not silently fetch a different version from the URL.

If the original input snapshot has expired from retention, retry is rejected with an actionable error. Re-ingesting changed data creates a new job rather than mutating history.

### 10.6 Provider staging

Stage only complete, verified inputs. Record each remote staging resource before or as it is created through the operation ledger. Record readiness separately from upload completion.

The default implementation may use one immutable staging resource per attempt or job. Cross-job deduplication is optional and should not be required for the first working pipeline. Correct cleanup and identity are more important than upload optimization.

A local upload success is not proof of remote dataset readiness. Provider processing delays belong to `preparing`, not `running`.

### 10.7 Data-volume constraints

Expose local limits and known provider limits. Unknown provider limits must be labeled unknown. Use streaming I/O and bounded buffers for large video assets. Reject oversized jobs before remote dispatch whenever the size is already known.

Do not silently truncate outputs or upload only the first page of files. Large models/videos are not assumed to fit simply because GPU compute is available.

## 11. Remote runner and execution lifecycle

### 11.1 Runner responsibilities

The remote runner is generic and versioned. It must:

- Load a validated resolved job manifest and verify bundle/input identities.
- Create the logical directory layout and expose the `CC_*` environment contract.
- Verify resource/environment requirements before business execution.
- Perform bounded dependency/setup work remotely.
- Launch the explicit command without introducing extra shell evaluation.
- Capture stdout/stderr and optional structured progress safely.
- Enforce the runner's own deadline and supervise child processes.
- Write declared outputs and an execution-result manifest.
- Exit after one finite attempt, including failure paths.

It must not poll the local application for more jobs, receive provider credentials, automatically restart a failed payload, or keep the GPU session alive after useful work finishes.

### 11.2 Execution sequence

```text
API admission
    -> persist job + attempt
    -> snapshot local/URL inputs
    -> validate profile/capabilities
    -> stage private provider resources
    -> wait for staging readiness
    -> persist submission intent
    -> submit bounded remote execution
    -> persist acceptance/reference or submission ambiguity
    -> observe provider state
    -> runner prepares environment and executes command
    -> runner writes manifest/logs/outputs and exits
    -> observe provider terminal state
    -> retrieve and verify artifacts locally
    -> finalize job outcome
    -> asynchronously eligible cleanup, executed by the running runtime
```

“Eligible cleanup” means normal work performed while the operator's runtime is running; it is not a maintainer-hosted background service.

### 11.3 Result manifest

The runner must produce a machine-readable result file even when the payload fails, when the environment permits it. Suggested shape:

```json
{
  "manifest_version": "1",
  "job_id": "job_example",
  "attempt_id": "att_example_1",
  "attempt_nonce": "example-non-secret-identity-value",
  "runner_version": "0.1.0",
  "bundle_sha256": "example-digest",
  "input_manifest_sha256": "example-digest",
  "started_at": "2026-09-13T11:00:00Z",
  "finished_at": "2026-09-13T11:02:00Z",
  "phase": "completed",
  "exit_code": 0,
  "timed_out": false,
  "resource_check": {"gpu_required": true, "gpu_verified": true},
  "artifacts": [
    {"path": "responses.jsonl", "bytes": 1234, "sha256": "example-digest"}
  ],
  "error": null
}
```

Digest strings above are illustrative, not syntactically valid production hashes. Actual schemas require fixed-format digest values and validated IDs.

This manifest establishes identity/integrity against accidental confusion, not cryptographic attestation against malicious code running in the same remote environment. MVP assumes operator-trusted workloads.

### 11.4 Failure handling in the runner

Preserve the original payload exit code and failure stage. Do not replace a dependency-install failure with a generic “job failed.” Write a structured error and logs where possible, then exit.

A hard provider kill, out-of-memory event, or infrastructure loss may prevent the result manifest from being written. The runtime must use provider evidence and report the missing manifest honestly, rather than fabricating an exit code.

### 11.5 Process supervision

The remote wrapper should terminate the payload's process group on timeout and wait only for a bounded finalization interval. Child processes must not intentionally escape into a permanent service. Verify the approach in the provider environment.

A runner watchdog can stop its own process tree; it cannot guarantee provider hardware release, survive every kill mode, or act as a complete security sandbox. Provider-side timeout is a second control when supported.

## 12. Job state, attempts, and truthful status

### 12.1 Do not collapse independent state dimensions

Expose at least these dimensions:

1. **Orchestration status:** What the local runtime is doing.
2. **Execution observation:** What the provider/runner has established.
3. **Result availability:** Whether artifacts are present and verified locally.
4. **Cancellation/deadline intent:** What was requested or exceeded.
5. **Remote activity/release evidence:** What is known, not known, or only provider-reported.

A download error after successful remote execution does not mean the computation must run again. A lost polling connection does not mean the remote execution failed.

### 12.2 Proposed orchestration states

| State | Meaning |
|---|---|
| `queued` | Accepted durably and waiting for local admission/dispatch. |
| `preparing` | Snapshotting inputs, checking requirements, staging, or waiting for staging readiness. |
| `dispatching` | A durable submission intent exists and submission may be in flight. |
| `submitted` | Provider accepted the execution; it may still be queued/starting remotely. |
| `running` | Provider or runner evidence establishes active execution. |
| `collecting` | Execution outcome is known sufficiently to collect/verify results. |
| `blocked` | A recoverable condition prevents safe progress before new execution. |
| `reconciling` | Runtime is resolving incomplete or stale observations. |
| `cancelling` | Cancellation was requested and a supported operation is being observed. |
| `needs_attention` | Automatic safe progress is exhausted; the situation is unresolved, not falsely terminal. |
| `succeeded` | Payload success, required verified outputs, and provider terminal evidence all agree. |
| `failed` | A confirmed terminal failure or proven pre-submission failure is recorded. |
| `cancelled` | Work never submitted after cancellation, or remote cancellation is confirmed. |
| `timed_out` | A local pre-execution timeout occurred, or execution timeout and terminal evidence are established. |

`needs_attention`, `blocked`, and `reconciling` are nonterminal. They preserve the possibility that remote compute is still active. Do not use terminal `failed` merely to make a client polling loop stop.

### 12.3 Cancellation and completion races

Cancellation requests are operational events, not immediate terminal transitions. If completion wins the race, the attempt may end `succeeded` or `failed` with cancellation status `too_late`. A terminal job cancelled after completion must not have its historical outcome rewritten.

A local request deadline can be exceeded while the provider remains active. Expose `deadline_exceeded=true`, request cancellation when supported, and retain a nonterminal/attention state until termination is known.

### 12.4 Remote activity and hardware release

Suggested status fields:

```json
{
  "remote": {
    "execution_state": "unknown",
    "last_observed_at": null,
    "terminal_reported_at": null,
    "may_be_active": null,
    "release_evidence": "not_observable"
  },
  "result": {"status": "not_available"},
  "cancellation": {"status": "not_requested"},
  "deadline_exceeded": false
}
```

`may_be_active` is nullable: `true` means evidence/pending submission makes activity possible or established, `false` requires evidence of no active managed execution, and `null` means insufficient information. The exact schema may be refined, but unknown must remain representable.

A provider terminal status can establish execution termination for the adapter contract while precise hardware-release/accounting time remains `not_observable`. This distinction prevents success criteria from requiring an impossible billing probe while still avoiding claims that GPU consumption stopped at an invented timestamp.

### 12.5 Transition rules

State transitions must be centralized and tested. Every meaningful transition writes the new state and a sequenced event in one transaction. Store a state revision for optimistic concurrency.

Remote observations can skip intermediate states: a short job may be seen first as completed. Never require seeing `running` before accepting a validated terminal result. Conversely, a stale observation must not move a completed attempt backward to running.

## 13. Persistence, idempotency, and crash recovery

### 13.1 Durable entities

Use SQLite for at least:

```text
runtime_installation
workspaces
api_token_hashes
provider_instances / non-secret configuration references
jobs
attempts
submission_intents
provider_resources
operations
events
objects
artifacts
quota_observations
scheduler_leases / dispatch ownership
schema_migrations
```

Physical table names are delegated. Keep object bytes in filesystem storage and their metadata/digests/ownership in SQLite.

### 13.2 Job and attempt model

A job contains the immutable requested specification, resolved profile snapshot, frozen inputs, and an active/latest attempt pointer. Each attempt has its own ID, submission identity, provider reference, state, timestamps, errors, manifests, and artifacts.

Default is one attempt with no automatic compute retry. A user-requested retry creates a new attempt under the same job and preserves previous attempt history. A retry uses the same frozen inputs and original provider binding. Changed code, data, requirements, or provider selection require a new job or explicit clone.

Terminal attempt records remain immutable. An explicit retry may change the parent job summary to reflect a new active attempt, but it must not rewrite the previous attempt's terminal outcome or historical events.

Do not overwrite old attempt outputs with new attempt outputs. The job response must identify which attempt is active and which attempt each artifact belongs to.

### 13.3 HTTP idempotency

Require or strongly encourage `Idempotency-Key` for job creation; the implementation default is to require it for all compute-creating operations. Scope it to workspace plus operation type.

Store the canonical request hash and original result. Reusing the same key with the same request returns the original job/attempt. Reusing it with a different request returns `409 IDEMPOTENCY_CONFLICT`.

Persist the idempotency record in the same transaction as job/attempt creation. Retain it at least as long as job metadata. This prevents duplicate local admission after a client loses the HTTP response; it does not automatically make provider submission idempotent.

### 13.4 Write-ahead remote submission

Before contacting the provider, persist the attempt, selected profile/config revision, immutable input identities, deterministic remote resource name, attempt nonce, and submission intent state.

After submission, persist accepted identity, proven rejection, or unknown outcome. If the runtime crashes between the remote side effect and this commit, startup reconciliation must use the prewritten intent to find the existing resource.

Do not reset all `dispatching` jobs to `queued` on startup.

### 13.5 Recovery cases

| Crash/failure point | Required behavior after restart |
|---|---|
| Before durable acceptance | Client can retry creation using the same idempotency key. |
| After acceptance, before input preparation | Continue preparation from immutable source records; do not lose the job. |
| During object write | Remove/quarantine incomplete temporary files; never expose them as complete objects. |
| During provider staging | Reconcile the recorded staging identity; avoid untracked duplicate datasets. |
| After intent, before proven submit | Inspect intent and remote identity; only submit when non-acceptance is established. |
| After provider acceptance, before local reference commit | Reconcile by persisted identity; never automatically push again. |
| While remote compute runs | Resume observation of the same attempt. |
| During artifact download | Resume/retry collection without rerunning compute. |
| After artifact rename, before DB commit | Recover through staged manifest/digest checks or orphan sweep. |
| During cleanup | Retry only known owned resources; already absent is an idempotent outcome. |

### 13.6 Exactly-once claims

Do not advertise exactly-once execution. The intended behavior is **durable local admission with idempotency, no automatic duplicate dispatch after uncertainty, and explicit reconciliation of remote side effects**.

Distributed uncertainty cannot be solved by retrying everything. When the system cannot prove whether execution happened, preserve the ambiguous state and require an explicit resolution rather than spending quota again.

### 13.7 Concurrency and storage ownership

MVP uses one runtime process per state directory. Acquire an OS-level process lock; a PID file alone is not sufficient proof of ownership. A second process targeting the same directory must fail clearly.

Use transactional dispatch claiming and bounded workers within that process. SQLite settings, driver choice, transaction isolation, durability mode, migration strategy, and backup approach must be documented and tested on the supported platforms.

Never hold a database transaction open during a network request or a long file transfer. No secret values should be persisted in job rows or logs.

## 14. Scheduling, quota, timeout, and cancellation policy

### 14.1 Default scheduler

Use a durable local queue with a conservative default of one potentially active remote attempt per provider account. This is a connector policy, not a statement of Kaggle's concurrency limit.

Use FIFO within a workspace and simple round-robin selection among eligible workspaces. Avoid introducing priorities, preemption, or distributed scheduling in MVP. Limit global/per-workspace queue size and return clear backpressure errors instead of accepting unbounded work.

A queued or running job in an application is not the same as an allocated GPU. Keep local preparation, provider queue wait, actual execution, and local result retrieval distinguishable.

### 14.2 Capacity accounting

Count submitted, running, cancellation-pending, and submission-ambiguous attempts against account capacity until observation establishes otherwise. A local timeout or operator closing the application must not immediately free a slot when remote activity remains possible.

If two configured instances point to the same provider account, concurrency/quota policies should use a common account scope when that identity is verifiable. Do not advertise coordination across separate runtime installations; other tools or notebooks can consume the same account allowance.

### 14.3 Quota model

Represent quota observations with resource class, limit/used/remaining values, units, reset timestamp when supplied, observation timestamp, source, precision, and confidence/status. Normalize units explicitly.

Possible states are `known`, `unknown`, `stale`, and `unavailable`. Do not interpret missing quota as zero or unlimited. The current official source formats quota values in hours; an adapter using formatted values must preserve their reduced precision rather than pretending they are exact seconds. [S-09]

Separately maintain local estimates for managed attempts. A local runtime cannot account for all external notebooks or determine a provider's exact accounting rules merely from elapsed payload time.

### 14.4 Unknown and exhausted quota

Default policy for a free-only provider is:

- Known exhausted allowance: block new dispatch, show the observed reason/reset information, and avoid repeated submissions.
- Unknown allowance: allow a bounded, eligible free-only job with a visible quota warning unless the operator selects strict quota mode.
- Strict quota mode: block dispatch until fresh sufficient quota can be established.
- Provider rejection: record the rejection; do not repeatedly resubmit automatically.

If the job was only locally blocked before any submission, fresh evidence can make it dispatchable without creating a new attempt. If the provider has rejected a submission, require an explicit resume/retry action before another compute-creating request under the default policy.

Do not turn “30 hours per week” into a countdown computed from runtime uptime. Do not promise the reset occurs on a particular weekday or timezone without provider evidence.

### 14.5 Timeout layers

Implement separate limits for API calls, local input ingestion, provider operations, remote setup, remote wall time, cancellation observation, and artifact transfer. One giant timeout is not sufficient.

Each remote attempt has a finite wall budget and a finalization reserve. Setup and payload execution share that budget. The runner should stop starting new payload work once there is insufficient remaining time to finish safely.

Record the runner deadline origin and provider timeout semantics separately. A provider timeout may include phases the runner cannot observe; the agent must test and document this rather than treating all clocks as identical.

If the requested duration exceeds configured/provider limits, reject it or return an explicit validation adjustment that the client must accept. Never silently increase a limit. Default examples should remain small even when the account allows longer execution.

### 14.6 Default local policy values

These numbers are proposed operational defaults, **not Kaggle limits or measured performance claims**. They are configurable and may be adjusted by the agent with documented rationale.

| Setting | Default |
|---|---|
| Active attempts per provider account | 1 |
| Queue limit per workspace / global | 100 / 1,000 |
| Default remote wall budget | 1,800 seconds |
| Example GPU smoke-test budget | 120 seconds |
| Example small-LLM budget | 900 seconds |
| Default setup budget | 300 seconds, within wall budget |
| Default finalization reserve | 60 seconds, within wall budget |
| Status poll interval | 15 seconds with jitter |
| Transient observation backoff ceiling | 120 seconds; honor longer provider retry instructions |
| Fresh quota observation age | 5 minutes |
| Local-only blocked-queue recheck | 5 minutes or a relevant configuration/quota event |
| Per ordinary provider control-call timeout | 60 seconds; transfers use separate bounded limits |
| Automatic compute retries | 0 |
| Automatic provider fallback | Disabled |
| Remote retained sessions | Disabled / not implemented in MVP |

A submitted attempt being merely polled must never be resubmitted because a poll exceeded its timeout.

### 14.7 Cancellation semantics

For a locally queued/preparing job with no possible remote execution, cancellation prevents dispatch and can complete immediately after local work is stopped safely.

For an accepted/possibly accepted execution, persist cancellation intent first. If the adapter has verified cancellation support and the required identity, invoke it and observe the result. A successful cancel-request response is not necessarily proof that execution ended.

If cancellation is unsupported or identity is missing, mark the operation `manual_required` or `needs_attention`, expose provider diagnostics/manual instructions, and continue safe observation. Do not mark the job `cancelled` merely because the local CLI process was killed.

**Never implement cancel by deleting a notebook or dataset.** Resource deletion is a separate operation, with different semantics and a risk of destroying evidence/results.

### 14.8 Runtime shutdown

Graceful shutdown stops accepting new jobs, stops new dispatch, commits state, and exits within a local bound. By default, it does not attempt to cancel already submitted remote work.

Document clearly that stopping the runtime does not necessarily stop remote compute. On restart, resume reconciliation. An optional future `shutdown --cancel-active` must use the same honest capability-aware cancellation path, not unconditional success messages.

## 15. HTTP API and application integration

### 15.1 Transport and versioning

Use REST-like HTTP endpoints with JSON metadata and streamed binary transfer. Default bind address is `127.0.0.1`; default port is `7331`, configurable. No special significance is assigned to this working port.

Use bearer tokens for protected routes, request IDs for diagnostics, and a versioned error envelope. Timestamps are UTC RFC 3339 strings. IDs are opaque strings. Durations and byte counts must have explicit units.

The API is asynchronous for remote work. A `202 Accepted` response means a local operation was durably accepted; it does not mean the provider allocated a GPU or completed the operation.

### 15.2 Suggested endpoint inventory

All workspace-scoped endpoints enforce token authorization for that workspace.

| Method and route | Purpose |
|---|---|
| `GET /healthz` | Minimal liveness; no provider call, secrets, or account details. |
| `GET /readyz` | Local readiness: state store and required runtime services, not proof of GPU availability. |
| `GET /v1/info` | Runtime/API versions and public feature information. |
| `GET /v1/workspaces/{w}/profiles` | Allowed generic compute profiles and sanitized capability summaries. |
| `GET /v1/workspaces/{w}/profiles/{p}/quota` | Latest permitted quota summary with age/status. |
| `POST /v1/workspaces/{w}/objects` | Stream a binary file/archive into immutable object storage. |
| `POST /v1/workspaces/{w}/objects/import` | Snapshot a relative path under an allowed import root. |
| `GET /v1/workspaces/{w}/objects/{o}` | Retrieve object metadata, not arbitrary host paths. |
| `POST /v1/workspaces/{w}/jobs/validate` | Validate a specification and return requirements/warnings; never allocate compute. |
| `POST /v1/workspaces/{w}/jobs` | Durably accept an immutable job with idempotency. |
| `GET /v1/workspaces/{w}/jobs` | Cursor-paginated job listing with bounded filters. |
| `GET /v1/workspaces/{w}/jobs/{j}` | Job status, active attempt, results, conditions, and action links. |
| `GET /v1/workspaces/{w}/jobs/{j}/attempts` | Attempt history. |
| `GET /v1/workspaces/{w}/jobs/{j}/events` | Durable cursor-based event history. |
| `GET /v1/workspaces/{w}/jobs/{j}/logs` | Available logs with attempt/source/availability metadata. |
| `GET /v1/workspaces/{w}/jobs/{j}/artifacts` | Verified artifacts, scoped to an explicit/default active attempt. |
| `GET /v1/workspaces/{w}/jobs/{j}/artifacts/{a}/content` | Stream one authorized artifact. |
| `POST /v1/workspaces/{w}/jobs/{j}/cancel` | Persist and process cancellation intent. |
| `POST /v1/workspaces/{w}/jobs/{j}/retry` | Explicitly create a new compute attempt when safe/appropriate. |
| `POST /v1/workspaces/{w}/jobs/{j}/reconcile` | Refresh/reconcile existing remote identity without new execution. |
| `POST /v1/workspaces/{w}/jobs/{j}/collect` | Retry collection of existing results, without rerunning compute. |
| `GET /v1/workspaces/{w}/operations/{op}` | Observe an asynchronous control operation. |

Administrative workspace/token/provider configuration should primarily use local CLI/config files in MVP. A complete remote administration API is unnecessary.

### 15.3 Object upload

An object upload is a binary request, with optional declared length/digest and bounded metadata. The server streams to a temporary file, calculates its digest, validates limits, atomically publishes the file, and commits ownership/metadata before returning a complete object ID.

Incomplete uploads do not become usable objects. Identical content may be deduplicated within a workspace, but global cross-workspace deduplication is not an MVP requirement.

Large uploads may be retried from the beginning in MVP. Resumable multipart upload is a future improvement; do not claim it works until implemented.

### 15.4 Admission response

```json
{
  "job_id": "job_example",
  "attempt_id": "att_example_1",
  "status": "queued",
  "created_at": "2026-09-13T11:00:00Z",
  "links": {
    "self": "/v1/workspaces/example/jobs/job_example",
    "events": "/v1/workspaces/example/jobs/job_example/events",
    "artifacts": "/v1/workspaces/example/jobs/job_example/artifacts"
  }
}
```

The response should include a `Location` header. A repeated idempotent admission returns the original identifiers and indicates that it is a replay, not a new attempt.

### 15.5 Status response

A status response should contain immutable request/resolution summaries, orchestration status, execution observation, current phase, known progress, result status, cancellation status, conditions, timestamps, and the active attempt ID.

`GET` must not create compute or perform an implicit retry. It may return cached provider observations with explicit age; the background reconciliation loop, while the runtime is running, refreshes them.

### 15.6 Operation responses

Cancel/retry/reconcile/collect use durable operation records. Suggested response:

```json
{
  "operation_id": "op_example",
  "kind": "cancel",
  "status": "accepted",
  "job_id": "job_example",
  "attempt_id": "att_example_1",
  "remote_termination_confirmed": false
}
```

For cancellation with no verified remote-cancel capability, the resulting operation becomes `manual_required`, not `succeeded`. The intent remains visible and dispatch is prevented wherever still locally controllable.

Compute retry must reject unresolved submission/active execution with `409 REMOTE_EXECUTION_UNRESOLVED`. It must also direct users to collection recovery when the computation already succeeded and only artifact retrieval failed. Rerunning already successful work can be expressed as a new job rather than misleadingly calling it recovery.

### 15.7 Pagination and event polling

Use opaque cursors and fixed maximum page sizes. Job lists should have a stable order with an ID tie-breaker. Event cursors should use monotonically increasing per-job sequence numbers or equivalent durable ordering.

Ordinary polling is sufficient for MVP. An SSE endpoint may be added later for runtime events, but it must not be advertised as token streaming or proof of live provider logs. WebSockets are not required.

### 15.8 HTTP error mapping

| Status | Typical use |
|---|---|
| `400` | Invalid JSON, schema violation, malformed paths/parameters. |
| `401` | Missing or invalid API authentication. |
| `403` | Workspace/profile/operation not allowed. |
| `404` | Resource not visible to the caller or not found. |
| `409` | Idempotency conflict, illegal transition, unresolved active execution, retry not appropriate. |
| `413` | Request/upload exceeds configured size limits. |
| `422` | Syntactically valid job cannot meet required capabilities or execution constraints. |
| `429` | Local admission/queue/request limits; include appropriate retry information. |
| `503` | Runtime cannot safely admit work due to local unavailable infrastructure. |

An upstream execution failure after admission is represented in the job/attempt, not retroactively as the original POST returning an HTTP error.

### 15.9 Minimal language-neutral workflow

```text
1. Configure RUNTIME_URL, WORKSPACE_ID, and WORKSPACE_TOKEN.
2. Upload the code bundle and local inputs, or request permitted input ingestion.
3. Submit one JSON job with a stable Idempotency-Key.
4. Persist the returned job_id in the application's own business record.
5. Poll until results are available, a terminal failure occurs, or operator action is required.
6. Download declared artifacts by artifact ID.
7. Verify digest/size where relevant; update application business state.
```

The application can exit and reconnect later using the job ID. It does not have to keep the original HTTP request open for the remote execution duration.

## 16. Artifacts, logs, progress, and event delivery

### 16.1 Artifact contract

An artifact record includes artifact ID, workspace/job/attempt IDs, normalized relative path, media type if known, byte length, digest, creation/collection time, verification status, and retention/expiry information.

Do not expose provider URLs as the only artifact retrieval mechanism. The runtime should serve verified local bytes through its authenticated API. Provider references belong to diagnostics and collection internals.

### 16.2 Collection procedure

Wait until the provider exposes a stable collectible result, enumerate all relevant pages, download to temporary storage, verify identity and digests, and only then publish artifacts atomically.

Always collect the runner result manifest and bounded logs when available, including failure cases. Prefer declared outputs to indiscriminate download of the entire remote filesystem.

A required output missing after confirmed completion prevents job success. Optional outputs can be absent. A provider success flag alone is not enough to prove that the business command succeeded and all outputs arrived.

### 16.3 Integrity and safety

Reject traversal paths, absolute paths, duplicate/conflicting filenames, symlinks, unexpected device entries, and outputs escaping the declared root. Apply file-count and total-byte limits before/during transfer.

Validate attempt identity before associating outputs with the job. Do not trust MIME types or extensions for safe browser rendering; serve artifacts as downloads by default and avoid content sniffing.

### 16.4 Logs

Keep three sources distinct:

- `runtime`: Local preparation, scheduling, provider calls, and recovery events.
- `provider`: Logs actually obtained through the provider's supported interface.
- `payload`: stdout/stderr captured by the remote runner and collected when available.

Expose availability states such as `live`, `delayed`, `after_completion`, `unavailable`, and `unknown`. Returning runtime logs while calling them live payload logs is misleading.

Bound log size, line size, and retrieval page size. Record explicit truncation markers. Redact known secrets at capture/export boundaries, while documenting that user code can still print data the system cannot recognize as sensitive.

### 16.5 Progress

Core progress should use phases: preparing, submitting, remote queued, running, collecting, complete. Do not invent a percentage based only on elapsed time.

Optionally allow a documented JSON-lines progress file or structured stdout convention from cooperating jobs. Progress should include source, timestamp, optional completed/total units, and message. Treat it as untrusted informational data, not control-plane instructions.

A remote progress file available only after completion does not enable real-time progress. The API must distinguish historical progress from current observations.

### 16.6 Durable events

Store important state transitions and operational outcomes in the local event log. Include event ID/sequence, timestamp, job/attempt IDs, type, and a bounded structured payload.

Suggested events include `job.accepted`, `inputs.ready`, `provider.staging.ready`, `submission.intent_recorded`, `submission.accepted`, `submission.unknown`, `execution.observed`, `cancellation.requested`, `collection.failed`, `artifact.verified`, and `job.completed`.

No mandatory external message broker or webhook receiver is needed. Outbound webhooks can wait until retry/delivery/SSRF semantics are deliberately designed.

## 17. Error model and recovery semantics

### 17.1 Structured errors

Use stable error codes, a clear message, failure stage, provider context where safe, whether a control operation is safely retryable, whether compute may already have started, and a recommended next action.

```json
{
  "error": {
    "code": "PROVIDER_SUBMISSION_UNKNOWN",
    "message": "The submission response was lost; the remote execution may exist.",
    "stage": "submission",
    "request_id": "req_example",
    "job_id": "job_example",
    "attempt_id": "att_example_1",
    "safe_operation_retry": false,
    "compute_may_have_started": true,
    "recommended_action": "reconcile",
    "details": {"provider_instance": "personal-kaggle"}
  }
}
```

Do not use a bare `retryable=true` flag that clients may interpret as permission to rerun expensive compute. Separate retrying an observation/transfer from creating another attempt.

### 17.2 Error taxonomy

At minimum define categories/codes for:

| Category | Examples |
|---|---|
| Validation | `INVALID_JOB_SPEC`, `UNSUPPORTED_CAPABILITY`, `INVALID_INPUT_PATH`, `RESOURCE_REQUIREMENT_UNSATISFIED`. |
| Authentication/authorization | `RUNTIME_AUTH_REQUIRED`, `WORKSPACE_FORBIDDEN`, `PROVIDER_AUTH_FAILED`, `PROVIDER_ACCESS_DENIED`. |
| Input preparation | `INPUT_NOT_FOUND`, `INPUT_FETCH_FAILED`, `INPUT_CHANGED`, `INPUT_DIGEST_MISMATCH`, `INPUT_TOO_LARGE`. |
| Provider preparation | `STAGING_FAILED`, `STAGING_NOT_READY`, `PRIVATE_STAGING_UNAVAILABLE`, `PROVIDER_STORAGE_LIMIT`. |
| Submission | `PROVIDER_REJECTED`, `PROVIDER_SUBMISSION_UNKNOWN`, `PROVIDER_RATE_LIMITED`, `QUOTA_EXHAUSTED`. |
| Execution | `DEPENDENCY_SETUP_FAILED`, `COMMAND_FAILED`, `REMOTE_TIMEOUT`, `RESOURCE_EXHAUSTED`, `PROVIDER_EXECUTION_LOST`. |
| Observation | `PROVIDER_UNREACHABLE`, `PROVIDER_STATE_UNKNOWN`, `REMOTE_IDENTITY_MISMATCH`. |
| Results | `RESULT_MANIFEST_MISSING`, `ARTIFACT_MISSING`, `ARTIFACT_DIGEST_MISMATCH`, `ARTIFACT_COLLECTION_FAILED`. |
| Operations | `REMOTE_CANCEL_UNSUPPORTED`, `REMOTE_EXECUTION_UNRESOLVED`, `ILLEGAL_STATE_TRANSITION`, `IDEMPOTENCY_CONFLICT`. |
| Local runtime | `STATE_STORE_UNAVAILABLE`, `DISK_LIMIT_EXCEEDED`, `STATE_DIRECTORY_LOCKED`, `CONFIGURATION_INVALID`. |

Avoid mapping every upstream failure to authentication or quota exhaustion. Preserve a sanitized raw provider code/message for troubleshooting when its meaning is not confidently known.

### 17.3 Retry policy matrix

| Operation | Automatic behavior |
|---|---|
| Read-only status/quota/log polling | Bounded backoff and jitter; no new compute. |
| Input download before snapshot freeze | Bounded retry of the same source under policy; record final immutable bytes. |
| Artifact download | Bounded retry/resume when supported, without compute rerun. |
| Resource creation | Reconcile deterministic identity after uncertainty before considering another call. |
| Compute submission | Never automatically repeat after ambiguous acceptance. |
| Payload failure | No automatic retry. |
| Provider switch | Never automatic. |
| Cleanup of ledger-owned resources | Bounded idempotent retry; missing resource can count as already removed. |

Record automatic control-plane retries in diagnostics/events at a suitable level. “No silent retries” refers especially to workload re-execution; safe network recovery should be observable without flooding the UI.

### 17.4 Explicit operator actions

Provide distinct actions for reconcile, retry collection, retry compute, cancel, inspect provider instructions, and cleanup. Do not call all of these “retry.”

When automatic progress stops, show what is known, what is unknown, whether quota may still be consumed, and the next safe action. A useful error must not require reading the runtime source code to understand its effect.

## 18. Configuration, credentials, and workspaces

### 18.1 Configuration model

Use a human-readable TOML runtime configuration and JSON job specifications. Keep global policy, provider-instance configuration, compute profiles, workspace access, and credential references separate.

Suggested configuration, with illustrative values only:

```toml
schema_version = 1

[server]
listen = "127.0.0.1:7331"
auth_required = true

[storage]
state_dir = "./.compute-connector/state"
blob_dir = "./.compute-connector/blobs"
max_local_bytes = 21474836480
min_free_bytes = 2147483648

[scheduler]
max_active_per_account = 1
max_queued_per_workspace = 100
max_queued_total = 1000
status_poll_seconds = 15

[execution]
default_remote_wall_seconds = 1800
setup_seconds = 300
finalization_grace_seconds = 60
automatic_compute_retries = 0

[policy]
free_only = true
allow_remote_internet = true
automatic_provider_fallback = false
unknown_quota = "warn_and_allow_bounded"

[providers.personal-kaggle]
type = "kaggle"
enabled = true
credential_ref = "env:KAGGLE_API_TOKEN"
account_name = "SET_YOUR_KAGGLE_USERNAME"
cost_class = "free_allowance"
transport = "official-client"
cli_executable = "kaggle"
private_resources = true
staging_license = "copyright-authors"
remote_auto_cleanup = false

[profiles.default-gpu]
provider = "personal-kaggle"
accelerator = "gpu"
minimum_gpu_count = 1
remote_internet = "disabled"

[workspaces.example]
allowed_profiles = ["default-gpu"]

[retention]
job_metadata_days = 30
artifacts_days = 7
completed_input_days = 7
unreferenced_object_hours = 24
```

This is a proposed configuration contract. The agent must finalize a consistent schema, document precedence, and test examples. Do not assume the literal values above identify a real account or working credential.

### 18.2 Precedence and validation

Default precedence is CLI override, documented environment override, configuration file, then built-in default. Resolve relative paths against the configuration file directory, not whichever working directory happens to launch the runtime.

Reject unknown keys and contradictory settings. For example, a profile requiring internet cannot silently override an operator policy forbidding it. Effective job resolution should show which defaults were applied.

`profiles.*.remote_internet` is a default, while `policy.allow_remote_internet` is an upper permission bound. With the example configuration, jobs default to no remote internet but may explicitly request it. Setting the global bound to false prohibits that request. Implement an equivalent per-profile permission bound when profiles need stricter policy. A provider unable to enforce requested network disabling must report that limitation rather than treating the flag as a security guarantee.

Changing a profile affects future accepted jobs only. Active attempts keep their resolved non-secret configuration snapshot and credential reference. Credential rotation may update the value behind that reference, but must not silently change the provider account binding.

Do not allow deleting/disabling the only adapter configuration needed to reconcile active attempts without retaining a recovery path. New dispatch can be disabled while observation/collection remains possible.

### 18.3 Credential handling

Provider credentials belong to the operator's runtime environment or protected credential files. Store references such as `env:NAME` or `file:/protected/path`, not plaintext secrets in job specs or SQLite.

For Kaggle, official authentication supports multiple mechanisms; the adapter should document the selected supported modes and precedence instead of scanning every host credential source indiscriminately. [S-02]

Do not pass provider credentials to the remote runner. Do not include them in a bundle, dataset, notebook source, process command-line argument, downloadable diagnostic archive, or logs.

A Python bridge may receive credentials through a tightly controlled environment or protected file as required by the official client. Sanitize subprocess environment inheritance and cleanup temporary material.

### 18.4 Application tokens

Generate high-entropy local API tokens. Store only token digests plus identifiers/scopes in the state database. Support revocation and workspace binding. Show newly generated secrets once through an explicit command, never in ordinary runtime logs.

Use one administrative token or local administrative CLI authority for setup and separate workspace tokens for apps. A workspace token must not list another workspace's jobs or fetch its blobs by guessing IDs.

### 18.5 Job secrets

General remote job-secret injection is outside MVP unless a provider-supported, tested mechanism is implemented. The small LLM example must avoid gated/private models needing secret delivery.

Plain `execution.environment` is explicitly non-secret. If a caller requests a secret reference and the provider cannot deliver it securely, reject the job instead of embedding the secret in generated source.

### 18.6 Workspaces are not hostile multi-tenancy

Workspaces provide ownership, filtering, scoped API authorization, and operational separation for the same operator's apps. They do not promise isolation from another malicious job running with the same remote account/environment or from the machine's administrator.

No organization management, payments, user signup, or public tenant onboarding is needed.

## 19. Security, privacy, and abuse boundaries

### 19.1 Threat model

MVP assumes one trusted operator and primarily operator-trusted workloads. Nonetheless, uploaded archives, filenames, URLs, provider output, and HTTP requests must be treated as untrusted input to the local runtime.

Protect against accidental credential disclosure, arbitrary host-file access, path traversal, cross-workspace access, remote-content confusion, local command injection, resource exhaustion, and unintended public exposure.

Do not claim that the runtime can safely execute arbitrary hostile code on shared Kaggle infrastructure or isolate mutually untrusted tenants. Do not add a local execution provider as an invisible fallback.

### 19.2 Local API exposure

Bind to loopback by default, require authentication, disable CORS by default, and validate expected Host/Origin behavior where relevant to local-browser threats. Do not expose an unauthenticated API on `0.0.0.0` for convenience.

Non-loopback deployment must be an explicit operator choice with TLS and authentication guidance. It may use an operator-managed reverse proxy, but the reference local workflow must not depend on one or on a commercial tunnel.

Use request/body/time limits and rate limits on expensive admission/import routes. Administrative token/provider operations should not be exposed unnecessarily over the application API.

### 19.3 Archive and path safety

Before extraction, validate archive members and apply limits to both compressed and expanded size. Reject:

- Absolute paths, `..` traversal, Windows drive/UNC paths, and invalid separators.
- Symlinks, hard links, device nodes, FIFOs, and unsupported archive entry types.
- Duplicate normalized paths, case-collision paths where the platform is case-insensitive, and reserved/control paths.
- Excessive file counts, extremely long paths, and decompression bombs.

Use containment checks during extraction, not just string-prefix checks before it. Never extract provider archives directly over the runtime's state directory or application repository.

### 19.4 Local command execution

Provider client invocations must use argument arrays, controlled executable paths, a minimal environment, bounded stdout/stderr capture, deadlines, and process-tree cleanup on supported host platforms.

A job's shell command is data sent to the remote runner, not a command to execute on the runtime host. Validation must never “test” uploaded commands locally.

The fake provider should use fixtures by default; a test fixture that executes local code must be explicit, isolated, and not enabled in production.

### 19.5 URL ingestion and SSRF

Restrict MVP URL ingestion to permitted HTTPS destinations. Validate DNS resolutions and connection endpoints, reject loopback/private/link-local/metadata destinations, revalidate redirects, and prevent redirect-based protocol changes.

Do not forward incoming runtime authorization headers to downloaded URLs. Avoid ambient proxy credentials or unreviewed redirects leaking secrets. Apply total size/time budgets even when content length is absent or misleading.

For a legitimately private/internal data source, the operator can use upload or allowlisted local import in MVP rather than weakening SSRF protections globally.

### 19.6 Secrets and metadata

Redact configured credentials from logs and support bundles. Exclude environment variables by default from diagnostic export. Keep provider token files outside generated job/staging directories.

Remote control manifests may contain IDs, digests, non-secret environment settings, and timing data, but must not contain runtime bearer tokens, account API tokens, or arbitrary inherited host environment.

A filename, job name, or URL can itself contain sensitive information. Use opaque provider resource names by default rather than human job titles containing private data.

### 19.7 Privacy of remote resources

Make private visibility explicit in generated metadata and verify it through supported observations when possible. Never switch to public visibility to resolve an authorization error.

“Private” is access control provided by the compute service, not end-to-end encryption or a guarantee that the service cannot access uploaded data. Documentation must advise against uploading sensitive or regulated material without checking the provider's suitability and the operator's obligations.

### 19.8 Filesystem permissions

Use restrictive permissions for state directories, token files, credential references, and temporary provider data. Implement platform-appropriate handling on Windows instead of assuming Unix mode bits suffice everywhere.

The local OS administrator can inspect the runtime process/files. Do not claim the workspace token model protects against the machine owner. Encryption at rest beyond the operator's filesystem is not an MVP requirement; keep the boundary explicit.

### 19.9 Supply chain

Pin runtime dependencies and the adapter's client environment. Keep lockfiles/checksums in the repository, use trusted upstream sources, and verify released executable checksums in installation documentation.

Remote dependency installation executes package code under the workload's authority. It is not inherently safe merely because it occurs on a provider. Examples should use small, reviewed, reproducible dependency sets and avoid executing remote model code unless explicitly needed and reviewed.

Do not automatically run `curl | sh` from arbitrary job input on the local host.

### 19.10 Policy boundaries

Do not implement quota rotation across accounts, automated account creation, CAPTCHA bypass, activity simulation, remote anti-idle loops, or submission bursts intended to defeat platform restrictions.

Do not use a future session mode as a vehicle for circumventing maximum runtimes. A retained session must remain bounded, explicitly selected, and permitted by its provider.

## 20. Local storage, retention, and cleanup

### 20.1 Suggested storage layout

Use OS-appropriate application directories by default, with explicit overrides for portable deployments. The relative layout may be:

```text
compute-connector/
├── config.toml                    non-secret configuration
├── state/
│   ├── runtime.db
│   ├── runtime.lock
│   └── migrations/
├── blobs/
│   └── <workspace-id>/
│       ├── objects/
│       ├── artifacts/
│       └── temporary/
├── logs/
├── diagnostics/
└── provider-tools/
    └── kaggle/                    optional managed/pinned client environment
```

The agent may keep migration assets embedded in the binary instead of a filesystem directory. Do not put user credentials in this layout by default unless the operator explicitly supplies a protected credential file there.

Store paths in a portable form where possible; do not expose physical paths as public API identifiers.

### 20.2 Atomic file lifecycle

Write uploads/downloads/manifests to temporary files, verify them, then rename into final locations. Coordinate database state with file publication using a recoverable staged lifecycle, since a filesystem rename and a database transaction are not one cross-system transaction.

A startup sweep may quarantine/remove orphaned temporary files. It must not delete active attempt files based solely on modification time.

### 20.3 Default limits

Proposed local limits, adjustable by configuration and not representations of provider capacity:

| Item | Default |
|---|---|
| JSON request body | 1 MiB |
| Compressed code bundle | 100 MiB |
| Expanded code bundle | 500 MiB |
| Files in one bundle | 10,000 |
| One input object | 2 GiB |
| Total declared input bytes per job | 4 GiB |
| Total artifacts collected per attempt | 4 GiB |
| Files collected per attempt | 10,000 |
| Captured stdout / stderr | 20 MiB each, with explicit truncation markers |
| Runtime-managed local storage | 20 GiB |
| Minimum free disk reserve | 2 GiB |

Provider/profile limits can be stricter. Enforce the effective bound and report its source. Do not increase limits automatically to complete a job.

### 20.4 Retention policy

Default metadata retention is 30 days; completed input/artifact retention is 7 days; unreferenced upload retention is 24 hours. Active, ambiguous, or recovery-required jobs pin the data necessary for safe observation/collection.

Metadata may outlive artifact/input bytes. Expose expiry and return an explicit expired/not-retained result rather than pretending the artifact never existed.

An explicit retry is available only while the immutable inputs remain available. Missing retained inputs require a new job or explicitly re-created original bytes with verified identity; do not silently substitute a current URL response.

### 20.5 Remote resource ledger

Track every connector-created provider resource with owner account, instance, resource ID, purpose, job/attempt association, creation operation, references, and cleanup state.

Do not discover cleanup candidates merely by a shared name prefix and delete them. A prefix can help discovery, but deletion requires ledger ownership and identity checks.

Do not delete resources whose execution status is unknown or whose outputs have not been safely collected under the applicable policy.

### 20.6 Cleanup defaults

Remote automatic cleanup is disabled initially. Provide `cleanup --dry-run` as the default behavior and an explicit `--apply` mode for known completed resources. A future configurable automatic retention mode may call the same tested ownership-safe cleanup service.

Local unreferenced temporary/object cleanup may run automatically under retention policy. Remote cleanup failures must be visible because orphaned staging objects can affect storage capacity even when no GPU is running.

Deleting local metadata must not orphan an active remote resource silently. Defer removal or retain a minimal recovery/ownership record until remote state is resolved.

### 20.7 Backups and upgrades

Provide documented SQLite-consistent backup/export rather than copying a live database naively. Migrations should be ordered, tested, and forward-safe, with a backup/recovery procedure before destructive changes.

Do not promise downgrade compatibility across arbitrary schema versions. Reject unsupported schema versions clearly and document the restore path.

## 21. CLI, installation, platform support, and developer experience

### 21.1 Working executable name

Use `compute-connector` as the provisional executable/repository name. Branding and package availability are not reasons to block implementation. Keep naming centralized and avoid baking the working name into provider identity formats more than necessary.

### 21.2 Proposed operational commands

These commands are target UX, not existing commands:

```text
compute-connector init
compute-connector serve --config config.toml
compute-connector doctor
compute-connector workspace create example
compute-connector token create --workspace example
compute-connector provider check personal-kaggle
compute-connector bundle create ./job --output job.tar.gz
compute-connector bundle inspect job.tar.gz
compute-connector job validate --file job.json
compute-connector job submit --file job.json --idempotency-key EXPLICIT_KEY
compute-connector job status JOB_ID
compute-connector job logs JOB_ID
compute-connector job artifacts JOB_ID
compute-connector job cancel JOB_ID
compute-connector job reconcile JOB_ID
compute-connector job collect JOB_ID
compute-connector job retry JOB_ID --reason "explicit operator retry"
compute-connector cleanup --dry-run
compute-connector cleanup --apply
```

The CLI should use the same application service/API semantics rather than implementing a separate scheduler. Online job commands should call the running runtime; offline commands such as bundle inspection/config validation should not require provider access.

### 21.3 Initialization

`init` creates a non-secret example config, state directories with appropriate permissions, and instructions for workspace tokens/provider authentication. It must not submit test jobs, enable billing, create remote resources, or launch browser auth without an explicit command.

For unattended use, provide noninteractive flags and documented environment variables. An interactive setup wizard is optional, not the only installation route.

### 21.4 Doctor and preflight

Doctor should separate local checks, read-only authenticated checks, and compute-consuming verification.

Default `doctor` checks configuration, directory permissions, free disk, state lock, SQLite schema, executable availability, client versions, credential references, and static compatibility. Provider read probes may be an explicit mode; any command that allocates GPU must require an explicit test flag and display its configured budget.

Never consume GPU quota simply because a user asks whether the runtime is installed correctly.

### 21.5 Cross-platform default

Target these control-plane release builds:

| Platform | Initial target |
|---|---|
| Linux | amd64 and arm64 |
| macOS | amd64 and arm64 |
| Windows | amd64 |

The agent must validate the selected SQLite driver, subprocess behavior, path rules, token permissions, and official-client installation on each claimed platform. Do not claim support based solely on a successful cross-compile.

Windows arm64 and additional architectures can be deferred. The remote execution environment is separately described by each provider.

### 21.6 Installation methods

Primary installation is downloading/building the Go runtime plus installing the adapter's supported prerequisites. Provide a pinned optional Python environment bootstrap for the official Kaggle client. Do not silently mutate the user's global Python environment.

An optional container image may include the Go runtime and pinned provider client. Mount operator-owned configuration/state and inject credentials through supported secure means. Explain loopback/container port behavior and path translation.

Docker must remain optional. Do not mount the host Docker socket or require privileged containers for normal orchestration.

### 21.7 Developer commands

Provide discoverable commands for format, lint, test, build, run, contract-test, and opt-in provider integration tests. A Makefile may be a convenience, but Windows contributors must not need GNU Make merely to run the project.

Provide PowerShell and CMD-compatible entry points or a cross-platform Go task command with thin wrappers. POSIX shell scripts alone are insufficient for the claimed local platform support.

Pin formatter/linter versions and document installation. CI should run the same commands used locally. Do not add a large build framework only to wrap three commands.

### 21.8 First-run experience

The documented reference path should let an operator:

```text
Install runtime and official-client prerequisites
    -> configure one Kaggle instance
    -> create one workspace/token
    -> start runtime
    -> run read-only doctor
    -> explicitly run bounded GPU smoke test
    -> submit the small LLM batch example
    -> retrieve responses and environment/result manifests
```

The app integration must not require manually building or editing a notebook for each job.

## 22. Example workloads and application clients

### 22.1 Keep examples separate from core

Put example workloads under `examples/jobs/` and application integrations under `examples/clients/`. Core packages should not import a video library, tokenizer, inference framework, or a specific model client.

An example can demonstrate the execution contract without becoming a product feature that the core must understand.

### 22.2 GPU smoke test

The first real probe should perform a small actual GPU computation and emit:

```text
hardware.json
result.json
execution-result.json
stdout.log
stderr.log
```

It should assert a GPU is present and used, record device/runtime details, compute a small deterministic or numerically checked result, and exit quickly within a finite configured limit.

Override all phase budgets for this tiny test, for example a 120-second remote wall budget, 30-second setup ceiling, and 15-second finalization reserve. Do not inherit the normal 300-second setup default into a shorter total wall budget.

A device listing alone is not sufficient evidence that the payload used GPU compute. No local GPU or paid inference endpoint should be involved.

### 22.3 Small LLM batch example

The first meaningful workload reads a small `prompts.jsonl` input, loads a small openly downloadable text-generation model, performs bounded batch inference on the GPU, and writes `responses.jsonl` plus `metrics.json`.

Select the exact model autonomously during implementation using these defaults:

- Prefer an openly accessible model roughly in the sub-billion to low-billion parameter range that fits the verified free GPU environment with margin.
- No mandatory paid API, gated approval flow, private token, or user-supplied model-server endpoint.
- Verify the model's actual license and download requirements from its primary model card.
- Pin a specific revision and dependency set in the example.
- Use a tiny prompt set, small output-token limit, batch size one initially, and a finite remote wall budget.
- Avoid remote custom model-code execution unless reviewed and unavoidable; choose another simple model first.

The model choice is a technical example decision, not a question for the owner. The agent must not freeze an unverified model into the proposal merely because it remembers a model name.

Metrics should include setup/model-load/inference timing when measurable, actual device information, input/output counts, and resolved model revision. Token throughput is an observation from that run, not a universal provider benchmark.

Provide a network-enabled first example when downloads are necessary and document that setup/download time may occur within the allocated session. A fully staged/offline example is a useful follow-up, not a reason to hide network assumptions.

### 22.4 Video example after the core path

A later video example can process a short synthetic or redistributable clip through the same bundle/input/output contract. Its code and dependencies belong in the example, not in a `video` subsystem of the runtime.

Verify the actual requested acceleration path. GPU availability does not prove that a particular codec, hardware encoder, rendering backend, or framework build is usable. Fail or document CPU-only behavior explicitly; do not market a CPU-only render as a successful GPU demonstration.

Use a small asset and strict output/time limits. Large production footage, distributed rendering, editing timelines, and workflow orchestration remain application concerns.

### 22.5 Node.js, Python, and Go clients

Each reference client must show upload, job submission with idempotency, status polling, handling of `needs_attention`/unknown state, artifact download, and error reporting.

Use standard HTTP facilities or very small dependencies. Keep examples complete enough to run after setting the three integration values: base URL, workspace ID, and workspace token.

Do not ship three full SDKs before proving the common API. Application clients must never call Kaggle directly or contain provider credentials.

### 22.6 Example acceptance

For the same example job, Node.js, Python, and Go clients must be able to observe the same JSON state/artifact model. This is a language-neutral integration test, not a requirement that all three clients submit GPU jobs on every CI run.

## 23. Testing, verification, and acceptance criteria

### 23.1 Test tiers

| Tier | Environment | Purpose |
|---|---|---|
| Unit | Offline/local CPU | State transitions, validation, errors, policy, path handling, identity, and hashing. |
| Component | Local temporary SQLite/filesystem | Durable admission, object lifecycle, queue fairness, API authorization, migrations. |
| Provider contract | Deterministic fake provider | Common behavior independent of Kaggle. |
| Adapter fixture | Official-client/bridge output fixtures | Parsing, raw-state mapping, retries, pagination, client-version assumptions. |
| Fault injection | Fake/controlled transport | Ambiguous submission, process crash, network failures, disk failures, stale observations. |
| Live read-only | Operator credentials, explicit opt-in | Auth/capability/quota probes without allocating compute. |
| Live compute | Operator credentials, explicit opt-in and finite budget | GPU smoke test, LLM batch, termination, artifacts, and recovery evidence. |

No unit/default CI test should consume Kaggle GPU allowance or require secrets. Live tests must be separately named and explicitly enabled.

### 23.2 Required fault scenarios

Test at least:

1. Client loses the admission response and repeats its idempotency key.
2. Same key is reused with a different specification.
3. Runtime crashes immediately before/after writing submission intent.
4. Provider accepts a submission but the response is lost.
5. An ambiguous resource cannot be rediscovered conclusively.
6. Provider state is unknown or changes format.
7. A stale poll arrives after a terminal observation.
8. Runtime restarts while a remote attempt is active.
9. Output collection fails halfway through a large file.
10. A required output is missing or has the wrong digest.
11. Output pagination contains more than one page.
12. A cancellation request races with completion.
13. Cancellation is unsupported or lacks a usable session identity.
14. Local deadline expires while remote activity is unknown.
15. Quota is missing, stale, exhausted, or consumed by external activity.
16. Input staging succeeds but readiness is delayed.
17. A URL redirects into a private network destination.
18. A bundle contains traversal, links, duplicates, or excessive expanded size.
19. A workspace references another workspace's object/artifact.
20. The state directory is opened by a second runtime process.
21. Disk runs out during upload, collection, or database commit.
22. Provider credentials rotate or the configured account changes.
23. A managed remote resource is manually modified outside the runtime.
24. Cleanup is retried after the remote resource is already absent.
25. A CLI exits nonzero after a possible remote side effect.

### 23.3 Critical invariants to assert

Tests must assert that no ambiguous submission creates an automatic second execution, no artifact from another attempt is accepted, no cancellation request is reported as confirmed without evidence, and no unsupported GPU requirement becomes a silent CPU fallback.

Assert workspace authorization on job reads, event reads, logs, objects, artifacts, operations, and profile access. A single secure top-level POST is not enough.

Assert that provider credentials are absent from generated bundles/notebook sources and sanitized diagnostic exports.

### 23.4 Live Kaggle acceptance checklist

The first Kaggle-capable release must have a sanitized evidence record showing:

- A real operator-authenticated, private, bounded job was submitted through the supported client path.
- Multi-file code and a separate input file arrived with verified digests.
- A small GPU operation and the selected LLM workload actually used the expected GPU execution path.
- The job exited and the provider reported a terminal execution state.
- Available hardware-release/accounting evidence was recorded; unobservable details remain labeled unobservable.
- All required outputs were downloaded and verified against the correct attempt.
- A runtime restart preserved the existing attempt rather than submitting another.
- Capability results for cancellation, live logs, and quota match the actual tested environment.
- No maintainer service, paid API, required billing setup, or mandatory public tunnel participated in the reference path.

### 23.5 What counts as “verified”

A passing fake-provider test proves local architecture behavior, not Kaggle support. A passing CLI parser test proves command construction, not account eligibility. An upstream feature description is evidence to investigate, not proof that a configured account can use it.

Use statuses such as `passed-live`, `passed-offline`, `documented-upstream`, `not-tested`, `unsupported`, and `blocked-environment` in the evidence report. Avoid a single green checkbox that obscures what was actually exercised.

### 23.6 Missing credentials in an agent environment

If no authorized Kaggle credential is available, the agent should implement and test all offline pieces, write an executable opt-in live-test procedure, and mark live provider validation as blocked by environment.

Do not ask the owner to paste secrets into chat or fabricate successful live results. Do not stall the entire roadmap while waiting for credentials. The project cannot be declared live-provider-ready until the required live evidence exists, but planning and substantial implementation can proceed.

### 23.7 Performance acceptance without invented promises

Measure local admission latency, object-transfer throughput, memory use during streaming transfer, scheduler overhead, startup recovery duration, and database behavior on a documented reference environment.

Do not promise a fixed Kaggle startup time, fixed tokens-per-second, a universally available GPU model, or a render-speed multiplier. Provider allocation and workload performance are observations, not runtime service guarantees.

Set practical local acceptance targets in the implementation plan after measuring a baseline. Correctness, bounded memory, and recovery safety take precedence over micro-optimizing a small control plane.

## 24. Observability and support diagnostics

### 24.1 Structured local logs

Use structured logs with timestamp, severity, request ID, workspace ID, job ID, attempt ID, operation ID, and provider instance where relevant. Keep event names stable and secrets redacted.

Log phase durations and operation outcomes. Avoid dumping full job inputs, provider responses, environment variables, prompts, or model output into normal informational logs.

### 24.2 Useful metrics

A minimal metrics snapshot or optional local endpoint should include queue depth, active/ambiguous attempts, stage durations, provider request counts/errors, reconciliation outcomes, bytes transferred, disk usage, and cleanup backlog.

Treat quota observations separately from local estimated usage. Do not label elapsed wall time as authoritative GPU billing.

A hosted telemetry service is not required. Telemetry must be off by default; preferably omit outbound telemetry entirely in MVP.

### 24.3 Diagnostic export

Provide a redacted support bundle containing runtime/client versions, configuration with secrets removed, recent relevant events, capability evidence, and a small selected log window. Exclude job inputs/artifact contents by default.

Require explicit operator action to export diagnostics. Never upload them automatically to the maintainer or a third-party service.

### 24.4 Operational honesty

A status screen or CLI should be able to say:

```text
Execution was submitted; current provider state is unknown.
Last successful observation: <timestamp>.
The remote job may still be using compute.
No automatic re-execution has been attempted.
Next safe action: reconcile or inspect the provider account.
```

This is preferable to a reassuring but incorrect “cancelled” or “failed” state.

## 25. Repository structure and engineering standards

### 25.1 Suggested layout

```text
compute-connector/
├── README.md
├── LICENSE
├── SECURITY.md
├── CONTRIBUTING.md
├── AGENTS.md
├── go.mod
├── go.sum
├── Makefile
├── config.example.toml
├── .env.example
├── .computeignore.example
├── cmd/
│   └── compute-connector/
├── internal/
│   ├── app/                 application services
│   ├── domain/              jobs, attempts, states, errors
│   ├── api/                 HTTP handlers/middleware
│   ├── scheduler/
│   ├── reconcile/
│   ├── provider/            neutral contracts and registry
│   │   ├── kaggle/
│   │   └── fake/
│   ├── credentials/
│   ├── packaging/
│   ├── transfer/
│   ├── artifacts/
│   ├── store/
│   │   └── sqlite/
│   ├── config/
│   └── observability/
├── runner/
│   └── python/              generic remote wrapper
├── tools/
│   └── kaggle-bridge/       only if required by adapter ADR
├── api/
│   ├── openapi.yaml
│   └── schemas/
├── migrations/
├── examples/
│   ├── jobs/
│   │   ├── gpu-smoke/
│   │   └── llm-batch/
│   └── clients/
│       ├── nodejs/
│       ├── python/
│       └── go/
├── tests/
│   ├── contract/
│   ├── fixtures/
│   ├── fault/
│   └── integration/
├── scripts/
│   ├── dev.sh
│   ├── dev.ps1
│   └── dev.cmd
├── deploy/
│   └── docker/
└── docs/
    ├── proposal.md
    ├── architecture.md
    ├── roadmap.md
    ├── implementation-plan.md
    ├── decisions/
    ├── research/kaggle-feasibility.md
    ├── providers/kaggle.md
    ├── job-contract.md
    ├── api.md
    ├── security.md
    ├── operations.md
    ├── compatibility.md
    ├── testing.md
    └── troubleshooting.md
```

This is a boundary illustration, not an instruction to create empty abstractions for every folder. Keep packages small enough to understand and consolidate where the same boundaries can be preserved with less code.

### 25.2 Dependency selection

Use a current supported Go version selected and pinned during implementation. Prefer standard-library facilities for HTTP, subprocesses, logging, and basic concurrency where they are adequate.

Choose a maintained SQLite driver with a viable cross-platform build story. Record the driver choice, CGO implications, migration library strategy, and why the selected dependencies are necessary. Do not assume a particular package version from memory.

The agent must verify current dependency documentation and licenses before adopting APIs. Avoid using this proposal as a reason to pin arbitrary unverified versions.

### 25.3 Testing and lint

Require formatting, static analysis, unit/component tests, and race checks where supported. Python runner/bridge code should have its own formatting, lint/type-check decisions, dependency lock, and unit tests.

Validate OpenAPI/JSON schemas and all committed sample job/config documents. Keep generated clients optional. No binary secrets, model weights, rendered videos, runtime databases, or real account data should be committed.

### 25.4 Continuous integration

Default CI is offline with respect to compute allocation. Use fake providers and fixtures. Live integration jobs must be manually gated, secret-scoped, and impossible to trigger with untrusted pull-request code.

Do not assume the owner's Git hosting service or create a remote repository from unrelated account history. Adapt CI to the actual target repository after inspection. Running/hosting CI remains the operator's choice, not a mandatory paid runtime dependency.

### 25.5 Documentation standards

Every public command/config example must be validated against the implementation before release. Identify what is supported, experimental, unknown, and out of scope. Include the last-tested client versions and provider evidence date.

Explain cold starts, finite sessions, staging delays, quota uncertainty, data-upload implications, and artifact retention near the quickstart, not buried only in legal text.

Do not claim “plug any code into Kaggle” without explaining the execution bundle, environment, dependency, and data-transfer constraints.

### 25.6 Agent-specific repository guidance

If an existing target repository contains `AGENTS.md`, `CLAUDE.md`, `.claude/CLAUDE.md`, contributor rules, or other applicable engineering instructions, read and follow them before changes. This is a new independent project unless the target repository explicitly says otherwise; do not import another project's business architecture or conventions from unrelated conversation history.

## 26. Milestone outcomes and planning instructions

### 26.1 The next agent owns the roadmap

The owner is not asking this proposal to prescribe every task or sprint. Derive a roadmap and implementation plan from the required outcomes below, with dependencies, acceptance tests, and decision gates. Do not estimate dates without a real execution context, and do not treat optional features as release blockers.

### 26.2 Recommended outcome sequence

| Outcome | What must exist before moving forward |
|---|---|
| M-0: Evidence and scope | Repository instructions inspected; approved requirements mapped; current official Kaggle surfaces reviewed; risk/feasibility report started; proposed transport/default decisions recorded. |
| M-1: Thin real-provider proof | Small private GPU execution through the supported path; input/output checksums; terminal evidence; no hidden paid dependency. Build only enough scaffolding to test this. |
| M-2: Portable core | Versioned job model/API, workspace authorization, immutable object ingestion, fake provider, and clear provider boundaries. |
| M-3: Durable orchestration | SQLite queue, attempts, idempotency, submission-intent ledger, recovery, and safe collection retries under fault tests. |
| M-4: Integrated Kaggle batch | Adapter implements the validated lifecycle inside the durable core; capability/identity/error mapping matches evidence. |
| M-5: Usable developer product | CLI, doctor, configuration, direct-install path, cross-platform commands, small LLM example, and three thin language clients. |
| M-6: Release hardening | Security/retention/cleanup tests, compatibility documentation, operator warnings, acceptance evidence, and reproducible release artifacts. |

These are milestone outcomes, not a mandatory linear waterfall. Some core scaffolding and tests can run alongside provider research. The important ordering is to de-risk the real compute path before building extensive abstractions or a polished interface around an unverified assumption.

### 26.3 Stop/go rules

Proceed with the batch adapter when a supported private input -> bounded execution -> identified result path is demonstrated.

If cancellation or live logs remain unproven, ship batch with explicit capability limitations rather than faking the feature or adding unsupported browser automation.

If basic private execution, identity-safe collection, or acceptable bounded termination cannot be achieved, record the precise blocking evidence and continue provider-neutral/offline work. Do not silently redesign the product as a paid service or hosted tunnel architecture.

### 26.4 Required planning artifacts

The next agent should produce:

```text
Scope/requirements matrix
Architecture and dependency diagram
Kaggle feasibility report and capability matrix
Architecture decision records for major defaults
Roadmap with acceptance-based milestones
Implementation plan with task dependencies and tests
Risk register with predetermined fallback behavior
Compatibility and live-verification checklist
```

Each task should state the intended behavior, main components affected, acceptance test, dependencies, and whether it consumes external compute/credentials. Separate research tasks from proven implementation tasks.

### 26.5 Avoid overbuilding

The first implementation should not require a dynamic plugin loader, workflow graph engine, public dashboard, SDK generator, distributed queue, automatic model cache service, or a second production provider.

A small real vertical slice plus strong failure semantics is more valuable than a large skeleton whose `Submit`, `Cancel`, and `ReadQuota` methods return optimistic placeholders.

## 27. Risks and predetermined responses

| Risk | Response already authorized by this proposal |
|---|---|
| Quota differs from the owner's 30-hour expectation | Read account-specific data when possible; show unknown/stale states; never hard-code entitlement. |
| GPU unavailable or account not eligible | Block/report clearly; no CPU or paid-provider fallback. |
| Provider startup is too slow for interactive use | Keep batch positioning; retained sessions remain separate future research. |
| Official client changes | Pin supported versions, test fixtures, record evidence, and upgrade deliberately. |
| CLI output is not sufficiently structured | Use a small official-client Python bridge, not guessed CLI flags or undocumented web endpoints. |
| Cancellation cannot be implemented safely | Advertise it as unsupported/unverified; provide truthful cancellation intent/manual guidance and bounded sessions. |
| Logs only appear after completion | Expose delayed/completed logs; retain local runtime events; do not invent live output. |
| Submission outcome is ambiguous | Reconcile deterministic identity; preserve `needs_attention`; no automatic resubmission. |
| Output API refers to a latest run | Isolate attempts, retain stronger identifiers when available, and verify the result manifest. |
| Dataset readiness is delayed | Wait in preparation before starting GPU compute. |
| Private staging fails | Stop with a structured error; never make resources public as a workaround. |
| Dataset metadata requires a license | Preserve rights and use an appropriate configured metadata label; do not relicense arbitrary user data. |
| Remote model/dependency setup is expensive | Use small pinned examples, stage inputs before allocation, and measure setup separately. |
| Framework/CUDA/model incompatibility | Verify environment before business work; fail explicitly and record versions. |
| Large video input/output exceeds limits | Fail early with byte/limit details; do not truncate or introduce mandatory paid storage. |
| Remote compute survives local shutdown | Persist identity and use bounded execution; document that local shutdown is not remote cancellation. |
| Local data loss or process crash | Durable state, atomic file lifecycle, recovery tests, and documented backups. |
| Cleanup risks deleting user resources | Ledger-based ownership, dry-run default, no deletion of unknown/active resources. |
| User workload contacts paid APIs | Keep reference code free; document that free dispatch policy cannot police arbitrary workload spending. |
| Provider terms change or disallow a use | Update warnings/compatibility; do not bypass restrictions or treat OSS disclaimer as permission. |
| No live credentials in the agent environment | Finish offline work and live-test harness; mark provider acceptance unverified without asking for secrets in chat. |
| Another compute source appears | Add an adapter against the existing contract; do not rebrand the core around that provider. |

## 28. Future extensions without premature implementation

### 28.1 Additional providers

Add a new provider only after verifying its supported API, permitted use, execution environment, data transport, account/cost model, cancellation, and identity semantics. Do not assume every free notebook product can act as an unattended batch service.

A future provider may be local GPU, operator-controlled SSH compute, a legitimate free compute allowance, or another documented execution service. These are architectural possibilities, not recommendations that any particular service currently satisfies the requirements.

Keep free-only reference behavior. Paid capacity support would be an explicit separate product/configuration decision, never a fallback hidden behind an existing free profile.

### 28.2 Retained-session mode

The owner accepts exploring a mode that retains a session for multiple requests or interactive LLM use. It must be opt-in, separate from batch, and bounded by maximum lifetime and idle timeout.

This requires resolving supported connectivity, authentication, request routing, model warmup, crash behavior, session identity, quota accounting, and provider terms. A quota warning alone does not solve these technical or policy problems.

Model it as a separate `Session` resource with its own lifecycle and capability, rather than pretending an infinite-running notebook is an ordinary finite job. Do not equate releasing a GPU session with suspending and later restoring a live model in VRAM.

A future session feature must not be implemented by anti-idle evasion, hidden public tunnels, unsupported editor APIs, or account rotation. It may remain unavailable for Kaggle while being supported by a different provider.

### 28.3 Other deferred features

Potential later work includes optional SDKs, SSE events, secure job-secret references, shared immutable model caches, resumable uploads, output streaming, webhook delivery, richer compute selection, explicit workflow DAGs, or optional UI.

Each feature needs its own scope and failure semantics. None should be silently pulled into MVP merely because the API can be extended to support it.

## 29. Decision register and default answers

Use this register to avoid asking the owner the same questions again. “Default” means the agent can revise through a documented technical decision while preserving approved requirements.

| Topic | Default answer |
|---|---|
| Working project name | `compute-connector`; rename later without redesign. |
| Source license | Apache-2.0 unless the target repository already specifies otherwise. |
| Runtime language | Go; owner-approved. |
| API | HTTP/JSON with streamed binary transfers; `/v1`. |
| Job spec version | `compute-connector/v1alpha1`. |
| Deployment | Local/operator-hosted process; optional Docker, no maintainer-hosted service. |
| Provider strategy | Compiled-in adapter modules; Kaggle first, fake provider for testing. |
| First provider transport | Pinned official Kaggle CLI; narrow public-client Python bridge when needed. |
| Initial remote languages | Python and explicit remote Linux shell. |
| Storage | SQLite plus local filesystem; no mandatory broker/database service. |
| Input transfer | Upload first, allowlisted local import, public HTTPS ingestion before compute. |
| Code packaging | Immutable `.tar.gz` bundle with safe extraction; configurable only through a documented format decision. |
| Privacy | Private provider resources; never auto-publish inputs or outputs. |
| Remote secrets | Not a general MVP feature; reject unsupported injection rather than embed plaintext. |
| Workspace scope | Multiple apps of one trusted operator; scoped API tokens. |
| Compute profile | Generic alias resolved to a provider instance/config snapshot. |
| Scheduling | One active/possibly active attempt per account; workspace FIFO with round-robin fairness. |
| Automatic compute retry | None. |
| Retry recovery | Separate reconcile, collect, and explicit new attempt. |
| Provider fallback | None. |
| Quota unknown | Warn and allow bounded free-only jobs by default; optional strict blocking mode. |
| Remote network | Disabled by default; explicit job request may enable only when global/profile policy permits. |
| Provider cancel missing | Honest unsupported/manual-required path; never use delete as cancel. |
| Live logs missing | Delayed/completed logs plus truthful local events. |
| Success proof | Matching runner success + required verified outputs + provider terminal evidence. |
| Exact hardware release time | Report only when observable; otherwise distinguish terminal execution from unobservable accounting. |
| Local shutdown | Preserve active remote identity; no implicit remote cancellation. |
| Remote cleanup | Dry-run/explicit apply first; ledger-owned completed resources only. |
| First workload | Small real GPU smoke test, then small open-access LLM batch. |
| First clients | Thin Node.js, Python, and Go HTTP examples; no mandatory SDK. |
| Video implementation | Example after the generic path; no video business logic in core. |
| Retained sessions | Future separate capability/resource; not MVP. |
| Dashboard | Not MVP. |
| Platforms | Linux amd64/arm64, macOS amd64/arm64, Windows amd64, validated before claiming support. |
| Repository host/remote | Inspect the actual target; do not invent or infer a remote from unrelated projects. |
| Missing routine choice | Choose the simplest reversible implementation, record an ADR, and continue. |
| Missing external credentials | Continue offline; mark live verification blocked; never request secrets in chat. |

## 30. Final definition of done

### 30.1 Product behavior

MVP is done when an operator can install/run the runtime, configure their own supported Kaggle credentials, submit a portable Python/shell job from an ordinary HTTP client, observe honest state, retrieve verified results, and see terminal execution evidence without using a maintainer-hosted service or a mandatory paid component.

An application must not need to edit a notebook, understand provider dataset syntax, install the Kaggle client, or possess provider credentials to perform its normal job lifecycle.

### 30.2 Reliability and safety

Restart/recovery, idempotent admission, ambiguous submission, artifact-only retry, unsupported cancellation, unknown quota, workspace authorization, archive/URL safety, and ownership-safe cleanup must have tests. No automatic provider switch, silent CPU downgrade, or automatic compute retry is allowed.

### 30.3 Provider evidence

A sanitized live evidence report must support the claimed Kaggle path and list client/account-dependent capabilities. Features not exercised remain marked unverified. Provider terminal evidence and hardware-release observability must not be conflated.

### 30.4 Engineering handoff

The repository must include current documentation, configuration/schema examples, operational commands for supported platforms, a small LLM batch example, thin language clients, a fake provider, contract tests, and an implementation/decision history another contributor can follow.

### 30.5 Explicit exclusions from completion claims

Do not declare MVP incomplete merely because it lacks an interactive chat server, a second real provider, a web dashboard, a dynamic plugin system, or guaranteed remote cancellation that the initial provider cannot support.

Conversely, do not declare Kaggle support complete solely because the fake provider works, the API is generated, or a subprocess can invoke `kaggle --help`.

## 31. Instructions to the implementation agent

### 31.1 Your assignment

Treat this document as the product owner's implementation brief. Start by inspecting the repository and applicable engineering instructions, then independently prepare the research, architecture decisions, roadmap, and implementation plan.

Do not reopen the seven accepted technology/scope choices or ask whether the connector should be a library, whether Kaggle should be hard-coded, whether Docker should be mandatory, or whether interactive serving should be MVP. Those questions are already answered.

### 31.2 Decision-making authority

You may choose maintained dependencies, concrete package names, SQLite driver, HTTP router, test tooling, task decomposition, specific model/example versions, and internal table/interface details. Use the supplied defaults unless evidence justifies a better choice.

Record material choices in an ADR that states the problem, selected option, rejected alternatives, consequences, and verification method. Avoid writing ADRs for trivial naming choices.

### 31.3 Handling uncertainty without another discovery round

For a provider capability question, research official sources and run a bounded probe when authorized credentials are available. For a routine engineering choice, make the simplest reversible decision and continue. For an unavailable external prerequisite, mark the precise environment block and continue unrelated work.

Do not invent provider support, silently expand scope, or implement unsupported APIs to avoid reporting a limitation. Do not ask the owner to supply secrets through chat. Do not require product clarification for working names, minor defaults, or library selection.

### 31.4 First deliverables

Prepare the following before substantial implementation:

```text
1. A concise restatement of the approved product and MVP boundaries.
2. A requirement-to-component-to-test traceability matrix.
3. A current Kaggle capability/feasibility report with evidence levels.
4. An architecture proposal preserving the provider-neutral core.
5. An acceptance-based roadmap and dependency-aware task plan.
6. A decision/risk register with the default responses in this brief.
```

Then implement according to the actual assignment and repository workflow. Keep progress reports evidence-based: distinguish code written, tests passed, provider behavior observed, and external prerequisites not yet available.

### 31.5 External actions

Do not assume authorization to publish a repository, push to an unrelated remote, deploy a public service, spend money, allocate paid compute, or delete existing provider resources. The owner requested a self-hosted OSS project direction, not a hosted deployment or account operation.

Live tests should use only authorized credentials and the explicit finite test budget. Repository cleanup may remove only newly created/test-owned resources under the ownership ledger.

### 31.6 Compact handoff prompt

The owner may send this instruction together with this file:

> Read the entire attached proposal and all applicable repository instructions. Treat the owner-approved decisions as fixed and the implementation defaults as permission to make routine choices without asking me again. Research the current official Kaggle interfaces, distinguish documented features from live-verified behavior, and create the architecture decisions, roadmap, and implementation plan. Preserve the provider-neutral sidecar runtime, batch-first scope, private data flow, durable recovery, explicit compute retries, and no mandatory paid infrastructure. Do not assume remote cancellation, live logs, exact quota, or exactly-once execution without evidence. Mark environmental blockers precisely and continue the work that can be completed independently.

## 32. Sources and evidence limitations

### 32.1 Reference list

References below were consulted on **2026-09-13**. Mutable branches/documentation may change. The implementation agent should record a concrete released client version and, where practical, an upstream commit or tag in its own evidence report.

| Ref | Primary source | Used for |
|---|---|---|
| [S-01] | Kaggle Terms of Use | Narrow confirmation of personal/non-commercial/third-party-use wording; not a complete legal review. |
| [S-02] | Official Kaggle CLI documentation index: installation and authentication | Official authentication options and client installation boundary. |
| [S-03] | Official Kaggle CLI kernel command documentation | Batch submission, status/output, latest-run behavior, timeout option, and accelerator caveats. |
| [S-04] | Official Kaggle kernel metadata documentation | Provider-specific metadata fields, private/GPU/internet/source options. |
| [S-05] | Official Kaggle dataset command documentation | Private creation, dataset preparation/versioning, and input staging investigation. |
| [S-06] | Official Kaggle dataset metadata documentation | Required license metadata and supported labels; avoid accidental relicensing. |
| [S-07] | Official Kaggle CLI changelog | Existence of additions for kernel logs and accelerator quota; version-sensitive behavior. |
| [S-08] | Official Kaggle CLI output-format documentation | Structured output is command-specific, including quota. |
| [S-09] | Official Kaggle API extended source | Quota retrieval/formatting and a reference for structured-client investigation. |
| [S-10] | Official Kaggle CLI parser source | Reviewed command surface and absence of a kernel-cancel entry in that snapshot. |
| [S-11] | Upstream issue 1169 in the official Kaggle CLI repository | Contributor-reported cancellation/session-identity limitations; a research lead, not an API guarantee. |
| [S-12] | Official Kaggle Docker Python repository | Managed Python/GPU environment provenance and compatibility investigation. |
| [S-13] | Kaggle Notebooks documentation | Current platform limits/features to recheck; dynamic page not fully extractable in this preparation. |

[S-01]: https://www.kaggle.com/terms
[S-02]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/README.md
[S-03]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/kernels.md
[S-04]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/kernels_metadata.md
[S-05]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets.md
[S-06]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/datasets_metadata.md
[S-07]: https://github.com/Kaggle/kaggle-cli/blob/main/CHANGELOG.md
[S-08]: https://github.com/Kaggle/kaggle-cli/blob/main/docs/output_format.md
[S-09]: https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/api/kaggle_api_extended.py
[S-10]: https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/cli.py
[S-11]: https://github.com/Kaggle/kaggle-cli/issues/1169
[S-12]: https://github.com/Kaggle/docker-python
[S-13]: https://www.kaggle.com/docs/notebooks

### 32.2 What was not performed during proposal preparation

No Kaggle account was authenticated, no GPU job was submitted, no provider credential was accessed, no quota was consumed, no live cancellation was tested, and no repository was implemented or published as part of writing this proposal.

The evidence baseline is documentation/source review. Live capability tests, package/version pinning, compatibility validation, and implementation are tasks for the next agent under the operator's authorized environment.

### 32.3 Interpretation rule

When the implementation encounters a conflict between an old assumption and current official evidence, update the feasibility report and capability descriptor. Preserve the approved product boundaries and record the technical decision. Do not turn uncertainty into a silent fallback, and do not turn a missing optional capability into an excuse to abandon the provider-neutral core.

---

**End of proposal.** The intended result is a small, honest, durable compute runtime that applications can integrate once and that maintainers can extend provider by provider.
