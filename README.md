# Compute Relay

> **Project status:** portable-core components and M3 durable orchestration are complete
> at their implemented offline gates; PR #18 is merged. M4-01's local/read-only Kaggle
> preflight foundation is in review in PR #19. M1 live acceptance and the integrated batch
> adapter remain gated. Production `serve`, artifact HTTP and remote cleanup apply are
> separate work; offline preflight is not live Kaggle or GPU readiness.

Compute Relay is the repository for an open-source, self-hosted **compute connector
runtime**. The intended runtime sits beside an application, accepts finite jobs through a
provider-neutral HTTP/JSON boundary, and coordinates execution on external compute
providers. Kaggle is the first provider to investigate and implement; it is not the identity
of the core product.

The approved product direction is captured in [`docs/proposal.md`](docs/proposal.md). That
proposal uses **Compute Connector** as a working name. This repository uses **Compute
Relay**; public executable, package, and API naming remain provisional until recorded in an
architecture decision.

## Product direction

The intended product will:

- run on infrastructure controlled by the operator;
- use the operator's own provider account and credentials;
- execute bounded Python or shell jobs through an explicit job contract;
- preserve durable local job and attempt state;
- stage inputs, observe execution, and retrieve verified artifacts;
- expose unsupported, unknown, or unverified provider capabilities honestly; and
- keep provider-specific behavior behind adapters so applications integrate once.

It is **not** a hosted GPU service, transparent remote VRAM, a CUDA proxy, an always-on
model server, a remote desktop, or a guarantee of immediate GPU allocation. The reference
workflow must not require a maintainer-operated service or mandatory paid infrastructure.

## Repository state

The repository currently establishes:

- approved proposal, provider-neutral architecture baseline, and stable requirement IDs;
- Apache-2.0 licensing and OSS governance/security/contribution policies;
- mandatory instructions for human and automated contributors;
- Conventional Commits documentation, local hooks, and CI validation;
- an acceptance-based roadmap and dependency-aware M-0 through M-6 implementation plan;
- a dated review of Kaggle CLI `v2.2.4` and K-01 through K-16 feasibility ledger;
- initial ADRs for the official-client boundary, attempt-scoped Kaggle identity, and
  Go/SQLite baseline;
- a risk register, compatibility target matrix, and live-verification checklist;
- Go tooling, a pre-release help/version/bundle executable, strict API schemas, domain
  state semantics, provider ports and a deterministic fake provider;
- workspace token/authorization services, guarded loopback HTTP, immutable upload/import/
  HTTPS snapshots, finite runner assets and offline smoke/fault tests;
- SQLite installation/workspace/token-digest/object metadata, embedded migrations,
  OS state locking and consistent database-only backup/restore;
- durable job/attempt admission, immutable profile resolution and object pins, original
  receipt replay, state/event transactions and authenticated create/validate/status handlers;
- transactional FIFO/round-robin scheduling, shared-account capacity, fenced local leases,
  bounded local workers and explicit quota uncertainty policy;
- immutable input preparation, private-staging and submission intent ledgers, exact provider
  binding snapshots, one-shot mutations and read-only same-attempt recovery;
- durable attempt-scoped cancel/retry/reconcile/collect records, immutable receipts,
  current operation status, one-shot cancellation and authenticated control HTTP contracts;
- a bounded transfer-only collector, immutable per-attempt result snapshots, verified local
  blobs, atomic artifact/state/event publication and authenticated internal result reads;
- named retention holds, irreversible expiry/audit, exact bound-store local byte deletion,
  expired-input admission/retry guards and authenticated exact-ledger remote dry-run previews;
- an executable 25-scenario fault catalog with fresh named-test qualification, linked
  evidence boundaries and a recovery/state-semantics guide; and
- explicit environment-reference resolution and a bounded local/read-only Kaggle preflight,
  with server-account checking, a fixed isolated SDK helper and an opt-in operator command.

Admission returns `202` only after committing local metadata. It does not fetch pending URL
inputs, inspect bundle bytes, start a scheduler or allocate compute. Scheduler/dispatch
composition is explicit and migration starts paused. A scheduler claim alone never permits
remote submission: the orchestration component must verify inputs and commit each intent
before its one allowed provider mutation. The supplied bound provider is a nonexecuting fake.
Terminal provider observation opens collection, not job success; status exposes cached
structured recovery conditions without polling or replaying compute.

Control requests explicitly name an attempt and require an idempotency key. POST replay
returns the original receipt; GET returns current operation state. Cancellation intent is
not termination evidence, and compute retry preserves the original frozen inputs and binding.
Collect accepts a durable transfer-only ticket. The separately composed M3-06 engine consumes
it, verifies the manifest and every selected blob, and publishes the complete metadata set
atomically. Restart and explicit collection retry reuse the original result pin, never
another compute execution. No accepted ticket alone implies available artifacts.

M3-07 retains active, ambiguous, held and recovery-required references regardless of age.
Default eligibility windows are 24 hours for unreferenced inputs and seven days for safely
completed references/results. Expiry commits before byte deletion; `result.expired` is a
sequenced event that does not rewrite execution outcome or original receipts. Input and
result roots have separate persistent identities. Metadata/ownership history stays retained;
remote previews never apply deletion, and staging preview remains explicitly unavailable.

M3-08 qualifies the implemented offline boundary against all 25 numbered proposal fault
scenarios using 34 distinct nominated tests. `fault-test` checks exact scenario/source
identity and rejects missing, skipped, failed or incomplete fresh test evidence. The
[matrix](docs/fault-matrix.md) explains what each test proves and what remains unverified;
25/25 is not a coverage percentage or a live-provider guarantee. The
[recovery guide](docs/recovery.md) separates receipt recovery, reconciliation, transfer retry,
explicit compute retry, cancellation, expiry and cleanup preview.

M4-01's [preflight](docs/providers/kaggle-preflight.md) defaults to installed-version checks
without credential lookup or provider calls. Explicit read-only opt-in uses one selected
environment token, compares a server-returned account identity before querying quota-data
availability, and returns sanitized observations. It always reports `batch_ready=false` and
does not implement/register a dispatch-capable Provider. No M1 live gate is waived.

The upload smoke retains its explicitly nondurable metadata fixture; store/admission/scheduler/
dispatch checks use temporary SQLite. A database-only backup does not include input or result
blob bytes or prove full runtime recovery. M3's offline gate closed with PR #18's owner merge.
These components and the separate preflight utility are not a complete production
orchestration service; production `serve` and live-provider acceptance remain separate gates.

Implementation claims must be backed by code, tests, and—where provider behavior is
involved—dated evidence. A passing fake-provider test will not be described as proof of
live Kaggle support.

## Documentation map

| Document | Purpose |
|---|---|
| [`docs/proposal.md`](docs/proposal.md) | Approved product direction, defaults, feasibility gates, and boundaries. |
| [`AGENTS.md`](AGENTS.md) | Mandatory repository-wide instructions for humans and coding agents. |
| [`docs/scope-and-requirements.md`](docs/scope-and-requirements.md) | Requirement-to-component-to-test traceability. |
| [`docs/architecture.md`](docs/architecture.md) | Provider-neutral architecture baseline and invariants. |
| [`docs/auth-and-objects.md`](docs/auth-and-objects.md) | Implemented workspace auth, HTTP/upload boundary, blob recovery and smoke checks. |
| [`docs/storage.md`](docs/storage.md) | SQLite repositories, locking, migrations, database-only backup/restore and evidence limits. |
| [`docs/admission.md`](docs/admission.md) | Durable job acceptance, canonical request identity, replay, frozen references and pending preparation. |
| [`docs/scheduler.md`](docs/scheduler.md) | Durable fairness, account/worker reservations, fenced leases, quota policy and the local-only phase boundary. |
| [`docs/dispatch.md`](docs/dispatch.md) | Input freeze, private staging, one-shot intents, reconciliation and the collection handoff. |
| [`docs/operations.md`](docs/operations.md) | Explicit attempt controls, immutable receipt replay/current GET, cancellation evidence, frozen-input retry and transfer-only tickets. |
| [`docs/collection.md`](docs/collection.md) | Pinned result verification, bounded transfers, atomic publication, scoped reads, recovery and directory limitations. |
| [`docs/retention.md`](docs/retention.md) | Pin-aware expiry, exact bound-store byte sweep, metadata preservation, remote dry runs and recovery limits. |
| [`docs/fault-matrix.md`](docs/fault-matrix.md) | All 25 fault cases, exact nominated tests, executable qualification and offline/live evidence limits. |
| [`docs/recovery.md`](docs/recovery.md) | Operational state interpretation and safe recovery by durable boundary, without invented runtime commands. |
| [`docs/providers/kaggle-preflight.md`](docs/providers/kaggle-preflight.md) | Explicit local/read-only commands, credential scope, pinned SDK transport, bounded reports and the unchanged live gate. |
| [`api/README.md`](api/README.md) | Versioned schemas and operation-level implementation status. |
| [`docs/roadmap.md`](docs/roadmap.md) | Acceptance-based M-0 through M-6 outcomes and status. |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | Dependency-aware executable task plan and authorization boundaries. |
| [`docs/research/kaggle-interface-review.md`](docs/research/kaggle-interface-review.md) | Pinned official-client source review and M-1 probe sequence. |
| [`docs/research/kaggle-feasibility.md`](docs/research/kaggle-feasibility.md) | K-01 through K-16 evidence ledger and go/no-go rule. |
| [`docs/risk-register.md`](docs/risk-register.md) | Ranked risks, predetermined responses, and evidence gates. |
| [`docs/compatibility.md`](docs/compatibility.md) | Toolchain/provider/host targets and live-verification checklist. |
| [`docs/decisions/`](docs/decisions/README.md) | Accepted and proposed architecture decisions. |
| [`docs/development/commit-convention.md`](docs/development/commit-convention.md) | Canonical commit-message rules. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Contributor workflow and quality expectations. |
| [`SECURITY.md`](SECURITY.md) | Vulnerability and credential-disclosure reporting. |

See [`docs/README.md`](docs/README.md) for the complete documentation system.

## Local developer smoke and qualification

With the pinned Go toolchain, from the repository root:

```text
go run ./cmd/uploadsmoke
go run ./cmd/storesmoke
go run ./cmd/admissionsmoke
go run ./cmd/schedulersmoke
go run ./cmd/dispatchsmoke
go test -run=TestCollection ./internal/store/sqlite
go test -run='TestRetention|TestCleanupPreview|TestDelete' ./internal/store/sqlite ./internal/blobfs ./internal/retention
go run ./cmd/devtool fault-test
go run ./cmd/kagglepreflight --help
```

The upload check exercises loopback HTTP, synthetic workspace tokens, streamed upload,
digest verification, isolation, revocation and reopened temporary blobs with fixture
metadata. The store check separately exercises actual SQLite persistence, revocation and
database-only backup/restore. The admission check verifies concurrent/restarted receipt
replay and changed-request conflicts. The scheduler check verifies shared-account capacity,
round-robin restart and stale fencing with explicitly advanced test time. Store/admission/
scheduler checks report `blob_bytes_checked=false`; no job command is executed by them.
The dispatch check additionally verifies real bundle/input bytes, one simulated submission
with a lost response and same-attempt reconciliation after database reopen. Its terminal
fixture stops at collection, with no result-success claim or actual workload execution.
The collection component tests separately verify actual result blobs, complete publication,
pagination and recovery against a synthetic provider. Retention tests exercise temporary
byte expiry/deletion, root identity and synthetic remote previews. They never execute the
fixture command or apply remote cleanup.

`fault-test` runs the nominated fault regressions uncached and is also included in
`go run ./cmd/devtool check`. Repository-wide race tests remain a separate
`go run ./cmd/devtool test-race` command. The matrix documents actual process-kill and SQL
faults separately from injected provider observations, local deadlines and dry-run outcomes.
No admitted workload or official live provider CLI is invoked by qualification.

These checks and preflight help are finite and never call Kaggle. None is a production
runtime. Read the preflight guide before explicitly enabling its authenticated network mode.
See the component and fault-matrix guides for their distinct evidence limits and
local-versus-CI verification disclosures.

## Current execution boundary

Provider credentials and live probes belong to each operator's own runtime. GitHub Actions
are offline code-quality/build/test checks only, not deployment or GPU execution workflows.
The local Linux engineering runtime is used for executable smoke checks where available.

Live Kaggle tasks require explicit authorization for the exact credential use and finite
side effect:

- read-only authentication/quota probes;
- private synthetic staging resources;
- bounded GPU smoke/timeout/log/restart tests; and
- identity-safe test-resource cleanup.

Credentials must not be pasted into chat, committed, passed to remote jobs, or captured in
fixtures. The owner's request for offline/read-only M4-01 preparation does not waive M1-08
or authorize live side effects in this session. Follow the implementation plan's owner-review
and provider-evidence gates; preflight success never grants dispatch permission.

## Contributing

Read [`AGENTS.md`](AGENTS.md), [`docs/proposal.md`](docs/proposal.md), and
[`CONTRIBUTING.md`](CONTRIBUTING.md) before changing the repository.

Enable the versioned local Git hook and commit template with one of:

```bash
./scripts/setup-git-hooks.sh
```

```powershell
./scripts/setup-git-hooks.ps1
```

```cmd
scripts\setup-git-hooks.cmd
```

Every commit subject must follow Conventional Commits, for example:

```text
docs(repo): bootstrap open-source project
feat(provider): add deterministic fake adapter
fix(store): preserve ambiguous submission state
```

The default collaboration path is a focused branch and pull request. Direct pushes to
`main` require explicit owner authorization for the specific task.

## Security and provider responsibility

Never commit provider credentials, runtime API tokens, account exports, real job inputs, or
private artifacts. Do not paste secrets into issues or chat. Provider availability,
eligibility, quota, terms, and pricing can change; operators remain responsible for their
accounts, workloads, data rights, and provider compliance.

See [`SECURITY.md`](SECURITY.md) for private reporting guidance.

## License

Compute Relay is licensed under the [Apache License 2.0](LICENSE). Third-party services,
uploaded data, models, dependencies, and generated artifacts retain their own terms and
licenses.
