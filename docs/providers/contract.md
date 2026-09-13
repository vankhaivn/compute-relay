# Provider ports and deterministic fake

> **Task:** M2-04. **Status:** implemented offline; in review until merged.
>
> **Requirements:** PRD-03, PRV-01, PRV-03, VER-01; supporting DOM-02/03,
> DUR-03, OPS-04/05, and artifact identity invariants.

## Boundaries

`internal/provider` defines the required finite-batch interface and a registry of explicitly
configured instances. `internal/ports` defines consumer-sized Store, BlobStore,
CredentialResolver, Clock and EventSink interfaces. `internal/provider/fake` implements the
provider boundary with predetermined in-memory fixtures. None imports Kaggle, networking,
host filesystem access, or process execution. An import-boundary test enforces this.

The composition root registers instances; there is no plugin loader, reflection-based
factory framework, implicit provider selection or fallback. Registration rejects duplicate
instances, nil adapters, invalid capability evidence, and advertised optional capabilities
without corresponding methods. Reflection is used only to reject typed-nil interfaces.

The public M2-03 schemas and domain enums are unchanged. These are internal Go seams, not a
published adapter ABI and not a new application-facing job format.

## Required and optional behavior

The required Provider surface is Describe, Check, Validate, Prepare, Submit, Observe,
ReconcileSubmission, ListArtifacts, FetchArtifact and Cleanup. Cancellation, log reading and
quota reading are separate optional interfaces.

A `ResolvedJob` is a post-admission snapshot: the caller validates the M2-03 JobSpec schema,
resolves workspace/profile/policy, freezes inputs, derives required capabilities and the
finite wall budget, and persists identities. The port checks identity/digest/budget
consistency; it is not a duplicate public schema validator. The initial internal ceiling is
86,400 wall seconds, matching the bounded contract direction; adapters/policies can be stricter.

Plans copy mutable specification and capability slices. Preparation binds the full plan
fingerprint, immutable inputs, attempt nonce, provider instance and operation identity.
Upload/preparation completion does not imply readiness. The fake can delay readiness until
an explicit test-control action.

`Submit` returns an explicit outcome, never a bare successful-looking zero value:

| Outcome | Required evidence and next action |
|---|---|
| `accepted` | A validated reference matching the prewritten attempt identity; observe it. |
| `rejected` | Proven non-acceptance of this request and a structured reason. |
| `unknown` | Compute may have started; no safe resubmit flag; reconcile existing identity. |

Always call `SubmissionOutcome.Validate(expectedIdentity)` before consuming a result.
Reconciliation also validates against the expected identity. `not_found` is a lookup result,
not proof that an uncertain request was rejected, and never authorizes automatic resubmission.
There is no retry loop or hidden compute retry in the port or fake.

Missing optional capabilities stay explicit: cancellation yields `manual_required` without
termination confirmation, logs are unavailable, and quota values remain nil/`unknown`.
Optional output validators reject contradictory cancellation, invented logs, nonfinite
quota numbers and known quota without values/units/provenance. Unknown hardware requirements
are retained as `VerifyAfterStart`; the fake never converts an unverified GPU requirement
into a successful CPU result.

## Artifacts and cleanup

References include installation/workspace/job/attempt/instance/intent identity, resource
key, nonce and frozen digests. Artifact metadata and pagination cursors are execution-scoped.
Collection rejects another attempt/version, unsafe paths, invalid lengths and wrong digests.
`CopyVerified` streams into an unpublished destination and never writes beyond the declared
length or policy bound. On error the caller must discard partial bytes. Reader/writer
implementations remain responsible for cancelling blocking I/O.

The synthetic `execution-result.json` identifies its runner as `fake-fixture-1` and carries
the correct job/attempt/nonce/digests. `ValidateManifestIdentity` is only the identity gate.
M3 must additionally validate the full result schema, required output set and execution
outcome before publishing success. Fixture GPU flags are simulation data, not live evidence.

Cleanup is separate from cancellation. It requires explicit `dry_run` or `apply`, an exact
remote reference, creation-operation identity, a ledger reference and the caller's assertion
that results are collected. The adapter checks terminal state and resource identity. M3
must verify the actual durable ledger row and collection evidence before invoking this port;
a syntactically valid ledger ID alone is not authorization. Repeating cleanup of an already
removed owned fixture is idempotent; unknown or active executions cannot be deleted.

## Infrastructure seams, not premature implementations

`Store.CommitAttempt` requires one atomic compare-and-swap of an attempt revision plus its
sequenced event. `AttemptChange.Validate` checks identity, revision, transition and event
association before storage. The future implementation must also check the stored revision
and next per-job sequence inside the transaction. EventSink is optional post-commit delivery,
not a second authoritative event store. No transaction is held during provider calls.

BlobStore accepts workspace-scoped object identities and streaming readers rather than host
paths. CredentialResolver scopes transient operator credentials to a callback; no production
environment/file resolver is implemented here. Clock allows deterministic tests without
sleeping. SQLite, filesystem publication, authentication and durable admission remain later
tasks, not completed capabilities of this change.

## Local tests and smoke command

With the repository's pinned Go toolchain, run from the repository root:

```bash
go test ./internal/provider/... ./internal/ports/...
go test -race ./internal/provider/... ./internal/ports/...
go test -count=100 ./internal/provider/... ./internal/ports/...
go vet ./internal/provider/... ./internal/ports/... ./cmd/provider-smoke
go run ./cmd/provider-smoke
```

The smoke command accepts no workload/config/credential arguments. It uses only the fake,
registers it, prepares/submits one simulated execution, advances predetermined observations,
collects two digest-checked artifacts, checks manifest identity, and reports:

```json
{"artifacts_verified":2,"cancellation":"manual_required","evidence":"implemented_offline","provider":"fake","provider_network_calls":0,"quota":"unknown","simulated_executions":1,"workload_executed":false}
```

The reusable `internal/provider/providertest` suite receives a factory and controlled fixture
hooks rather than importing a concrete adapter. Future adapters can use it with offline
transports. It must never be pointed at an operator account. The minimal fake actually lacks
Cancel/ReadLogs/ReadQuota methods; a separate Complete fixture supplies them to test capability
negotiation and cancellation/completion races.

Tests also cover accepted-but-response-lost, unresolved submission, proven rejection, delayed
readiness, reconnect without a second submission, stale domain rollback, cross-attempt
artifacts/cursors, partial-transfer retries, concurrent explicit attempts, unknown states,
unsupported GPU, credential canaries and literal workload commands that are never executed.

### Evidence from this implementation session

New packages and the local smoke executable were built/tested on Linux amd64 using the
available Go 1.23.2. The nine imported production domain files were verified byte-for-byte by
Git blob SHA against `main` at `a8d6f76c9354cb30734cdb700d78b012403c2e72`.

The sandbox could not download the pinned Go 1.27.1 toolchain or the existing schema-validator
modules. Local checks therefore used a temporary **untracked** alternate module file with
the same module path and no dependencies, selecting only the new standard-library-only
packages and unchanged domain dependency. The committed `go.mod`, `go.sum`, public schemas
and workflows are unchanged. Full-repository/exact-toolchain/native-platform results are
reported separately in the PR; targeted local success is not claimed as that verification.

Local format, vet, unit/contract, race, 100-repeat, CGo-free build and smoke checks passed.
No Kaggle credential was read, no real provider resource was created/deleted, and no GPU ran.
Reconnecting to the same in-memory fake backend simulates observer recovery; it does not
prove persistence across an OS process crash. That requires M3 fault tests.

## OSS runtime and CI policy

Operators configure credentials in their own runtime environments. GitHub Actions remain
code-quality, build and offline test infrastructure only: no secret-backed Kaggle runtime,
deployment or GPU workflows. The local smoke above is not added as an Actions runtime job.
Existing Go CI picks up ordinary offline unit/contract tests. Live probes remain separate
operator-run opt-in commands with explicit authorization and finite budgets.
