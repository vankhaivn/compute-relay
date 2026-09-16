# Executable offline recovery fault matrix

> **Task:** M3-08; PR #18, in review until owner merge.
>
> **Requirements:** VER-02 and the DUR/DOM/OPS requirements mapped below. This qualifies
> the implemented offline component boundary, not production composition or a live provider.

## Reproduce the qualification

From the repository root with the pinned Go 1.27.1 toolchain and locked dependencies:

```text
go run ./cmd/devtool fault-test
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

The first command runs the fault qualification. `check` includes that command in addition to
format, module, contract, vet, unit-test and build checks. `test-race` remains the separate
repository-wide race run. POSIX, PowerShell and CMD wrappers accept `fault-test` unchanged.
No new workflow, provider credential, remote compute or deployment is required.

The versioned [catalog](../internal/devtool/fault-matrix.json) is the machine-readable mapping
from the approved [proposal section 23.2](proposal.md#232-required-fault-scenarios) to exact
source-file/test-function identities. It contains 25 numbered cases and 34 distinct root
tests; some roots contribute to several cases. The count is not a coverage percentage and
subtests are not miscounted as independent root tests.

The checker parses the numbered list in the exact proposal section and compares complete
scenario text in order. Substrings elsewhere, swapped scenarios, missing/extra cases,
renumbering or duplicate sections cannot qualify; LF/CRLF checkouts are equivalent. It checks
all function references against parsed source, then executes those named tests using fresh
`go test -json -count=1` output. Each required package must start and finish, and each
nominated root must run and pass. A missing or skipped root, a skipped descendant, test/build
failure, incomplete package, malformed stream or exceeded budget prevents qualification.
A green package with no matching test is insufficient. There is no saved-log import or
cached-result fallback.

Successful output includes FM01 through FM25, exact source/test references, actual compiler
and platform, and `25/25 passed-offline`. Inspect the PR's final-head evidence comment for the
commit and CI run that produced it; an earlier green checkpoint is not final-head evidence.

Output is bounded to 1 MiB per JSON event, 64 MiB total and a 32 KiB failure-diagnostic tail.
The invocation has a 15-minute context budget and each test package a five-minute timeout.
These are developer-test limits, not remote-workload deadlines. The runner uses a fixed
argument vector and executes repository tests only; it never launches admitted workloads.

Source presence and an observed pass cannot prove that assertions are semantically adequate.
Review the tests and evidence limits below when changing the catalog. Do not remove a case,
weaken a rejection or nominate a trivial test merely to obtain a green qualification.

## Evidence map

`S` below denotes `internal/store/sqlite/`. File/test pairs are explicit in the linked
catalog; the names below identify the principal assertions, not just the package that ran.
Every row also depends on VER-02.

| Case | Scenario and required outcome | Executed evidence | Other requirements / evidence boundary |
|---|---|---|---|
| FM01 | Lost admission response: replay the original job/attempt, including after restart. | `TestLostHTTPReceiptReplaysCommittedIDs`; `TestAdmissionReplaySurvivesRemapDisableAndRestart` | DUR-01/02, API-03. Actual HTTP response-writer fault and SQLite reopen; no provider execution. |
| FM02 | Same key with a changed specification must conflict, not create another job. | `TestAdmissionConcurrentKeysAndConflicts` | DUR-02, JOB-01. Twenty concurrent local callers plus changed-request and workspace checks. |
| FM03 | Crash before/after submission intent: preserve the committed gate and never infer permission to resubmit. | `TestDispatchProcessKillAtMutationIntentBoundaries` | DUR-03. Real Go child-process kill around preparation/submission transactions; remote state is synthetic and the child makes no provider call. |
| FM04 | Provider acceptance with a lost response: recover the same identity with one submission. | `TestDispatchLostSubmitResponseRestartAndTerminalCollectionGate` | DUR-03, PRV-02. Fake acceptance/response loss plus real SQLite reopen; terminal observation alone does not publish artifacts. |
| FM05 | Inconclusive rediscovery must retain uncertainty and account capacity. | `TestDispatchUnknownNotFoundStopsAndKeepsAccountCapacity` | DUR-03, OPS-01. Repeated not-found observations cannot prove non-acceptance. |
| FM06 | Unknown/new provider state must not erase confirmed evidence or invent success. | `TestFaultMatrixUnknownAndModifiedObservations` | DOM-02/03, PRV-02. Unknown and invalid enum observations at the provider port; not a live CLI-format compatibility test. |
| FM07 | A stale poll after terminal evidence must be rejected without changing state/events. | `TestFaultMatrixStalePollCannotRewriteTerminal` | DOM-03, DUR-03. Both stale and current handles reject a late nonterminal poll. |
| FM08 | Restart while remote work is active must preserve one attempt and its remote reference. | `TestFaultMatrixRestartKeepsActiveAttempt` | DUR-01/03, OPS-01. SQLite reopen while the synthetic backend remains in the parent process. |
| FM09 | Halfway failure in a large output must publish nothing and recover without compute. | `TestFaultMatrixLargeTransferFailureRecoversWithoutCompute` | JOB-05, DUR-01/03. A 16 MiB synthetic file fails after 8 MiB; explicit collection after reopen retains the original pin and verifies exact bytes. No partial-byte range resume claim. |
| FM10 | Missing required outputs or wrong digests must not become available results. | `TestCollectionBadTransfersRequireExplicitRecovery` | JOB-05, PRV-02, DOM-02. Missing output/manifest, wrong nonce/digest, partial/late-error/overflow/panic and cursor-cycle cases. |
| FM11 | Collect all pages, not just the first page. | `TestCollectionPublicationReceiptAndScopedReads` | JOB-05, PRV-02. Forced two-file pages, at least three list calls, all selected blobs read back; unselected scratch bytes are not fetched. |
| FM12 | Cancellation racing completion must preserve the winning terminal evidence. | `TestCollectionCancellationRaceKeepsTerminalEvidence`; `TestControlsUnsupportedAcceptedAndConfirmedCancellation` | DOM-02/03, OPS-04. Stale publication is fenced; completion can be too late for cancellation, not relabeled cancelled. |
| FM13 | Unsupported cancellation or missing session identity must remain explicit and unresolved. | `TestControlsUnsupportedAcceptedAndConfirmedCancellation`; `TestFaultMatrixCancellationWithoutRemoteIdentityStaysUnresolved` | OPS-04, PRV-03. Capability/identity absence makes zero cancel calls; no fabricated termination or replacement compute. |
| FM14 | A local deadline with unknown remote activity must not free capacity or mark remote timeout. | `TestFaultMatrixLocalDeadlineKeepsUnknownExecution`; `TestSchedulerAmbiguousCapacitySurvivesExpiryAndRestart` | OPS-01/03, DOM-02. Real local observation-invocation timeout plus a controlled persisted deadline-exceeded state fixture. Does not implement or prove a production overall-job deadline service. |
| FM15 | Missing/stale/exhausted/external-consumption quota must follow explicit policy, not invented allowance. | `TestQuotaDecisionsPreserveUnknownAndPrecision`; `TestSchedulerQuotaLatchAndStrictPolicy`; `TestFaultMatrixQuotaConsumedAfterPositiveObservation` | OPS-01/02. Synthetic quota and a proven rejection after a positive observation; no live account quota is queried or consumed. |
| FM16 | Staging upload success is not readiness; wait without repeating Prepare. | `TestDispatchStagingLossAndDelayedReadinessNeverRepeatPrepare` | DAT-04, DUR-03. Delayed readiness/lost acknowledgement at the provider port; actual provider privacy/readiness needs M1/M4 evidence. |
| FM17 | A redirect into a private destination must be stopped before the unsafe connection. | `TestEveryRedirectRevalidatesDNSAndPolicy` | DAT-03, SEC-02. Real loopback TLS fixture and controlled DNS/peer mapping; no external HTTP request. |
| FM18 | Reject archive traversal, links, duplicates/collisions and excessive expansion. | `TestArchiveMaliciousPathCorpus`; `TestArchiveCollisionAndTypeCorpus`; `TestArchiveBoundsAndManifestValidation` | DAT-02, SEC-02. Actual generated archive bytes and strict inspection; no unsafe extraction. |
| FM19 | Foreign-workspace input/artifact references must be denied. | `TestDurableAdmissionHTTPContractAndAuthorization`; `TestCollectionPublicationReceiptAndScopedReads` | API-02, PRD-07. HTTP input authorization and current-authority internal artifact reads, including revoked tokens and foreign attempts/IDs. No new artifact HTTP endpoint. |
| FM20 | A second runtime process must not open the owned state directory. | `TestProcessKillPreservesCommittedWALAndRollsBackIncompleteTransaction` | DUR-04. Parent open fails while the real child owns the lock; reopen after kill preserves committed WAL only. |
| FM21 | Disk failure during upload/collection/commit must not expose partial success. | `TestIncompleteInvalidAndFaultedWritesAreNeverVisible`; `TestCollectionPublicationRollbackAndDiskFull`; `TestSQLiteDiskFullRollsBackWithoutReplay` | DUR-01/03. Upload write/free-space faults are injected; collection-publication and database tests produce actual SQLITE_FULL. Does not fill the host disk or claim sudden-power-loss coverage. |
| FM22 | Credential/account changes must preserve the original binding rather than select a fallback. | `TestFaultMatrixCredentialRotationKeepsFrozenAccount`; `TestDispatchFrozenProfileDoesNotFollowRemap` | SEC-01, PRV-02, JOB-04. Synthetic effective-account rejection and same-account recovery; no credential value or live rotation is involved. |
| FM23 | Externally modified resource identity must fail closed and retain recovery evidence. | `TestFaultMatrixUnknownAndModifiedObservations` | PRV-02, DUR-03. A changed provider resource version is injected; no real remote resource is modified. |
| FM24 | Repeating cleanup for an already-absent owned resource must be safe. | `TestCleanupPreviewUsesLedgerAndNeverApplies` | OPS-05. Qualifies M3-07's dry-run-only repeated absence, exact ownership and frozen binding. Remote apply and staging preview remain unimplemented; this is not evidence of live remote deletion. |
| FM25 | A nonzero CLI exit after a possible effect must not be treated as non-acceptance. | `TestFaultMatrixNonzeroCLIAfterAcceptanceDoesNotResubmit` | DUR-03, PRV-02, OPS-03. A real repository-owned helper exits 23 after synthetic acceptance; reopen reconciles without a second submission. Not an official Kaggle CLI or live side-effect test. |

## Supporting invariants beyond the 25 rows

The full `check` and `test-race` suites continue to run all existing tests, not only these
34 roots. They include private-staging refusal, input snapshot/URL freeze, immutable receipt
truth tables, schema contracts, collection publication and lease races, expiry/event rollback,
retention/root-binding safety, and secret-canary checks. The focused matrix does not replace
those suites, runner tests, the M6 security audit or the separate live acceptance gates.

The helper-only `TestFaultMatrixCLIChild` is intentionally not catalog evidence. Its normal
suite invocation skips; the nominated parent must actually observe exit 23. A skip of any
catalogued root or descendant is still a qualification failure, not an allowed helper skip.

## Evidence record and milestone interpretation

PR #18 records exact-head CI runs, actual qualification output and disclosed local limits.
New observation/transport, large-transfer and policy regressions were committed before the
checker; documentation is committed separately. Checkpoints are pushed rather than held only
in a disposable Linux runtime. Never equate a draft checkpoint with a completed gate.

Local engineering used Go 1.23.2. Exact checker code passed vet, race/negative tests, actual
Go event-protocol fixtures and bounded fuzz in an unshipped standard-library harness. The
continuation separately ran the exact numbered-section parser and its table tests with
`go test -race -count=10 ./...` in a temporary standard-library module. These are checker/parser-only
evidence: neither run qualified the full catalog against local SQLite/modernc or the pinned
Go 1.27.1 repository. Full-stack/native/race evidence comes from offline CI.

After owner merge and passing qualification, M3's implemented **offline** orchestration gate
can be recorded complete. This does not authorize M4 or prove a complete installed runtime.
M1 live acceptance remains blocked; production `serve`, artifact HTTP/CLI, remote cleanup
apply, provider-specific staging cleanup and live compatibility keep their own gates.

See [recovery semantics](recovery.md), [implementation plan](implementation-plan.md),
[retention](retention.md), [collection](collection.md) and the requirement
[traceability matrix](scope-and-requirements.md). Stop after PR #18 for owner review/merge.
