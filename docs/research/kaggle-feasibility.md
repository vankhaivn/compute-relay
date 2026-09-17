# Kaggle feasibility and live evidence gates

This ledger preserves the stable **K-01–K-16** questions from the proposal. Upstream source
review at **2026-09-13** used CLI 2.2.4; current component references also pin SDK 0.1.35.
The [interface review](kaggle-interface-review.md) links the exact sources. No operator live
run is recorded by this documentation cleanup. Offline implementation and live evidence are
different columns, not interchangeable completion labels.

Use [current status](../status.md) for shipped features and the
[validation checklist](../development/validation-checklist.md) for executable steps. Record
actual results/bugs in [validation results](../development/validation-results.md); do not copy
CI summaries into this ledger as account evidence.

## Gate matrix

All live conclusions below remain **unverified** until a dated, authorized result is linked.
A missing operation can instead require a documented unsupported/blocked conclusion; it must
not be replaced by an invented command or unsafe fallback.

| Gate | Question | Existing boundary | Evidence still needed / safe response |
|---|---|---|---|
| K-01 | Does the configured credential identify the expected account? | Explicit token resolver and read-only preflight. | Authorized positive and missing/invalid-credential results; no secret disclosure. |
| K-02 | Can that account run the selected GPU? | Fixed CUDA arithmetic acceptance example, no CPU fallback. | Actual tensor/device/calculation and hardware report under a finite approved budget. |
| K-03 | Do multi-file code and private inputs arrive intact? | Private attempt staging and frozen marker/file hashes. | Real private visibility, bundle/input hashes, rights-compatible metadata and test-owned identity. |
| K-04 | Is staging ready before compute? | Readiness observed separately from creation/upload. | Real processing/readiness evidence and fail-closed behavior for incomplete/unready input. |
| K-05 | Is the execution bound to one intended attempt? | Prewritten slug, exact kernel ID/version/source and nonce. | Original-identity acceptance/recovery evidence; no remote exactly-once or same-version rerun guarantee. |
| K-06 | Are observed raw states interpreted truthfully? | Versioned raw-field mapping and retained uncertainty. | Sanitized real lifecycle/failure observations; absent/new values must not become success. |
| K-07 | Are all correct-attempt results retrieved? | Complete pagination, explicit file/version reads and immutable M3 publication. | Actual manifest/required output hashes and pagination evidence; the fixed six-file run alone need not exercise multiple pages. |
| K-08 | What logs are available and when? | Bounded delayed/after-completion snapshots; no live SSE implementation. | Account/run timing and completeness evidence, or an explicit availability limitation. |
| K-09 | Is the requested remote timeout enforced? | Separate invocation, runner and provider budgets. | Approved small timeout experiment with terminal evidence; local timeout is insufficient. |
| K-10 | What termination/release evidence exists? | Execution/result/release dimensions remain separate. | Recorded provider terminal facts and explicit unobservable hardware/accounting details. |
| K-11 | Is a cancellable session target verifiable? | Batch cancellation is manual-required; no guessed session ID or delete-as-cancel. | Supported public operation plus reliable target evidence, or an explicit unsupported conclusion. |
| K-12 | What quota, age and precision are available? | Raw duration/reservation-aware account Monitor; missing fields stay unknown. | Authorized account observations, negative/missing data behavior and observed units; no guessed entitlement/reset. |
| K-13 | Which environment actually ran? | Bounded Python/framework/CUDA/device provenance and resource checks. | Real runtime versions/device/memory and needed workload dependencies, not local client metadata. |
| K-14 | Does staged execution work with internet disabled? | Explicit provider network setting and no-download fixed acceptance job. | Actual staged run and observed configuration; the runner itself is not a firewall or network-isolation proof. |
| K-15 | Does restart preserve the original execution? | Durable intent/recovery plus different-process acceptance records. | Real resume evidence; separately authorized response-loss/fault checks remain distinct from normal restart. |
| K-16 | Can cleanup remove only owned inactive resources? | Ownership ledger/local retention; remote apply and staging cleanup are not shipped. | A reviewed exact-target procedure and authorized evidence, or a recorded implementation blocker. Retain unresolved resources/state. |

## Go/blocked decision

A scoped `passed-live` GPU/restart report demonstrates that fixed experiment only. The report
still keeps `full_m1_acceptance=false`. Evaluate the remaining timeout, fault, pagination,
capability and cleanup criteria before proposing the M1-08 decision. Required missing evidence
must stay blocked; optional absent capabilities may be explicitly unsupported rather than faked.

Do not close a row because its feature exists upstream, a mocked SDK test passes, a kernel
reports complete, or the local process exits zero. Every promoted conclusion needs exact
source/executable/client identity, scope, time, authorization and sanitized supporting evidence.
A failure of private staging, identity-safe collection or bounded termination blocks the batch
claim; it does not permit public-data workarounds, paid/CPU fallback or blind resubmission.

The [implementation plan](../implementation-plan.md) tracks unfinished code separately from
unfinished evidence. General server workers, production registration and release readiness
are not enabled by editing this ledger or successfully running the experiment.
