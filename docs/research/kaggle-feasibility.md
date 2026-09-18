# Kaggle feasibility and support boundaries

This document preserves the stable **K-01–K-16** questions from the proposal. Upstream source
review at **2026-09-13** used CLI 2.2.4; current component references also pin SDK 0.1.35.
The [interface review](kaggle-interface-review.md) links the exact sources. Current shipped
behavior and limitations belong in [current status](../status.md); this page is not a run diary.

The fixed authorized acceptance path has live-qualified private staging, actual Tesla T4 CUDA
execution, separate-process reconciliation, terminal observation, bounded logs and complete
artifact collection/publication for its tested account and workload. That does not imply universal
quota, timeout, cancellation, release, cleanup or arbitrary-workload support.

| Gate | Question | Current project conclusion / remaining limit |
|---|---|---|
| K-01 | Does the configured credential identify the expected account? | Qualified for the fixed account path with explicit token identity; credentials remain operator-owned and account-specific. |
| K-02 | Can that account run the selected GPU? | Qualified for the fixed Tesla T4 CUDA arithmetic workload; no CPU fallback or universal accelerator guarantee. |
| K-03 | Do multi-file code and private inputs arrive intact? | Qualified for private staged bundle/input bytes with frozen hashes and test-owned identity. |
| K-04 | Is staging ready before compute? | Qualified for the fixed path with separately observed readiness before submission. |
| K-05 | Is execution bound to the intended attempt? | Qualified for the fixed path through exact kernel ID/source/nonce checks and separate-process reconciliation; remote exactly-once and same-version external reruns remain unobservable. |
| K-06 | Are observed raw states interpreted truthfully? | Terminal COMPLETE behavior is qualified; unknown/new provider values still fail closed by contract. |
| K-07 | Are correct-attempt results retrieved? | Qualified for complete six-file selected output publication with exact hashes; large live pagination is not claimed. |
| K-08 | What logs are available and when? | Bounded provider/log and retained control-log behavior is available for the fixed path; no live SSE product is claimed. |
| K-09 | Is the requested remote timeout enforced? | Not established as a provider guarantee; local/runner/provider budgets remain distinct. |
| K-10 | What termination/release evidence exists? | Terminal execution can be observed; exact hardware/accounting release remains unobservable. |
| K-11 | Is a cancellable session target verifiable? | No; current batch cancellation remains manual-required without a verified session target. |
| K-12 | What quota, age and precision are available? | Authorized quota observations are supported; they are not reservations or universal entitlement/reset guarantees. |
| K-13 | Which environment actually ran? | The fixed live path records Python/PyTorch/CUDA/device provenance and verified Tesla T4 arithmetic. |
| K-14 | Does staged execution work with internet disabled? | Qualified for the fixed no-download workload with provider internet disabled; the runner itself is not a firewall. |
| K-15 | Does restart preserve the original execution? | Qualified for the fixed path through a distinct-process resume/reconcile/collect flow. Induced lost-acknowledgement faults remain a separate question. |
| K-16 | Can cleanup remove only owned inactive resources? | Remote cleanup apply and staging cleanup are not shipped; exact-target manual cleanup requires separate authorization. |

## Support decision

The fixed Kaggle path is supported only within the boundaries above and the provider component
guides. General server workers, arbitrary-job provider registration and release readiness remain
separate product work. Missing optional capabilities stay explicitly unsupported rather than being
replaced by browser automation, hidden retries, CPU/paid fallback or unsafe cleanup.

Use the [validation checklist](../development/validation-checklist.md) when a fresh host/account or
provider/toolchain change needs re-qualification. Keep complete run records private; if a result
changes one of these conclusions, attach only the minimal sanitized evidence to a focused issue/PR
and update the affected current-purpose documentation.
