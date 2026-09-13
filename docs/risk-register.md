# Risk and decision register

> **Status:** M-0 baseline.
>
> **Reviewed:** 2026-09-13

This register turns the proposal's predetermined responses into reviewable engineering
controls. A risk is not closed merely because documentation names it. Closure requires the
listed evidence or an explicit scope decision.

## Rating

- **Critical:** can create duplicate compute, expose private data/credentials, corrupt
  attempt identity, or invalidate the first-provider path.
- **High:** can make the runtime materially unsafe, misleading, or unusable.
- **Medium:** can delay or reduce usability without violating a core invariant.
- **Low:** manageable project/process risk with limited operational effect.

## Active risks

| ID | Risk | Rating | Trigger or warning sign | Predetermined response | Evidence gate / owner |
|---|---|---:|---|---|---|
| R-01 | Status/output/log APIs resolve the latest run and can return another execution's data when a slug is reused. | Critical | Shared kernel slug, manual rerun, or output without attempt manifest. | Use one persisted provider-safe slug per attempt; verify nonce/job/attempt/digests; stop on identity mismatch. | K-05/K-07, M-1 and adapter owner. |
| R-02 | A lost mutation response or hidden client retry creates duplicate compute. | Critical | Timeout/nonzero process after request may have reached provider; SDK retry policy unclear. | Persist submission intent before mutation; audit client/SDK retry behavior; reconcile deterministic identity; never automatically resubmit. | Ambiguous-submit fault test, M-1/M-3. |
| R-03 | Private staging is unavailable, processed late, or accidentally public. | Critical | Visibility cannot be observed, status is not ready, or upload metadata conflicts. | Stop before compute; never switch public; preserve rights metadata; no mandatory paid-storage fallback. | K-03/K-04 live evidence. |
| R-04 | Credentials leak into job bundles, subprocess arguments, logs, database rows, or diagnostics. | Critical | Secret canary appears outside the credential resolver boundary. | Store references only; minimal subprocess environment; redact logs; no provider credential in runner; fail secret scans. | SEC-01 tests, M-2/M-6. |
| R-05 | Cancellation is advertised without a stable supported operation and target identity. | High | CLI has no cancel command; ordinary status lacks a proven cancellable session ID. | Capability remains unknown/unsupported; persist intent; return manual-required; never delete as cancel. | K-11; M-1/M-4. |
| R-06 | Provider reports success but outputs belong to a stale run, are incomplete, or fail integrity checks. | Critical | Missing/mismatched manifest, pagination, byte count, required output, or digest. | Collect to temporary storage, enumerate all pages, verify identity/size/digest, then atomically publish; retry collection only. | K-07 plus artifact fault tests. |
| R-07 | Unknown provider states are mapped to terminal failure, freeing capacity or encouraging retry while compute may be active. | Critical | New raw status, stale observation, local deadline, or provider outage. | Preserve raw value and `unknown`/`needs_attention`; count possibly active attempts against capacity; no compute retry. | K-06/K-10 and transition tests. |
| R-08 | Timeout is treated as proof of termination or exact hardware release. | High | Local/runner/provider clocks disagree or provider observation is delayed. | Keep separate timeout, terminal, `may_be_active`, and release-evidence fields; expose gaps. | K-09/K-10 live evidence. |
| R-09 | Quota is hard-coded, missing data becomes zero/unlimited, or reset semantics are invented. | High | Account differs from expected weekly allowance; API fields missing/stale. | Store source/unit/time/precision; use known/unknown/stale/unavailable; bounded warn-and-allow or strict block; no paid fallback. | K-12 fixture/live probe. |
| R-10 | Managed environment or accelerator options change and break dependencies or GPU execution. | High | CLI/environment revision changes, selected accelerator unavailable, framework/CUDA mismatch. | Pin tested client environment; verify actual device/runtime before payload; record provenance; fail rather than CPU downgrade. | K-02/K-13 and compatibility matrix. |
| R-11 | Logs are delayed/replayed but presented as live, or unbounded logs exhaust storage. | Medium | SSE reconnect, no logs while running, duplicate events, very large payload output. | Expose source/availability; cursor/dedupe where possible; bound and mark truncation; retain runtime events separately. | K-08 live probe and log tests. |
| R-12 | Dataset license metadata silently relicenses operator data. | High | Upload succeeds only after applying an inappropriate default license. | Require operator-compatible configured label; prefer preserving ownership such as `copyright-authors`; document that Kaggle metadata is not relicensing authority. | K-03 review/live probe. |
| R-13 | Provider terms or supported interfaces change. | High | Official docs/client behavior changes or permitted use becomes unclear. | Pin/review versions and terms, update evidence/compatibility, stop unsupported behavior; no browser automation or quota evasion. | Release review and provider docs owner. |
| R-14 | Local archive or URL input compromises the runtime host. | Critical | Traversal/link/bomb archive; redirect/DNS endpoint reaches private/metadata network. | Strict archive containment/types/limits; HTTPS-only bounded ingestion; revalidate DNS/IP/redirect/connection endpoint; use upload/import for private sources. | DAT-02/DAT-03 security corpus. |
| R-15 | SQLite/locking/subprocess behavior differs across Linux, macOS, and Windows. | High | Cross-compile passes but native locking, permissions, cancellation, or process cleanup fails. | Use a CGo-free driver, one-process state lock, native CI/test evidence, and platform-specific permission/process implementations. | ADR-0003 and compatibility gates. |
| R-16 | Database/file publication crosses a crash boundary and exposes incomplete objects or artifacts. | Critical | Crash after rename but before DB commit, disk-full, or orphan temp files. | Staged temp → verify → atomic rename plus recoverable DB state; startup orphan/quarantine sweep; never publish incomplete bytes. | M-3 component/fault tests. |
| R-17 | Cleanup deletes user-owned, active, or unresolved resources. | Critical | Prefix-based discovery, missing ledger record, unknown remote state. | Ledger ownership and identity checks; dry-run default; no deletion of active/unknown resources; absent owned resource is idempotent. | K-16 and cleanup fault tests. |
| R-18 | Workspace boundaries are mistaken for hostile multi-tenant isolation. | High | Public exposure or mutually untrusted workloads share host/provider account. | Loopback/auth default; document one trusted operator; enforce resource ownership but do not claim sandbox isolation. | API auth matrix and security docs. |
| R-19 | Scope expands into sessions, dashboard, DAGs, SDK generation, or another provider before the batch path is proven. | Medium | New mandatory abstraction without M-1 evidence. | Keep deferred features outside MVP; prioritize a thin real vertical slice and failure semantics. | Roadmap/PR review. |
| R-20 | Live validation cannot run in the engineering environment. | Medium | No authorized credentials or finite quota approval. | Complete offline contracts, fixtures, probe harness, and procedures; mark `blocked-environment`; never request secrets in chat or fabricate results. | VER-03 and feasibility report. |
| R-21 | A required model/example is gated, too large, or license-incompatible. | Medium | Token/approval needed, unknown license, poor fit in verified GPU environment. | Select a small open-access pinned model after environment evidence; keep example choice outside core; use bounded prompts/output. | M-5 model-card review and live example. |
| R-22 | Provider resources survive local shutdown and users assume compute stopped. | High | Runtime exits while attempt submitted/running/unknown. | Shutdown stops new dispatch but preserves identity; no implicit cancel; restart reconciliation; explicit operator warning. | Recovery tests and operations docs. |

## Decision register

| Decision | Current answer | Authority/status | Change mechanism |
|---|---|---|---|
| Product form | Self-hosted OSS sidecar runtime with HTTP/JSON. | Owner-approved | Owner approval required. |
| Runtime language | Go; direct install primary, Docker optional. | Owner-approved | Owner approval required. |
| First provider | Kaggle, behind provider-neutral ports. | Owner-approved | Owner approval required. |
| MVP execution | Finite Python and explicit Linux shell jobs. | Owner-approved | Owner approval required. |
| Storage | SQLite metadata plus local filesystem bytes. | Approved default; ADR-0003 refines driver/tooling. | Superseding ADR within scope. |
| Provider transport | Pinned official Kaggle client environment; CLI plus narrow public-client bridge only where justified. | ADR-0001 | Superseding ADR with source/live evidence. |
| Kaggle attempt identity | One deterministic remote resource per attempt plus runner manifest; no exactly-once claim. | ADR-0002 | Superseding ADR with stronger official identity evidence. |
| Automatic compute retry | Zero. | Owner-approved | Owner approval required. |
| Provider/CPU fallback | Disabled. | Owner-approved | Owner approval required. |
| Cancellation | Capability-aware; intent is not proof; delete is never cancel. | Owner-approved/default | Capability evidence may upgrade support, not semantics. |
| Remote cleanup | Ledger-owned terminal resources; dry-run first. | Approved default | ADR if changing safety boundary. |
| Live tests | Explicit opt-in, authorized credential, finite budget, sanitized result. | Required verification policy | No exception through routine PR. |

## Review cadence

Revisit this register when:

- the pinned Kaggle client or managed environment changes;
- an M-1 live probe changes a capability conclusion;
- a public contract, persistence model, credential boundary, or retry/cancellation policy
  changes;
- a new provider is proposed; or
- a release claims a new platform or provider capability.

A closed risk remains in the register with the evidence that changed its status.
