# Operational risk register

Keep the remaining risks and required controls visible. An implemented guard reduces a risk;
a unit test or a written policy does not establish a live-provider guarantee. Use
[current status](../status.md) for feature availability and the
[validation checklist](validation-checklist.md) for executable checks.

Ratings: **Critical** threatens private data, durable identity or duplicate compute; **High**
can make operation unsafe or misleading; **Medium** limits usability or project progress.
IDs are stable so validation failures can reference the affected invariant.

| ID | Risk | Rating | Required control and remaining evidence |
|---|---|---|---|
| R-01 | Reused/latest kernel identity returns another execution's data. | Critical | Persist attempt identity; verify kernel ID/version/source, nonce and hashes. Same-version external reruns remain unobservable; K-05/K-07 live evidence is required. |
| R-02 | Lost response, hidden retry or upsert race creates duplicate compute. | Critical | Commit intent before one permitted mutation; recover by reads, never blind resubmit. SDK upsert is not atomic create-only. Preserve ambiguous evidence. |
| R-03 | Staging is public, unready or changed. | Critical | Verify private metadata and every frozen byte before submit; recheck marker/input remotely. No public or paid-storage fallback; K-03/K-04 remain live gates. |
| R-04 | Credentials enter bundles, arguments, state or diagnostics. | Critical | Explicit references, stdin-only helper secrets, restricted environment and private files. Redaction/buffer clearing are not universal secret detection or hostile-host protection. |
| R-05 | Cancellation targets an unverified session. | High | Current batch cancellation is manual-required; kernel ID is not a session ID. Never delete as cancel or infer termination from acknowledgement. |
| R-06 | Stale/incomplete output becomes available results. | Critical | Complete listing, original manifest/pin, independent length/hash/EOF/Close and final identity checks; publish atomically. Retry collection only. |
| R-07 | Unknown/stale state clears active capacity. | Critical | Keep uncertainty and stronger confirmed attempt evidence separate; fence stale callbacks. Missing SDK fields cannot supply default success. |
| R-08 | Local timeout is mistaken for remote termination/release. | High | Keep invocation, runner and provider budgets distinct. K-09/K-10 require bounded live evidence; release stays unobservable unless proven. |
| R-09 | Missing/stale quota becomes allowance. | High | Preserve units, precision, age and reservations; no guessed resets or entitlement. Exhaustion survives unknown data; fresh reads are not capacity reservations. |
| R-10 | Managed GPU/environment changes break the workload. | High | Pin the client, record actual remote environment and verify device/computation. No CPU or package-replacement fallback; K-02/K-13. |
| R-11 | Delayed or truncated logs are represented as live/complete. | Medium | Bound snapshots and bind cursors to identity/content; label availability/truncation. Retained log artifacts and provider snapshots are different products. |
| R-12 | License labels are mistaken for upload rights. | High | Preserve operator-compatible metadata; never silently apply a permissive license. The operator remains responsible for data rights. |
| R-13 | Provider interfaces or permitted use change. | High | Review versioned official sources and affected tests; stop unsupported behavior. No browser automation, quota evasion or undocumented fallback. |
| R-14 | Archive/URL input compromises the host. | Critical | Strict path/type/expansion checks and per-hop DNS/peer/TLS policy. Optional ingestion remains explicit; do not execute admitted code locally. |
| R-15 | Host/architecture/filesystem behaves differently from CI. | High | Native state-lock/permission/process tests plus clean-host validation. Artifact delivery requires trailer preservation and create-only hard-link support. |
| R-16 | File/SQL/HTTP boundaries expose unacknowledged bytes. | Critical | Verify temporary bytes, retain durable pins and require final acknowledgement before publication. No automatic orphan reset; preserve interrupted state for recovery. |
| R-17 | Cleanup deletes active, foreign or unresolved resources. | Critical | Exact ownership ledger and current evidence, dry-run before separately authorized apply. Remote apply/staging cleanup are not shipped; partial upload resources may remain. |
| R-18 | Workspace authorization is mistaken for hostile multi-tenant isolation. | High | Private loopback service and one trusted operator; enforce current authority without claiming a workload sandbox or safe public exposure. |
| R-19 | Product work outruns proof of the first compute path. | Medium | Prioritize local/operator acceptance and remaining worker integration over new providers, sessions or dashboards. Track missing implementation separately from missing evidence. |
| R-20 | Unavailable live environment is reported as a pass. | Medium | Record not-run/blocked plus prerequisites; never substitute fixture/CI output for operator evidence or request secrets in chat. |
| R-21 | A model/example is gated, oversized or license-incompatible. | Medium | Select and pin only after environment/model-card review. Keep examples out of core and execution bounded; the LLM example is still planned. |
| R-22 | Shutdown/restore/upgrade is treated as safe redispatch. | High | Local shutdown does not stop remote work. Retain original binary/configuration, state/blobs and intent; never run original/restored copies concurrently or clear uncertainty manually. |

Design authority belongs to the [proposal](proposal.md) and [ADRs](decisions/README.md), not
a duplicate decision table. Review this register when evidence, dependency/provider versions,
public contracts or operational boundaries change. Put an actual failed check in a focused
issue/PR using the [bug report template](bug-report-template.md); do not append
development diaries here.
