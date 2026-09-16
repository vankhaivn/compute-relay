# ADR-0017: One-shot Kaggle execution with exact-source observation

- Status: proposed; implemented offline in PR #21, pending owner review/merge
- Date: 2026-09-16
- Task: M4-03
- Requirements: PRV-02, DUR-03, DOM-02/03
- Extends: ADR-0001/0002, ADR-0011 and ADR-0015/0016

## Context

Private staging is merged in PR #20. M3 already owns durable submission intent, resource
ownership, fencing and recovery. Kaggle's public SDK provides SaveKernel/GetKernel and
version-scoped status, but saving is an upsert; a local process/HTTP failure does not prove
non-acceptance. SDK defaults can also make absent status or resource fields look meaningful.
M1 live acceptance remains unverified; this task is offline implementation, not quota use.

## Decision

Provide an explicitly composed, immutable **per-attempt Executor**, not a second scheduler,
new journal or incomplete production Provider. Clone the original validated plan and exact
private ready staging reference. Only the caller receiving a successful NEW M3 BeginSubmission
commit may invoke Submit. Retain the durable gate across restart; an in-memory atomic latch
is only additional duplicate-call protection, never a replacement for the journal.

Derive a stable kernel slug from prewritten attempt/submission identity, excluding mutable
content. Package the existing five Python runner modules as inert, length/digest-checked source
against the unchanged runner lock. Embed only those named files, not local caches. Build a
strict resolved runner manifest from original input/command/resource/network/time/output fields,
and include original staging/attempt identity and effective GPU shape in the source digest.
Never execute this generated workload source on the control-plane host.

Recheck the complete private staging snapshot before kernel submission. Use the original
explicit credential reference/account and a fixed isolated SDK helper. Only actual HTTP 404
can lead to a freshly authorized save; observing an exact existing resource leads to reads,
not another save. Allow one SaveKernel wire request, private script and explicit finite policy.
Unknown outcomes after a possible save remain non-retryable submission uncertainty. No fallback,
new version, explicit update, deletion, interactive session or arbitrary endpoint is exposed.

Require exact raw owner/slug/numeric ID, private visibility, version 1, resource flags, data
sources and complete source digest. Read explicit version 1, query version-scoped status and
recheck current metadata afterward. Reject replacements rather than adopting a new identity.
Inspect raw fields before SDK defaults; absent/new/invalid status is unknown. Map only queued,
running, complete and error to established execution states. Cancellation request/acknowledgement
and new-script states do not prove termination. Always keep hardware release not observable.

M3 records fresh raw observations and separately preserves stronger confirmed attempt evidence.
Terminal provider state opens collection, not final business success or verified artifacts.
The remote-only bootstrap checks the staging marker before loading the locked runner, requires
a new result directory, and restores signal handlers. Output selection/publication remains
M3-06/M4-05 work, not an implicit download of arbitrary remote files.

Use the same reviewed version-bound private SDK session seam as the preflight/stager. Exact
RPC allowlisting, one armed send, no retries/redirects/proxies, verified TLS, bounded JSON and
stdio, parent context and independent watchdog constrain the leaf helper. Source and tokens
travel over stdin, not arguments/files. Interpreter/packages/operator/callbacks remain trusted;
this is not hostile-host attestation, universal erasure or an arbitrary-process supervisor.

## Provider and recovery limits

SaveKernel has no exposed atomic create-only/CAS guarantee. Another actor can race the absence
read and cause an upsert; post-save version/identity checks detect uncertainty but cannot undo
a remote effect. This design does not promise exactly-once remote execution. Likewise a manual
rerun of the same version with unchanged observable identity is not proven absent. Exclusive
attempt resources and separate live identity evidence remain necessary.

Dataset attachment uses owner/slug, not a claimed immutable mount. Full pre-submit verification
and remote marker/input hashes detect content changes without claiming an atomic provider lock.
Explicit CPU/GPU/internet and requested session budgets are bounded policy, not proof of actual
GPU allocation, managed timeout enforcement or hardware release. A false pay-to-scale observation
is a free-only check, not a reservation; missing/true values prevent new GPU save. Numerical
quota/capability mapping remains separate.

Source is reconstructed from original plan/preparation and immutable configuration/policy plus
this binary's bootstrap/runner. A binary/policy change can make old source unverifiable; fail
closed and preserve evidence instead of silently adopting it or resubmitting. No pre-submit
source-blob archive is introduced. Keep original compatible binaries/configuration for recovery;
transparent cross-version recovery is not established.

## Alternatives rejected

- Retry SaveKernel after timeout/nonzero exit: can create another version/execution.
- Change slug when source/policy changes: evades original intent and uncertainty.
- Trust current profile, title/prefix or SDK default values: weakens identity and truthfulness.
- Treat cancellation acknowledgement or local deadline as termination: invents evidence.
- Embed all local runner directory contents: captures caches or unrelated build files.
- Invent a production Provider with unsupported methods: hides remaining M4 integration gates.
- Call offline SDK fixtures live acceptance: misstates provider behavior and authorization.

## Verification

Unit tests cover source determinism/strictness, exact embedded assets, concurrent invocation,
ambiguous outcomes, raw-state mappings, local process isolation and valid runner contracts.
Real SQLite/admission/blob/dispatch tests prove intent/ownership visible before helper entry
and no additional submission across lost acknowledgements, restart, remapping, stale polls
or replaced resources. SDK tests use actual pinned request/response types with mocked HTTP.
Bootstrap wiring tests use inert modules, not admitted workload execution.

The [execution guide](../providers/kaggle-execution.md) records exact primary sources, budgets,
composition and evidence limitations. PR #21 records final-head offline CI and narrow local
checks separately. No public contract, dependency pin, original Python asset/lock or migration
changes. Owner review/merge only; M4-04, output retrieval, full runtime and live acceptance retain
their own gates.
