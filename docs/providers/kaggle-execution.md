# Kaggle one-shot execution and exact-version observation

> **Task:** M4-03, offline component implementation in PR #21; in review until owner merge.
> **Requirements:** PRV-02, DUR-03, DOM-02/03.
> **Evidence:** actual pinned SDK with mocked HTTP, real SQLite/blob/dispatch integration
> with synthetic helper results, and isolated process tests. No live credentials or GPU.

M4-02 private staging is merged in PR #20. This task adds attempt-scoped submission,
observation and read-only ambiguity recovery. It preserves M3's durable mutation authority,
not a second queue or an automatic retry policy. Proposal section 23.6 permits this offline
work; M1 live acceptance/M1-08 go and complete integrated batch acceptance remain separate.

## Composition and authority

`kaggle.NewExecutor(stager, policy, plan, prepared, allowSubmit)` constructs one executor for
one immutable attempt. Supply the original validated `provider.Plan`, exact persisted private
ready `provider.Prepared`, and original account/configuration and execution policy. Constructor
work is local, deterministic source construction; it does not resolve a credential or call a
provider. The supplied `Stager` retains the M4-01 configuration and M4-02 verification rules.

The component implements the existing Submit, Observe and ReconcileSubmission method shapes,
not the entire `provider.Provider` interface. Production registration, capability/log/quota/
cancellation mapping, output retrieval and installed runtime composition remain separate.
Tests wrap it in a **test-only** fake capability surface; no production fallback is introduced.

**Only the invocation that just received a successful NEW M3 `BeginSubmission` commit may
call `Submit`.** The operator enablement flag is not that permit. The atomic in-process latch
prevents repeated/concurrent calls on an executor but is not durable idempotency. Rebuilding
an executor must never be used to rearm a journaled submission. After restart, load the original
plan/preparation and call read-only reconciliation or observation according to the journal.

M3 commits submission intent, execution ownership, state and sequenced event before helper
entry. Failed/uncertain acknowledgement never grants a mutation permit. A genuine not-found
read during recovery does not prove non-acceptance and cannot justify another save. Local
transactions never span the SDK or staging byte reads. Existing migrations, ownership records,
fencing and original job/control receipts are unchanged.

## Frozen source and identity

Kernel slugs derive from installation/workspace/instance/job/attempt/intent/resource-key/nonce
identity, not from mutable payload bytes. The `cre-` name uses a deterministic digest. Changing
contents or policy under the same intent must conflict with that identity, not select a fresh
resource to evade an uncertain outcome.

Source construction verifies the exact private version-1 staging reference, including numeric
dataset ID, owner/slug and marker digest. The resolved job is parsed with existing strict
admission validation and compared with frozen bundle/input IDs, names, targets and wall budget.
Source URLs are not refreshed or transmitted; all original inputs must already be objects.
CPU requests cannot silently acquire a GPU, and required internet must be allowed explicitly.

`runnerassets.Sources()` embeds only the five named Python modules and existing lock. Each
module's length/SHA-256 must match its unique lock entry. Bytecode caches and unrelated local
files are not embedded. The original Python modules, schema, tests and lock are unchanged.

The generated script includes the locked runner, original execution/resource/network/output/
timeout fields, frozen file digests, attempt nonce, plan/staging identity and effective GPU
policy. It excludes profile labels, human job name, credential references, provider tokens,
source URLs and local interpreter paths. The command and explicitly supplied job environment
are workload data; this packaging boundary is not a universal secret scanner.

The complete source is hashed. Remote references persist numeric kernel ID, owner/slug,
source SHA-256 and version `1` alongside the existing provider-neutral identity. Subsequent
reads cannot substitute another ID/source/version. A private title or a name prefix alone
is not ownership evidence.

## Submit and uncertainty

Before any kernel helper invocation, Submit re-observes the **entire** M4-02 staging resource
and compares the ready/private result with the original Prepared value. Changed numeric ID,
marker, version, visibility, unavailable bytes or incomplete readiness stop the kernel path.
This recheck creates no dataset and does not refresh input sources.

The fixed execution helper rechecks exact local Python/client pins, receives the token and
request over stdin, and verifies the active server account. It looks up only the exact kernel
owner/slug. A genuinely absent kernel (HTTP 404, not a JSON error string) can enter a NEW
journal-authorized save path. An existing exact source is observed instead of updated; an
existing mismatch fails closed.

A new call uses the official SDK's `SaveKernel` with private Python script, finite requested
session timeout, SAVE_AND_RUN_ALL, the selected staging dataset and explicit CPU/GPU/internet
settings. TPU, additional notebook/model/competition sources, custom images, interactive work,
priority and automatic fallback are not exposed. Only one save wire request can leave a helper.

After the save, acceptance requires a usable version-1 receipt, positive numeric kernel ID,
no reported invalid sources, exact source/private metadata readback, and a final matching current
version. A timeout, nonzero helper exit, invalid receipt, HTTP error or lost acknowledgement
after the save may mean compute started. The result is `submission_unknown`, never a blanket
safe-to-retry error. Proven pre-save failures can be rejected without inventing remote execution.

The SDK operation is an **upsert, not an atomic create-only/CAS API**. A pre-save absence read
cannot exclude an external actor creating the same name before SaveKernel. The component
never knowingly updates an observed resource and rejects unexpected version/identity after
the call, but does not claim a remote exactly-once guarantee or immunity to that race. Keep
attempt resources exclusively operator-owned; investigate uncertainty instead of resubmitting.

## Observation and state mapping

Recovery sends the original name/source digest, not another copy of private source or a save.
Observation additionally requires the first-pinned numeric kernel ID. Reads validate raw
metadata fields before the SDK can supply defaults, retrieve explicit version `1`, query its
version-scoped session status, then re-read current metadata. A newer version, changed source,
public visibility, changed account, additional data source or replacement numeric ID is rejected.

| Raw provider status | Execution / activity reported by this component |
|---|---|
| QUEUED | queued / possible |
| RUNNING | running / active |
| COMPLETE | succeeded / inactive |
| ERROR | failed / inactive |
| CANCEL_REQUESTED, CANCEL_ACKNOWLEDGED, NEW_SCRIPT | unknown / possible |
| Missing, null, invalid, new status, or failed status request | unknown / possible |

The SDK constructor defaults a missing status to QUEUED. This component deliberately checks
raw field presence and recognized value types first; absent evidence is not a queued fact.
Required private/resource flags likewise cannot be inferred from omitted SDK fields. A provider
response omitting those fields fails closed rather than receiving a compatibility exception.

Every observation reports release evidence `not_observable`; cancellation acknowledgements
are not proven termination, and local elapsed time is not a remote timeout. COMPLETE opens
M3's collection gate, not verified artifact availability or final business success. The runner's
result manifest and every required artifact still need M3-06/M4-05 verification.

The journal can record the latest UNKNOWN observation while the **independent persisted
attempt state retains stronger previously confirmed running/activity evidence**. Tests check
both facts, not just the journal snapshot. Fenced M3 updates reject a stale/late nonterminal
publisher after terminal evidence. Unknown observations cannot clear account capacity merely
because a lease expired.

`ObservedAt` is local UTC time after the bounded identity/status reads, not a server-provided
execution timestamp. Numeric kernel ID/version/source are not a cryptographic session identity.
An external rerun of the same version without an observable identity change is not ruled out.
New-version/source/ID changes are detected; full live identity evidence remains a separate gate.

## Remote-only bootstrap and output handoff

The control plane constructs source as inert data and never runs an admitted command. On the
remote Linux host the fixed bootstrap checks `/kaggle/input/<staging-slug>/relay-stage.bin`,
rejects missing/symlink/oversized/changed marker evidence, then loads only the five locked
runner modules. It invokes the original finite runner and restores signal handlers on return
or error. A second bootstrap invocation in the same process is rejected.

The runner requires a new `/kaggle/working/relay-result` directory. Its existing code verifies
bundle/input bytes and executes only the remote command under the frozen budgets. Control
manifests/logs and declared outputs live under that result prefix. A non-completed runner
phase fails the wrapper instead of inventing success.

The SDK attachment here names the dataset owner/slug. No immutable provider dataset-version
mount guarantee is claimed. Pre-submit full staging verification and the remote marker/byte
checks detect changed content, but are not an atomic provider lock. Code/input/scratch data
may also exist in the remote result tree; future output retrieval must select only the agreed
control/output namespaces. M4-03 does not implement artifact download, expose arbitrary remote
files or claim that provider persistence has been cleaned up.

## Policy, credentials and bounds

Default execution policy is CPU-only, internet disallowed, maximum wall budget 1,800 seconds
and a one-minute invocation context. Configured wall ceilings are 1–86,400 seconds and invocation
budgets one second to five minutes. GPU requires explicit NvidiaTeslaT4 or NvidiaTeslaP100 policy;
a CPU job with a GPU shape or GPU job without one is rejected, never silently upgraded/fallen back.
These labels/policies require live verification before a support claim.

For a new GPU save, the helper requires an explicitly false raw pay-to-scale flag in quota data.
True/missing/unknown stops before saving. This is a conservative free-only eligibility check,
not numeric quota scheduling, an atomic reservation or a guarantee that an account setting cannot
change afterward. Numerical quota precision/freshness and full optional capabilities are M4-04.

Source is bounded to 2 MiB, encoded manifest/module payload to 1 MiB, helper request/metadata
responses to 3 MiB, token to 8 KiB and each captured stdout/stderr stream to 16 KiB. The execution
helper allows at most ten exact RPC calls, one armed wire call per SDK method, with 5/10-second
connect/read timeouts, verified TLS, no ambient proxies, no redirects and no retries. The
separate full staging recheck shares Submit's invocation context and retains M4-02's own bounds.
Large inputs can exceed that budget and safely stop before save; no throughput promise is made.

The helper inherits no Kaggle token/proxy/Python-path environment, runs isolated in an empty
temporary home/cwd, and receives private source/token through stdin, not arguments or files.
It has a parent deadline and independent watchdog bounded by the remaining invocation budget.
A nonzero exit after writing a plausible report is still failure. Raw exception/output is not
reflected to the application. This fixed leaf process is not a general workload supervisor;
trusted sources/resolvers and OS operations must cooperate with cancellation.

## Recovery across configuration or binary changes

Reconstruct from original M3 plan/preparation, frozen account binding and execution policy.
Do not follow a remapped current profile. The generated source also depends on this binary's
bootstrap and locked runner assets. Changing those can make an older execution's source fail
comparison; rejection/manual investigation is safer than adoption or a new save.

No independent durable source-blob/archive is introduced before submission in this component.
Preserve the original binary/configuration when planning recovery of in-flight work. Transparent
cross-version recovery is not claimed. Retain intent/remote evidence and use the original
compatible implementation to inspect it; never erase a journal or generate a replacement slug.

## Verification and evidence tiers

```text
go test -race ./internal/provider/kaggle ./runner
go test -count=3 -run='TestExecution|TestExecutor' ./internal/provider/kaggle
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Inside `tools/kaggle-client`, use `uv run --locked python -m unittest discover -s tests -v`.
The existing workflows also run the original offline runner suite. No new workflow, dependency,
public schema or migration is added by M4-03.

Go tests cover deterministic source, asset hashes, strict plan/identity checks, concurrent
one-shot calls, missing/replaced staging, credential order, ambiguity, exact observations,
process isolation/deadlines and generated-manifest validation with the real Python contract.
Real SQLite/blob/dispatch tests independently read committed intent/ownership before simulated
helper entry and exercise lost intent/save/outcome acknowledgements, restart/remapping, missing
recovery, stale/replacement observations and collection-only handoff. These helper results are
synthetic, not real SDK calls.

Python tests separately use the actual pinned SDK with HTTPAdapter replaced by fixtures,
covering private save/readback, all raw status variants, omitted SDK defaults, wrong account,
source/version/ID/privacy changes, late/error receipts, pay-to-scale refusal and no resave.
Transport/protocol tests bound output, redirect bodies and watchdog lifetime. Bootstrap tests
use inert fixture modules and temporary path mapping for marker-before-entry/import/signal
wiring; they do not execute an admitted workload or claim a real remote mount.

Local Linux has Go 1.23.2 and Python 3.13.5, not the pinned SDK/Go dependency stack. Three bootstrap
unit roots passed locally against a byte-for-byte Git-blob-verified bootstrap; isolated gofmt
checks verified fixture alignment. This is narrow local evidence, not full local modernc,
SDK, generated kernel execution or end-to-end smoke. Full Go 1.27.1/native/race, locked Python
3.11.16/Kaggle 2.2.4/SDK 0.1.35 and runner-suite outcomes belong to exact-head CI in PR #21.

## Reviewed primary sources

Reviewed 2026-09-16 at existing pins; source/fixture findings are not live compatibility:

- [Official CLI kernel operations](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/kernels.md).
- [SDK kernel service methods](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py).
- [Get/Save/status request and response types](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py).
- [Execution and worker status enums](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_enums.py).
- [SDK default-value decoding](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_object.py) and [transport](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_http_client.py).

See [ADR-0017](../decisions/0017-one-shot-kaggle-execution.md), [staging](kaggle-staging.md),
[dispatch](../dispatch.md), [recovery](../recovery.md), and the
[implementation plan](../implementation-plan.md). Stop after PR #21 for owner review/merge.
M4-04/05, full Provider/runtime composition, live provider acceptance and remote cleanup are
not started or declared complete by this task.
