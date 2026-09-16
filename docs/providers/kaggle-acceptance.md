# Fixed GPU acceptance through durable state and a separate resume process

> **Task:** M4-06 harness/integration in PR #24; in review until owner merge.
> **Requirements:** PRD-04, VER-03, preserving DUR-03 and immutable results.
> **Live status:** blocked-environment; no authenticated Kaggle/GPU run was performed.

M4-01 through M4-05 are merged. This task composes their real ports into a finite operator
experiment, not a production multi-job server. Proposal section 23.6 permits the harness and
offline qualification while live credentials are unavailable. Merging this PR does **not**
close live M4-06 or the full M1 gate. No provider credential belongs in GitHub Actions.

## Fixed experiment, not arbitrary remote code

The repository-owned example has two Python files, `main.py` and `calculation.py`, under
`examples/jobs/gpu-smoke`. The local harness packages those exact sources deterministically,
admits one fixed job and generates a 32-byte random challenge as an immutable input object.
It never executes the CUDA workload on the control host or accepts a workload/path override.

On the remote host, the example uses preinstalled PyTorch. It creates two 64-by-64 float32
CUDA tensors, multiplies them, verifies both inputs and the product are CUDA tensors,
synchronizes device 0 and checks every result element and the exact total. A challenge-derived
scale from 1 through 7 gives an expected total of `64 * 64 * 64 * scale`. These small integers
are exactly representable; no floating tolerance or CPU fallback is accepted. Missing PyTorch,
missing GPU, CPU tensors or wrong arithmetic fail the experiment; no package install occurs.

Two bounded outputs, `result.json` and `hardware.json`, record original job/attempt/challenge,
input digest, calculation, device and actual Python/PyTorch/CUDA versions. The harness verifies
those claims against the immutable input and requested T4/P100 shape, after M3 verifies the
runner manifest and published bytes. Device listing alone does not qualify.

The fixed request uses a **120-second remote wall budget, 30-second setup budget and 15-second
finalization grace**, one minimum GPU and remote internet disabled. These are requested/frozen
budgets, not proof of provider-side timeout enforcement. The one-minute execution helper,
ten-minute collection default and operator invocation budget are separate local limits.
No custom images, models, interactive sessions, paid-capacity fallback or dynamic code are added.

## Build once and retain the exact executable

With the repository's pinned Go toolchain and locked dependencies, from the repository root:

```text
go build -trimpath -o kaggleacceptance ./cmd/kaggleacceptance
./kaggleacceptance --help
```

On Windows build `kaggleacceptance.exe` and invoke it with `./kaggleacceptance.exe` or the
PowerShell equivalent. Keep the binary private/outside source control. The command hashes its
own executable, with a 256 MiB defensive ceiling and context-aware reads. Every later invocation
must use the same executable bytes; ordinary `go run` temporary binaries are not the prescribed
multi-step procedure. Do not rebuild or upgrade midway and edit the record to bypass mismatch.

Install the existing pinned Python client environment using the [preflight guide](kaggle-preflight.md).
Create its non-secret JSON configuration with the actual canonical account, explicit `env:NAME`
reference and absolute Python executable. The remote PyTorch version is not installed or pinned
by this client environment; the experiment records the version actually observed in its result.

The paths below are illustrative. Replace them with dedicated private operator paths whose
parents already exist. Provision the selected token securely outside chat, source control and
command arguments. No command accepts a token value as a flag.

## Modes and explicit authorization

Local preparation creates a **new** private directory, SQLite state, input/result stores and
one admitted job. It resolves no provider credential and constructs no provider:

```text
./kaggleacceptance prepare --root /absolute/private/acceptance-run --config ./kaggle-preflight.json --machine-shape NvidiaTeslaT4
```

Choose `NvidiaTeslaP100` instead only when deliberately testing that shape. Existing directories
are rejected, including partially prepared state. There is no overwrite/reset/reinitialize flag.
Configuration and workload are frozen in `acceptance.json`; later modes cannot supply replacement
config, shape or source. Even cross-mode flags explicitly set to false are rejected by the CLI.

Only after the operator approves private synthetic staging and one bounded GPU attempt:

```text
./kaggleacceptance submit --root /absolute/private/acceptance-run --allow-private-staging --allow-gpu
```

Both flags are required. The command prints the fixed budget before starting. It verifies
account/free quota and requires a known remaining allowance of at least 120 seconds before
staging. This observation is not a reservation; subsequent provider checks and rejection handling
remain authoritative. Missing or exhausted quota stops before mutation.

The same M3 dispatch engine commits preparation/submission ownership before the real component
methods run. Once the submission intent is present, this invocation exits with `resume-required`.
It does not collect results in the submitting process. Repeating submit against that directory
returns the same handoff without constructing another provider or granting another submit permit.

Start a **new process** for read-only observation and collection:

```text
./kaggleacceptance resume --root /absolute/private/acceptance-run --allow-read-only
```

The read-only flag authorizes account/provider reads and artifact transfer, not provider mutations.
Both the repository wrapper and adapter disable new preparation/submission. Resume observes the
original identity, drives existing recovery and collects through M3 leases/pins/publication. It
never retries compute or chooses another account/profile/attempt.

A committed failed transfer requires an explicit collection operation/key for the same attempt:

```text
./kaggleacceptance collect --root /absolute/private/acceptance-run --allow-read-only --collection-key explicit-collection-retry-001
```

Reusing a key replays its original receipt; a genuinely new requested transfer retry needs a new
key. There is no compute-retry option in this utility. Existing accepted collection interruption
can resume after its lease; failed collection is not silently replaced.

Inspect retained state and rehash published results without contacting the provider:

```text
./kaggleacceptance status --root /absolute/private/acceptance-run
```

Local-only does not mean a byte-for-byte read-only filesystem: opening state can perform existing
migrations and each invocation issues/revokes ephemeral local application authority. The utility
stores no local token secret in the experiment record. It does not sweep retention or start a server.

All modes accept `--max-wait`, default ten minutes, bounded from one second to thirty minutes.
The control loop also stops after 1,800 iterations. A local timeout/interrupt/SIGTERM **does not
cancel remote work**. Preserve the directory and use the original recovery path. Do not turn an
uncertain outcome into a fresh experiment merely to get another submission opportunity.

## Durable journal and process boundary

`Store.InspectDispatch` is a new internal **operate-authorized read**, not a public status route.
It checks current authority, active attempt, installation identity, original frozen inputs and
the independent ownership ledger. It returns private journal data, no claim/fence/mutation permit,
and does not call the provider or reset a gate. Do not serialize it directly for clients.

The harness writes `submission-process.json` only **after** a successful new M3 submission-intent
commit and **before** returning the permit to the submitting caller. The record is create-only
and flushed. An I/O failure at that boundary leaves the intent committed but forbids the helper
call. Missing/partial records require investigation; no automatic record repair or reset exists.

A cryptographically random process nonce is stable within one process and new after exec.
Resume requires a different nonce and the same plan/intent. After publication, it also requires
an exact terminal observation in the resumed process before recording `resume-process.json`.
A previously valid resume record is retained across subsequent local status reads. Record fields,
ordering and original digests must agree; result bytes alone cannot replace restart evidence.

This establishes a cooperative local process boundary, not hostile-host attestation. The operator
owns the binary, environment, state and result files and can tamper with them. Same-version provider
reruns remain unobservable as described in [execution](kaggle-execution.md). A process nonce does
not prove that every crash window or remote exactly-once property has been tested.

## Reports and qualification

Reports exclude actual account names, token/reference values, interpreter paths, raw logs and
provider exception bodies. They contain opaque job/attempt identity, independent current states,
checked artifact paths/lengths/digests, GPU/restart checks and explicit evidence limits. Review
reports before sharing; the directory contains private account configuration and retained bytes.

Qualification requires successful provider execution/orchestration, available atomic publication,
all **six** expected result/control files, independent local byte hashes, exact challenge/arithmetic
and hardware checks, and consistent different-process submission/resume records. No success is
inferred from a `202`, kernel terminal status, plausible output prefix or missing field.

| Report / exit | Meaning |
|---|---|
| `prepared-local`; prepare exit 0 | Local job/state exists; no provider acceptance. |
| `resume-required`; submit exit 0 | A durable submission boundary exists; use a new process, not another submission. |
| `quota-blocked`, `needs-attention`, `collection-retry-required`, `restart-unverified` | Required evidence is missing; not a live pass. |
| `passed-offline` | Test-only synthetic result. The public live path rejects fixture records; the CLI never upgrades this to a live acceptance success. |
| `passed-live` | The explicitly authorized fixed experiment's retained GPU/result/restart checks qualified. Not full M1 or release acceptance. |
| status exit 0 | The local read succeeded; inspect `status` and `evidence` rather than treating exit 0 as GPU success. |

Resume/collect return exit 0 only for a qualified live report with both GPU and restart verified.
Errors use constant sanitized diagnostics and nonzero exit; raw internal errors are not echoed.
Even a qualified report keeps `full_m1_acceptance=false`, `remote_execution_count=not-observable`,
`provider_timeout_enforcement=unverified`, manual-required cancellation and the original release
evidence. Historical local verification is not a fresh provider-account compatibility check.

## Storage, recovery and remaining side effects

The experiment directory contains the original record, SQLite, input/result stores and process
records. Preserve all of them plus the original binary/configuration. A database-only snapshot
cannot restore deleted blobs or later process markers and does not stop remote compute. Do not
run original and restored copies concurrently or alter ownership/marker rows to bypass a block.
Input blobs are capped at 2 MiB each/4 MiB total, result blobs at 24 MiB each/64 MiB total, and
acceptance artifact selection at six files/64 MiB. Existing component transport bounds still apply.

No cleanup implementation is supplied by the acceptance adapter: it explicitly returns unavailable
rather than claiming dry-run success or applying deletion. Staging/kernel resources and partial
upload side effects may remain on the operator account. Cleanup must be separately reviewed and
authorized from exact ownership evidence; deleting a resource is not cancellation. Do not discard
local recovery state until the operator has resolved remote activity and resource retention.

The private SDK seam, supported storage host, upsert race, dataset-mount, optional capability and
cross-binary limitations of M4-01 through M4-05 remain. The scoped experimental Provider composes
those ports for one admitted fixed job; it is not general production registration, `serve`,
public artifact/log/quota routes, an automatic live doctor or M5 product completion.

## Offline verification and current live ledger

```text
go test -race ./internal/kaggleacceptance ./cmd/kaggleacceptance ./internal/provider/kaggle ./internal/store/sqlite
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

The existing locked client suite runs `uv run --locked python -m unittest discover -s tests -v`
inside `tools/kaggle-client`. CUDA arithmetic tests use synthetic tensor objects and import no
torch. Native CI builds/tests the actual pinned Go/modernc and Python client stacks; no new live
workflow, credential, GPU or dependency is introduced.

Real SQLite/admission/blob/collection tests use a synthetic remote backend and injected process
identities. They cover original admission, no-provider local modes, submission/response loss,
same-process refusal, one-submission recovery, current journal authorization, late transfer retry,
marker failure after committed intent, invalid restart records and fixture/live separation.
Actual child processes separately verify nonce freshness and run the real CLI's local prepare
and status against durable state. **Those child tests do not execute the live GPU workflow.**
Helper-only Go tests intentionally skip in ordinary suite invocation; their nominated parent tests
run them in children. No new process-kill-at-every-boundary claim is made.

Local engineering used Go 1.23.2 for formatting and Python 3.13.5 for three synthetic calculation/
input roots. The Python source and tests were verified against exact Git blob hashes before that
run. This is narrow local evidence, not full local pinned Go/modernc/SDK integration or real CUDA.
Exact-head CI and the changes resumed from the five existing commits are recorded in PR #24.

**Live ledger for this implementation: not run / blocked-environment.** There is no sanitized
real account/GPU/restart report in this PR. An operator-run pass can be recorded later with its
binary, configuration/client versions and scoped report, but does not by itself prove the full
M1 timeout/cancellation/fault/cleanup checklist or an unrestricted production go decision.

## Primary API references

Reviewed 2026-09-16: PyTorch documents [matrix multiplication](https://docs.pytorch.org/docs/2.13/generated/torch.mm.html),
[CUDA tensor location](https://docs.pytorch.org/docs/2.13/generated/torch.Tensor.is_cuda.html) and
[device synchronization](https://docs.pytorch.org/docs/2.13/generated/torch.cuda.synchronize.html).
These references explain the used operations, not the installed remote version or measured GPU
execution. Existing version-pinned Kaggle SDK sources remain linked from the component guides.

See [ADR-0020](../decisions/0020-explicit-durable-gpu-acceptance.md),
[artifacts](kaggle-artifacts.md), [storage](../storage.md) and the
[implementation plan](../implementation-plan.md). Stop after PR #24 for owner review/merge;
M5 and live provider acceptance have not started in this implementation session.
