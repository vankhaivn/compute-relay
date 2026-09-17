# Fixed GPU acceptance runbook

Run one operator-authorized experiment using the actual Kaggle components, durable state and
a separate resume process. This is not an arbitrary-job server. Use the
[validation checklist](../development/validation-checklist.md) for staged approval/expected results
and [results ledger](../development/validation-results.md) for observed evidence and failures.

## Workload and prerequisites

The fixed two-file example in `examples/jobs/gpu-smoke` uses preinstalled PyTorch to multiply
64×64 float32 CUDA tensors, synchronize device 0 and verify every element and exact total from
a frozen random challenge. Missing GPU/PyTorch, CPU fallback or wrong arithmetic fails; no package
install is attempted. `result.json` and `hardware.json` bind original job/attempt/challenge/input
and record observed Python/PyTorch/CUDA/device information.

Requested budgets are 120 seconds remote wall, 30 setup, 15 finalization, one minimum GPU and
internet disabled. These are frozen requests, not proof of actual provider timeout enforcement.
The operator supplies account eligibility, credentials, upload rights and finite effect approval.
A local GPU is not needed. Private staging/execution resources may remain afterward.

## Build once and keep the executable

```sh
go build -trimpath -o /absolute/private/kaggleacceptance ./cmd/kaggleacceptance
/absolute/private/kaggleacceptance --help
```

On Windows use `.exe` and absolute Windows paths. Install the locked client environment and
create the private non-secret JSON in [preflight](kaggle-preflight.md). The token lives only in
its selected environment variable. No mode accepts an inline secret.

The utility hashes its own executable and requires the same bytes for all later modes. Do not
use `go run`, rebuild/upgrade midway or edit the record to bypass mismatch. Keep the binary,
configuration, input/result stores, SQLite and process markers together for recovery.

## Commands

Replace paths with dedicated private locations whose parents exist. The acceptance root must
be new and must not be a normal `compute-relay init` root.

```sh
/absolute/private/kaggleacceptance prepare --root /absolute/private/acceptance-run --config /absolute/private/kaggle-preflight.json --machine-shape NvidiaTeslaT4
```

Choose P100 only deliberately. Prepare is local: it creates one fixed job, deterministic code
bundle, random immutable challenge and frozen configuration in a new directory. Existing or
partial directories are refused, not overwritten. Later modes accept no replacement source,
config or shape. It performs no provider call.

Only with private-staging and one finite-GPU-attempt approval:

```sh
/absolute/private/kaggleacceptance submit --root /absolute/private/acceptance-run --allow-private-staging --allow-gpu
```

The utility verifies free account quota with known remaining allowance of at least 120 seconds
before staging. A quota observation is not a reservation. M3 commits ownership before permitted
component effects. Once submission intent exists, it returns `resume-required` and does not
collect in that process. This is a durable boundary, not necessarily confirmed execution.
Repeated submit on that directory does not grant another mutation; never use a fresh root to
bypass an uncertain old submission.

Start a **new process** with the original executable:

```sh
/absolute/private/kaggleacceptance resume --root /absolute/private/acceptance-run --allow-read-only
```

Resume permits account/provider reads and collection, not preparation/submission. It retains the
original binding, attempt, result pin and verified cache. A committed failed transfer needs an
explicit collect operation/key:

```sh
/absolute/private/kaggleacceptance collect --root /absolute/private/acceptance-run --allow-read-only --collection-key explicit-collection-retry-001
```

Replay the same key only to recover that receipt; a new requested retry needs a deliberate new
key. There is no compute-retry option. Inspect/rehash local state without provider calls:

```sh
/absolute/private/kaggleacceptance status --root /absolute/private/acceptance-run
```

All modes accept bounded `--max-wait` (default ten minutes; one second–thirty minutes), with
at most 1,800 loop iterations. Local timeout/interrupt/SIGTERM does not stop remote compute.
Local status can open/migrate state and issue/revoke ephemeral local authority; it is not a
byte-for-byte read-only filesystem operation. It starts no server or retention sweep.

## What qualifies

Submission process evidence is create-only and flushed **after** new intent commit but **before**
returning a mutation permit. A failed marker write leaves intent committed and forbids the helper.
Do not repair missing markers from artifacts. Resume requires a different random process nonce
and the same plan/intent, plus exact terminal observation after publication. Previously valid
resume evidence is retained for later local status.

Qualification requires successful provider execution/orchestration, available atomic publication,
all six expected controls/results, independent byte hashes, exact challenge/CUDA arithmetic,
requested hardware and matching different-process records. No 202, plausible file or terminal
status alone qualifies.

| Status | Interpretation |
|---|---|
| prepared-local | Local state only; no provider acceptance. |
| resume-required | Use new-process read-only recovery; do not resubmit. |
| quota-blocked / needs-attention / collection-retry-required / restart-unverified | Required evidence missing; not a live pass. |
| passed-offline | Synthetic fixture evidence only; not promoted by the public live path. |
| passed-live | The scoped experiment qualified, not all M1 or release acceptance. |

Resume/collect exit 0 only for qualified live GPU/restart evidence. Status exit 0 means the local
read succeeded: inspect the report. Even a live pass keeps `full_m1_acceptance=false`, execution
count not observable, timeout enforcement unverified and manual cancellation/release limitations.
A normal process restart does not prove deliberately lost provider acknowledgement recovery.

## Retain state and report safely

Reports omit raw account/credential/path/log/exception data, but review them before sharing.
Actual roots and evidence stay private. Result storage/selection is bounded; the experiment uses
six files and cannot alone prove large live pagination. No cleanup apply is provided by the
adapter. Resolve activity and obtain exact-target cleanup approval separately; deletion is not
cancellation. Do not discard state while effects remain uncertain.

A database-only backup lacks blobs/process markers and cannot stop remote work. Never activate
original/restored copies concurrently. The operator controls the host and could tamper with it;
process nonces and hashes are not hostile-host attestation. SDK upsert, same-version rerun,
mount and cross-binary limitations remain in [execution](kaggle-execution.md).

Record failures using the [bug template](../development/bug-report-template.md) with source/binary
hashes, exact command, observed status and possible effects. Preserve the original failed run;
a patched binary may require a separately approved new experiment after old activity is resolved.
See [ADR-0020](../decisions/0020-explicit-durable-gpu-acceptance.md).
