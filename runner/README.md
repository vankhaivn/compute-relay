# Finite remote runner

The runner executes one resolved attempt on **Linux with Python 3.11 or newer**, writes
bounded results and exits. It is not a local compute provider, job queue or admission handler.
The implementation uses the Python standard library; GPU checks require PyTorch already
present in the remote environment. It does not install PyTorch or fall back to CPU.

## Entry points and files

From the repository root, contract validation does not execute the workload:

```text
python runner/python/run.py --manifest runner/examples/request.valid.json --validate-only
```

The example contains illustrative staged identities, not runnable matching bundle/input bytes.
An adapter or explicitly authorized disposable fixture host can execute a prepared attempt:

```text
python runner/python/run.py --manifest resolved-attempt.json --execute --staging-root staged --work-root attempt-new --network-mode disabled
```

**`--execute` runs the manifest's commands on the invoking host. Never use it to validate
untrusted jobs on the control plane.** The work root must be new and must not overlap staging.
There is no overwrite, resume or automatic retry of a payload.

| File | Responsibility |
|---|---|
| `python/run.py` | Explicit validation/execution entry point. |
| `python/relay_runner/contract.py` | Strict manifest decoding and semantic limits. |
| `python/relay_runner/files.py` | Frozen-input verification, safe bundle extraction and output hashing. |
| `python/relay_runner/process.py` | Process-group supervision and bounded logs. |
| `python/relay_runner/deadline.py` | Immutable deadline checks against UTC and Linux `CLOCK_BOOTTIME`. |
| `python/relay_runner/main.py` | Preparation, setup, resource checks, execution and finalization. |
| `manifest.schema.json` / `examples/` | Adapter-to-runner contract `compute-relay/runner/v1`. |
| `assets.lock.json` / `assets.go` | Reviewed source hashes and inert Go embedding of the seven modules. |
| `check.py` / `tests/` | Offline checks and repository-owned CPU fixtures. |

The [Kaggle execution component](../docs/development/providers/kaggle-execution.md) constructs the remote
script from the locked modules and frozen plan. Packaging those modules is not local execution.
The runner receives staged relative paths, job/attempt/nonce and digests, not provider tokens,
source URLs, account configuration or callback addresses.

## Frozen manifest and environment

The bundle declares staged path, size and SHA-256. Inputs additionally declare logical names
and targets. `input_manifest_sha256` hashes compact ASCII JSON input records containing only
`name`, `target`, `bytes`, `sha256`, sorted by target with sorted keys and no trailing newline.
Physical provider paths do not affect that identity. Each copied file is verified independently.
The manifest is limited to 1 MiB; duplicate keys, unknown fields, unsafe paths, case/prefix
collisions, reserved environment names and invalid budget arithmetic are rejected.

The attempt layout is:

```text
attempt-new/
  code/       validated bundle contents
  inputs/     verified input snapshots
  outputs/    declared results
  scratch/    temporary files, HOME and optional venv
  control/    manifest, provenance and bounded logs
```

Applications receive `CC_JOB_ID`, `CC_ATTEMPT_ID`, `CC_CODE_DIR`, `CC_INPUT_DIR`,
`CC_OUTPUT_DIR`, `CC_SCRATCH_DIR` and `CC_EXECUTION_MANIFEST_PATH`. Arguments are literal
values; `$CC_INPUT_DIR` expands only when the job explicitly invokes a shell. Python jobs
select `python`/`python3`; shell jobs select `sh`/`bash`. Missing interpreters fail.

The replacement environment uses attempt-owned paths. Only `CUDA_VISIBLE_DEVICES`,
`NVIDIA_VISIBLE_DEVICES` and `LD_LIBRARY_PATH` are inherited from the trusted provider host.
Credentials, ambient proxies and arbitrary Python startup configuration are not inherited.
Declared job environment is workload data, not a safe place for provider credentials.

## Resources, setup and networking

GPU-required jobs check existing PyTorch, device count/memory and a tiny device computation,
then repeat resource checking after setup changes. Failure prevents payload execution.
Device capacity is not a VRAM reservation and this probe does not prove arbitrary payload
code used the GPU.

The requested network mode must match the adapter's `--network-mode` declaration. The runner
does not implement a firewall/network namespace or prove internet availability. Provider-side
configuration must establish that boundary; provenance reports this limitation explicitly.

Optional Python requirements use an attempt-local `--system-site-packages` venv. Only a
complete compatible list of `name==version` pins and comments is accepted: no editable/VCS/URL
requirements, custom indexes, source builds or replacement of managed GPU/framework packages.
Install is wheel-only, `--no-deps`, noninteractive and bounded by setup time, without retry or
upgrade. Internet must be explicitly requested; staged offline wheels are not implemented.
Shell setup uses explicit argument arrays under the same budget. Neither runs during validation.

## Deadlines and results

Preparation, resource checks and setup share `setup_seconds`. Setup plus finalization grace
must be strictly less than the remote wall budget. An optional immutable
`timeouts.execution_deadline` is a canonical whole-second UTC timestamp, for example
`2026-10-12T20:00:00Z`. The earlier absolute deadline or relative setup/payload bound wins. New
admission, authorization and start after expiry are rejected; replay returns the original
receipt and does not rearm execution. The runner checks UTC and Linux `CLOCK_BOOTTIME`; payload
supervision checks the deadline every 50 ms and sends TERM then KILL within a documented
one-second grace, subject to a schedulable Linux host. Finalization has its independent
`finalization_grace_seconds` bound. The SSH container supervisor may add up to two seconds.
Software cannot stop a powered-off or suspended host; after resume it observes expiry. These
limits do not cover deliberate session escape, uninterruptible kernel I/O, SIGKILL/OOM or provider
loss. See [execution deadlines](../docs/execution-deadlines.md) for the full contract.

The SSH adapter requires an advertised `absolute_execution_deadline` capability for absolute
deadlines. It checks engine clock skew is at most five seconds and rejects skewed or unverifiable
checks before start. Timed-out absolute jobs return runner status 124. Provider queue/allocation
time remains separate from runner time. Host sleep, VM clock behavior and reboot remain
unqualified until explicit live tests.

For `execution_control=prepare_then_start`, the SSH container stays stopped after byte
verification. Execution requires a separate `--start-control` file with version
`compute-relay/start/v1`, exact manifest SHA-256, job/attempt/nonce, readiness revision,
authorization identity and finite start/wall/deadline bounds. The trusted supervisor and child
validate this control before creating a work root; budgets tighten only in memory. The control
contains no workload secrets. See [preparation gates](../docs/preparation-gates.md).

Stdout/stderr default to 20 MiB retained per stream, with seen/stored counts and truncation.
Known credential-pattern redaction is best-effort hygiene, not a universal secret detector.
Redacted child output is also mirrored to notebook stdout/stderr through a bounded best-effort
queue. A blocked writer, overflow or output failure can drop mirrored bytes without stalling
child drains or execution; retained files remain the artifact source. The separate Relay log
reader observes provider replay, with account-scoped live qualification still pending.
Declared outputs are checked for containment, links,
type, count, size and digest. Missing required output prevents success; a collection error
does not erase an earlier payload failure.

`control/execution-result.json` conforms to the public
[result manifest](../api/schemas/result-manifest.v1alpha1.schema.json), preserving identity,
phase, exit code and timeout facts. `control/environment.json` records bounded runtime/setup
provenance. CLI exit 0 means a completed runner result, 1 a recorded unsuccessful result,
and 64 rejected input or inability to finalize. Absolute-deadline SSH containers use the trusted
`--supervise` entry point and return 124 on timeout. A hard kill may prevent any final output.
A runner cancellation phase does not prove provider cancellation or hardware release.

The control plane still requires matching provider termination and verified artifact
publication. Code/input/scratch files are not application artifacts. See
[collection](../docs/development/collection.md) and [artifact retrieval](../docs/development/providers/kaggle-artifacts.md).

## Contributor checks

```text
python runner/check.py
go test ./runner
```

The Python check validates source/JSON syntax, the asset lock, decoder and explicit CPU
fixtures. It can execute those repository-owned fixtures on Linux; it does not call Kaggle
or allocate GPU. Go tests check the embedded source inventory/lock. For an intentional,
reviewed asset change only, update hashes with `python runner/check.py --lock` and rerun checks.
Never refresh the lock to conceal an unexpected source difference.

The locked client/CI Python is 3.11.16. Native control-plane checks on macOS do not
mean the remote runner supports those hosts. Keep full operator/provider run records outside this
guide; put only support-changing sanitized evidence in a focused issue/PR. Design constraints are
in [ADR-0007](../docs/development/decisions/0007-finite-remote-runner.md).
