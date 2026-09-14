# Finite remote runner

> **Task:** M2-09, implemented offline; PR #10 in review.
>
> **Execution target:** Linux with Python 3.11 or newer. CI pins Python 3.11.16.
> This is a remote deployment asset, not a local compute provider or an HTTP admission
> handler. No Go production component invokes it locally.

The runner processes one resolved attempt, writes bounded results, and exits. It does not
poll for more work, retry a payload, call the control plane, or claim provider hardware
release. The implementation uses only the Python standard library. An optional GPU check
uses PyTorch already present in the remote environment; the runner does not install it.

See [ADR-0007](../docs/decisions/0007-finite-remote-runner.md) for the design and evidence
boundary, and the [approved runner requirements](../docs/proposal.md#11-remote-runner-and-execution-lifecycle).

## Files and contracts

| Path | Purpose |
|---|---|
| `python/run.py` | Explicit validation or one-attempt execution entry point. |
| `python/relay_runner/contract.py` | Strict resolved-manifest decoding, limits and semantic validation. |
| `python/relay_runner/files.py` | Verified staging snapshots, strict bundle extraction and declared-output hashing. |
| `python/relay_runner/process.py` | Linux process-group supervision and bounded log capture. |
| `python/relay_runner/main.py` | Preparation, environment/resource checks, setup, payload and finalization. |
| `manifest.schema.json` | Internal adapter-to-runner Draft 2020-12 manifest, version `compute-relay/runner/v1`. |
| `examples/` | Schema/decoder examples, not a complete staged execution. |
| `assets.lock.json` | Reviewable hashes and lengths of runner source and JSON assets. |
| `dependency-inventory.json` | Empty third-party runtime dependency inventory and toolchain boundary. |
| `check.py`, `tests/` | Credential-free checks and explicit repository-owned CPU fixtures. |

The public HTTP job schema is unchanged. A future adapter resolves immutable object IDs to
staged relative paths, supplies job/attempt/nonce identities, and produces this narrower
manifest. It contains no source URLs, provider credentials, runtime tokens, account
configuration, or callback addresses. `bundle` has `path`, `bytes`, and `sha256`; every input
has `name`, staged `path`, logical `target`, `bytes`, and `sha256`.

`input_manifest_sha256` hashes the compact ASCII JSON array of input records containing
only `name`, `target`, `bytes`, and `sha256`. Sort records by target and object keys
lexicographically, with no whitespace or trailing newline. Physical staging paths are
excluded; remapping a provider path does not change input identity. Changing input bytes or
logical targets does. The runner verifies both the aggregate identity and every copied file.

The schema rejects unknown fields and bounds representation. Runtime validation additionally
checks duplicate keys, reserved environment names, portable path/case/prefix collisions,
aggregate byte limits, budget arithmetic and actual digests. The source manifest must be
at most 1 MiB. Job limits may lower, but not silently raise, the runner's bounds.

## Entry points

From the repository root, validation is non-executing and does not create a work directory:

```text
python runner/python/run.py --manifest runner/examples/request.valid.json --validate-only
```

The committed example contains illustrative staged identities. It is suitable for contract
validation, not execution without matching bundle/input bytes.

An adapter, or an explicitly authorized fixture on a disposable test host, invokes execution
with prepared paths:

```text
python runner/python/run.py --manifest resolved-attempt.json --execute --staging-root staged --work-root attempt-new --network-mode disabled
```

`--execute` really runs the manifest's commands on the machine invoking it. Never use it
as a control-plane admission check or an automatic local fallback. The work directory must
not already exist and must not overlap staging. Parents and staging belong to the trusted
adapter. A second invocation cannot overwrite or resume the same work root.

## Lifecycle and application environment

The runner verifies frozen inputs, extracts the M2-07 bundle into a fresh directory,
checks the environment/resources, performs bounded setup, invokes the exact argument
vector, supervises its process group, hashes declared outputs, and atomically writes results.
No `shell=True`, extra shell interpolation, or automatic retry is introduced.

```text
attempt-new/
  code/       validated bundle files
  inputs/     verified copied input snapshots
  outputs/    declared payload results
  scratch/    disposable files, HOME and optional venv
  control/    manifests, environment provenance and bounded logs
```

Applications receive `CC_JOB_ID`, `CC_ATTEMPT_ID`, `CC_CODE_DIR`, `CC_INPUT_DIR`,
`CC_OUTPUT_DIR`, `CC_SCRATCH_DIR`, and `CC_EXECUTION_MANIFEST_PATH`. Argument values such as
`$CC_INPUT_DIR` remain literal unless the application explicitly requests shell evaluation.
Python payloads select `python` or `python3`; shell payloads select `sh` or `bash`. Missing
interpreters fail rather than selecting a different execution kind.

A replacement environment supplies fixed search paths and attempt-owned temporary/home
locations. Only `CUDA_VISIBLE_DEVICES`, `NVIDIA_VISIBLE_DEVICES`, and `LD_LIBRARY_PATH`
are inherited as an explicit trusted-provider allowlist. Provider/runtime credentials,
proxy variables, arbitrary Python startup configuration and unrelated host environment
are not inherited or exported to provenance. Plain job environment remains non-secret.

## Resource and network evidence

For GPU-required jobs, an isolated interpreter imports existing PyTorch, checks count and
device-memory capacity, performs a tiny computation on visible devices, and synchronizes.
Failure prevents business execution and reports `RESOURCE_REQUIREMENT_UNSATISFIED`; CPU is
never a fallback. Device capacity is not a VRAM reservation, and a successful runner probe
does not prove that arbitrary payload code subsequently used the GPU.

The runner records Python/package and available GPU/framework/CUDA information in bounded
provenance. The resource check is repeated after dependency/setup changes. Local positive
GPU tests supply synthetic observations only. Actual GPU/provider compatibility still
requires authorized M1/M4 evidence.

The requested network mode must match `--network-mode`, which is the adapter's declaration.
The Python runner does not implement network namespaces, a firewall, or reliable internet
availability detection. Provenance says `provider_required_not_runner_enforced`. An adapter
must establish the requested provider setting or reject the job before claiming support.
CPU fixtures make no network request even though their host is not a network sandbox.

## Bounded dependencies

Optional Python requirements run remotely in an attempt-local `--system-site-packages`
venv, preserving access to managed GPU packages without modifying their base installation.
V1 accepts a restricted complete list of `name==version` pins and comments. Editable/VCS/URL
requirements, index flags, source builds and GPU/framework replacement are rejected.
Installation is wheel-only, `--no-deps`, no automatic retries or upgrade, noninteractive,
and under the shared setup deadline. Pip configuration inheritance is disabled.

The workload author must provide complete compatible transitive pins. Python requirement
installation requires explicitly requested remote internet; offline wheel staging is not
implemented in this task. Shell setup uses explicit remote argument arrays and the same
budget. Neither setup path executes during `--validate-only` or on API admission.

Installation command construction is unit-tested with a fake process result. No package was
downloaded and no remote venv/GPU-package compatibility was established in M2-09. These
remain provider-environment acceptance checks, not a promise that arbitrary requirements work.

## Deadlines, logs and results

Preparation, resource checks and setup share `setup_seconds` within the remote wall budget.
Payload work stops before the finalization reserve. `setup_seconds + finalization_grace_seconds`
must be strictly less than `remote_wall_seconds`; invalid budgets are rejected. The deadline
origin is the runner's monotonic start, distinct from provider queue/allocation clocks.

Each child starts in a new process group. On timeout, SIGTERM/SIGINT, or leader completion,
the wrapper sends group TERM then KILL and bounds its waits. Even a successful leader is
not permission to leave ordinary background children running. Deliberate session escape,
uninterruptible filesystem/kernel calls, SIGKILL/OOM and provider loss are outside this
watchdog's guarantees. Provider-side timeout remains a separate necessary defense.

Stdout/stderr stream independently through 64 KiB reads. Each defaults to 20 MiB retained,
with explicit truncation markers and seen/stored counts. Known token/authorization patterns
are redacted across chunks. This is best-effort hygiene, not a general private-data detector.
No live-log delivery or structured-progress API is introduced.

Declared file/directory outputs are checked for containment, links, type, count, size and
digest. Missing required output prevents success; absent optional output is allowed. An
output limit or collection error cannot overwrite the original payload/setup failure.

`control/execution-result.json` conforms to the existing
[result-manifest schema](../api/schemas/result-manifest.v1alpha1.schema.json). It preserves
attempt identity, payload exit code (null when not launched), failure phase and timeout.
`control/environment.json` records process/setup exit codes, durations, bounded package
provenance and log truncation. Files are flushed and atomically renamed where possible.

CLI exit 0 means a completed runner result, 1 means a recorded unsuccessful result, and
64 means rejected input or inability to finalize; it does not fabricate a result. A hard
provider kill may prevent all finalization. A runner `cancelled` phase records local signal
handling, not proof of provider cancellation or accelerator release. The eventual control
plane still requires matching artifacts and provider terminal evidence before job success.

## Verification and contributor commands

```text
python runner/check.py
python runner/python/run.py --manifest runner/examples/request.valid.json --validate-only
```

`check.py` checks UTF-8/LF/trailing whitespace, Python 3.11-compatible syntax, indentation,
JSON parsing, the source hash lock, the actual request decoder and the CPU fixture suite.
It does not claim to run a third-party formatter or static type checker. A dedicated type
checker/formatter is deferred rather than adding unverified tooling dependencies here.
After reviewing an intentional source/JSON change, update the lock explicitly:

```text
python runner/check.py --lock
```

The Linux suite covers Python/shell success and failure, preparation identity, unsafe
archives, required/optional/directory outputs, setup deadlines, payload timeout, actual
SIGTERM, child cleanup, bounded logs, environment canaries and synthetic resource decisions.
The available Linux runtime ran all 24 tests on Python 3.13.5. An optional local coverage
measurement using the already available coverage tool reported 94% over 668 statements;
coverage is not a runner runtime dependency.

The `Runner offline` CI job repeats the suite on Python 3.11.16, exports actual fixture
result JSON into its temporary directory, then uses the repository's pinned Go schema
validator to check those exact results. Existing Go CI also checks the runner schema and
asset lock on native Linux/macOS/Windows. Native control-plane CI is not a claim that the
remote runner executes on macOS/Windows. No job authenticates Kaggle, installs workload
packages, deploys a service, executes admitted user code, or allocates GPU compute.
