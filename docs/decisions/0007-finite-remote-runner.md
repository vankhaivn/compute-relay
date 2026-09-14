# ADR-0007: A finite Python runner outside the control plane

- **Status:** accepted
- **Date:** 2026-09-14
- **Task:** M2-09
- **Requirements:** JOB-02, JOB-03, JOB-05, SEC-02; proposal sections 9, 11, 14.5 and 19.

## Context

The portable core needs a concrete generic runner without implementing a local execution
provider, coupling business commands to Kaggle, or treating fixture success as live GPU
evidence. The approved proposal requires remote Python/shell commands, immutable input
identity, bounded setup and supervision, honest errors and a machine-readable result.

M2-07 already defines a strict regular-file USTAR/gzip bundle. M2-03 defines the public
result manifest. M3 durability and M4 provider integration are not available yet; they must
not be implied by an executable runner asset.

## Decision

Use Python 3.11+ standard-library code under `runner/python`, with Linux as the explicit
remote execution target. Do not import or invoke it from Go API validation/admission or
the fake provider. Require a deliberate `--execute` mode; `--validate-only` never launches
commands or creates the work root. CI pins Python 3.11.16, matching the repository's
existing Python baseline, separately from the local 3.13.5 verification environment.

Define `compute-relay/runner/v1` as an internal resolved-attempt manifest. It contains only
frozen byte identities, staged relative paths, generic execution/resource/network settings,
outputs and finite limits. Preserve the public HTTP job and result schemas unchanged.
Hash input identity independently of provider physical paths and check each staged file.

Extract the exact M2-07 regular-file USTAR subset ourselves, checking raw headers before
interpretation and enforcing compressed/expanded/file/path limits. Do not use unrestricted
`extractall`. Publish only into a new adapter-owned work root and never resume/reuse it.
This is containment for trusted-operator workloads, not attestation against hostile code
sharing the same user or an administrator mutating storage.

Launch explicit argument arrays with a replacement environment and a new Linux process
group. Stream both output pipes instead of buffering whole logs with `communicate`.
Terminate the group on timeout, signals and leader completion; bound termination waits.
Preserve the payload exit code and setup process outcome separately. Record a missing or
unavailable result honestly rather than synthesizing success after a hard kill.

Check a GPU requirement using a small computation in an isolated interpreter with existing
PyTorch, plus device count/capacity checks. Never install or replace the managed GPU stack
implicitly and never downgrade to CPU. An adapter's network-mode declaration must match the
manifest; actual egress enforcement remains with the provider. Neither a watchdog nor a
runner result proves that provider hardware was released.

For optional Python requirements, choose a per-attempt venv with system packages visible,
strict complete `name==version` pins, wheel-only/no-deps installation and no retries/upgrades.
Reject GPU/framework replacement and alternate-index/VCS/URL options. This intentionally
narrow strategy needs live environment validation before being advertised for Kaggle.
It is safer than blindly reinstalling a GPU framework or changing the provider's base
Python environment. Shell setup remains explicit argv under the same setup budget.

Keep logs bounded with visible truncation and known-pattern redaction. Results reference
only declared verified output files. Write the existing result-manifest shape and a separate
bounded environment/process-provenance record using temporary-file flush and rename.
Do not equate a payload result with provider terminal or accounting evidence.

## Tooling and verification

The runner and its tests have no third-party Python dependency. Record that empty runtime
inventory and hash all source/JSON assets in `runner/assets.lock.json`. Use standard-library
syntax, indentation and whitespace checks plus contract/fault tests now. A third-party
formatter and static type checker remain deferred; no claim that they ran is made.

Local Linux verification used Python 3.13.5, 24 unittest methods, real finite CPU/shell
payloads, actual process-group timeout and SIGTERM tests, and 94% statement coverage.
GPU-positive and pip installation paths are synthetic command/policy tests only. No
provider credential, package download or GPU allocation was used.

Add one offline CPU-fixture CI job on pinned Python, with generated result JSON checked
against the existing schema by the existing Go dependency. It is a code/contract check,
not a deploy workflow or live remote-runner test. Keep all provider prerequisites and
credentials outside this workflow. M3/M4 must integrate the asset through durable attempt
identity and explicit provider execution, then establish actual environment compatibility.

## Alternatives and consequences

A Go-only remote binary would add target-binary delivery to managed Python environments.
An HTTP worker polling for more commands would violate finite batch boundaries. Running
business commands locally through the fake provider would invalidate the control-plane
safety model. These alternatives are rejected for this milestone.

The narrow requirements format intentionally excludes some valid pip features. Network
isolation, arbitrary package compatibility, deliberate process escape, hard-kill finalization,
exact provider release time and live log delivery are not guaranteed. These limitations
remain visible in the [runner guide](../../runner/README.md), not successful-looking stubs.

## Primary references reviewed

These sources describe language/tool behavior, not a verified Kaggle environment:

- [Python 3.11 subprocess](https://docs.python.org/3.11/library/subprocess.html): argument
  vectors, environment replacement, new sessions, process outcomes and pipe supervision.
- [Python venv](https://docs.python.org/3.11/library/venv.html): per-environment setup and
  the explicit system-site-packages option.
- [Python tarfile](https://docs.python.org/3/library/tarfile.html): archive extraction risks;
  this runner uses a stricter framing/member policy rather than unrestricted extraction.
- [pip configuration](https://pip.pypa.io/en/stable/topics/configuration/): disabling
  configuration inheritance with `PIP_CONFIG_FILE` pointing to the null device.

The approved requirements are in [the proposal](../proposal.md); implementation and
limitations are in [runner/README.md](../../runner/README.md).
