# ADR-0001: Isolate the pinned official Kaggle client

- **Status:** accepted
- **Date:** 2026-09-13
- **Decision owners:** repository owner and implementation maintainers
- **Related requirements:** PRD-03, PRD-04, PRV-01, PRV-02, PRV-03, SEC-01,
  SEC-03, VER-03
- **Related evidence:** [`../research/kaggle-interface-review.md`](../research/kaggle-interface-review.md)

## Context

The control plane is implemented in Go, but the first provider's supported automation
surface is the official Python-based Kaggle client/CLI. The v2.2.4 CLI offers relevant
kernel, dataset, log, quota, and output operations, while not every operation provides a
stable machine-readable format or sufficient execution identity through its documented CLI.

Reimplementing undocumented web/editor endpoints in Go would weaken compatibility,
security, and terms compliance. Exposing Python or Kaggle details to applications would
violate the provider-neutral sidecar boundary.

The provider mutation path also needs explicit retry and ambiguity semantics. A generic
subprocess wrapper that retries any nonzero exit could duplicate compute.

## Decision drivers

- Use supported official interfaces only.
- Keep consuming applications independent of Python and Kaggle.
- Obtain structured results and execution identity where CLI text is insufficient.
- Pin and reproduce the exact client behavior used by fixtures and live evidence.
- Prevent shell injection, credential leakage, unbounded output, and accidental mutation
  retries.
- Preserve the ability to replace transport details without changing the common provider
  contract.

## Considered options

### Option A — Rewrite provider HTTP/editor APIs directly in Go

This appears to simplify packaging but would require undocumented endpoints, authentication,
and response semantics. It would drift from the supported client and encourage brittle
browser-automation behavior.

### Option B — Invoke only the human-facing CLI

This uses an official surface and is suitable for several operations, but status/push/output
formats are not uniformly structured. Parsing prose for durable identity and error semantics
would be fragile.

### Option C — Pinned CLI plus a narrow public-client bridge

The Go adapter owns orchestration and subprocess safety. It invokes documented CLI commands
where sufficient and a small Python bridge over public official-client APIs only where
structured data or identity cannot be obtained safely. The bridge is an internal executable,
not a service or application dependency.

## Decision

Select **Option C**.

The initial provider-tool environment will:

- pin `kaggle==2.2.4`, a concrete Python 3.11 runtime, and the exact resolved transitive SDK
  versions;
- live beneath an operator-controlled provider-tools directory or optional container image,
  never by silently modifying the global Python environment;
- expose a small allowlisted operation set, not arbitrary Python execution;
- accept structured request data through a controlled file/stdin contract and return
  structured JSON on stdout with diagnostics on stderr;
- use argument arrays, controlled executables and working directories, a minimal environment,
  bounded stdout/stderr, operation deadlines, and process-tree cleanup;
- pass credentials only through the explicitly configured official mechanism and never to
  remote job bundles or command-line arguments;
- prohibit wrapper-level automatic retry of compute-creating mutations after an ambiguous
  outcome; and
- retain raw/sanitized provider codes and opaque identities without leaking them into the
  common application contract.

The exact CLI-versus-bridge mapping is finalized after M-1 captures live responses. This ADR
selects the boundary and safety policy, not an optimistic capability map.

## Consequences

### Positive

- The project follows a versioned official client path.
- Go remains the application-facing control plane.
- Structured adapter responses can be introduced without parsing every human message.
- Provider dependencies and credentials remain isolated from applications and remote
  workload code.
- A future client upgrade has one testable compatibility boundary.

### Negative or limiting

- Direct installation requires a managed Python prerequisite for the Kaggle adapter.
- Release/doctor tooling must handle two runtimes.
- The bridge needs its own formatting, type, unit, fixture, and security tests.
- A single self-contained Go binary is not promised for the Kaggle-enabled build.

### Follow-up

- M1-01 creates the reproducible client environment and command inventory.
- M1-02 captures read-only auth/quota fixtures.
- M1-08 records the final operation mapping or supersedes this ADR if official evidence
  supports a simpler boundary.
- M4 adapter tests pin every accepted response shape and unknown-value behavior.

## Verification

- Reproducible installation reports exact Python/CLI/SDK versions.
- Secret canaries never appear in argv, output fixtures, generated sources, or diagnostics.
- Subprocess tests cover deadline, output limits, process cleanup, invalid JSON, and nonzero
  exits.
- Fault injection proves an ambiguous mutation is not automatically repeated.
- Applications and common domain packages do not import Kaggle/Python client types.

## Rejected alternatives

Option A is rejected because supported-interface fidelity is more important than a
single-language marketing claim. Option B is rejected as the sole mechanism because durable
identity/error semantics must not depend on unstable prose parsing.
