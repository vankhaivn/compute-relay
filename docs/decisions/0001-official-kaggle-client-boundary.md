# ADR-0001: Isolate the official Kaggle client

Status: accepted. Requirements: PRD-03/04, PRV-01/02/03, SEC-01/03, VER-03.

## Decision

Keep the Go application/domain boundary provider-neutral. Use a pinned official Python
client environment: documented CLI operations where sufficient and a narrow public-SDK bridge
where structured identities/results are required. No undocumented editor/web scraping or
application-side Kaggle SDK dependency. Exact versions live in the client manifests/lock.

Use controlled executables, argument arrays, working directories and minimal environment;
allowlist operations, bound input/output and lifetime, and handle subprocess completion explicitly.
Provider credentials use only the configured mechanism, never workload bundles or argv.
A nonzero exit after a possible mutation is uncertainty, not permission to repeat it.
Current fixed helpers are leaf processes with parent/watchdog bounds, not arbitrary workload
supervisors. Their reviewed private transport seam is explicit in ADR-0015, not a public API claim.

## Reason and consequences

CLI-only prose parsing weakens identity/error handling; rewriting unsupported HTTP endpoints
in Go weakens compatibility and provider boundaries. The narrow official-client approach requires
a managed Python prerequisite and version-bound tests, but keeps applications independent.
Source/fixture compatibility is not live account evidence or a final provider go decision.

See [client research](../research/kaggle-interface-review.md),
[preflight](../providers/kaggle-preflight.md) and [provider contract](../providers/contract.md).
