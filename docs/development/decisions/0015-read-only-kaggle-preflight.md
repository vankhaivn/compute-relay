# ADR-0015: Explicit credential scope and read-only preflight

Status: accepted. Requirements: PRD-04/08, SEC-01, PRV-03; preserves DUR-03 and live gates.

## Decision

Default to local pinned distribution checks without importing authenticated client code or
resolving credentials. Require explicit read-only opt-in for token introspection and quota-data
retrieval. Resolve only allowlisted environment references afresh; support opaque API tokens,
not ambient home/file/legacy/OAuth fallback. Compare an active server-returned account with the
immutable configuration before the quota read. Always keep preflight batch readiness false.

Use fixed isolated Python, an absolute controlled interpreter, empty temporary home/cwd, restricted
environment and stdin-only token. Return closed sanitized enums, not account names, paths or raw
provider errors. Keep the official SDK operation/typing path with a documented private session
hook bound to its reviewed version. Allow only exact ordered HTTPS reads, verified TLS, bounded
strict JSON and no redirect/retry/ambient proxy. Parent deadline and watchdog bound the leaf helper.

## Reason and consequences

Configured usernames and SDK defaults are not account evidence. CLI import/authentication and
implicit fallback can consume credentials during a supposedly local check. The explicit boundary
limits compatibility and introduces version-bound transport maintenance, not a stable public
session-injection guarantee. Clearing owned buffers cannot erase all OS/string copies.

See [setup/preflight](../providers/kaggle-preflight.md) and
[operator validation](../validation-checklist.md).
