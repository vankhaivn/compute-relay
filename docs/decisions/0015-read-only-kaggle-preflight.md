# ADR-0015: Explicit credential scope and read-only Kaggle preflight

- Status: accepted; PR #19 merged on 2026-09-16
- Date: 2026-09-16
- Task: M4-01 offline/read-only foundation
- Requirements: PRD-04/08, SEC-01, PRV-03; preserves DUR-03 and the M1 live gate
- Extends: ADR-0001 official-client boundary; no earlier decision is superseded

## Context and gate interpretation

The owner requested M4 after merging M3-08. M3 is complete at its implemented offline gate,
but M1 live acceptance and M1-08 go have not been established. Proposal section 23.6 permits
useful offline work and an opt-in live-test procedure while credentials are unavailable.
A preflight must not turn that permission into staging/submission authorization or infer
account identity from a configured username.

The existing credential port scopes transient byte access but has no environment resolver.
The pinned CLI supports multiple credential fallbacks, and its normal import/authentication
path is not an appropriate credential-free local version check. The official SDK exposes
structured introspection and quota methods but its default transport merges environment
settings and does not provide the bounded no-redirect seam this component needs.

## Decision

Implement a separate preflight service and finite `cmd/kagglepreflight` utility, not a
`provider.Provider`, dispatch registration or production `serve`. Require immutable non-secret
instance/revision/account/reference/interpreter configuration. Keep the original M1-08 go
dependency for batch activation; record M4-01's offline and live states separately.

Support only explicitly allowlisted `env:NAME` references and opaque API tokens in this
component. Resolve afresh per authorized call, clear the owned callback buffer on all exits,
and never search home directories, fall back to legacy keys/OAuth or cache a credential.
File and other credential modes require later deliberate implementation, not silent fallback.

Default to local checks of Python 3.11, kaggle 2.2.4 and kagglesdk 0.1.35 distribution metadata.
Do not import `kaggle` or the SDK or resolve a token in this mode. Require explicit read-only
opt-in before any credential/network use, and recheck local pins first. Version matching is
not installation integrity, authentication or batch readiness.

Run one fixed isolated Python helper with an absolute pinned-environment interpreter, empty
temporary home/cwd, restricted child environment and stdin-only token transport. No token
argument, credential file or raw diagnostic reaches the operator report. The helper calls
only public SDK token introspection and quota-statistics methods. Require an active token and
exact server-returned account match before quota. Expose quota-data availability only; do not
invent numeric allowance, freshness, scheduler capacity or eligibility. Always report
`batch_ready=false`.

Keep SDK request construction and typed response parsing, but explicitly bind a transport hook
to v0.1.35's private session layout through its public `http_client()` accessor. Reject a changed
layout/version. The hook limits the two exact production HTTPS RPC destinations and order,
uses requests' adapter without Session redirect-body prefetch, and disables redirects/retries,
ambient proxies and certificate overrides. Verify TLS, bound response bytes and strict JSON
before SDK decoding. This reviewed private seam is visible technical debt, not an invented
public injection API or an undocumented provider endpoint.

Bound token/config/output sizes, network reads and process lifetime. Use a Go process deadline
and an independent Python watchdog so the fixed helper self-terminates without its supervisor.
The utility handles interrupt/SIGTERM. The helper does not spawn descendants or run workloads;
it is not a reusable arbitrary-process supervisor. The local operator, interpreter/site packages
and resolver callback retain the existing trusted-host status. Buffer clearing cannot erase
all OS/immutable-string copies or provide a memory-forensics guarantee.

Return closed, validated enum reports with non-secret instance/revision and a check timestamp.
Hide actual usernames, credential references, paths, provider bodies and exception strings.
Keep authentication, account binding, quota availability and unimplemented batch readiness
independent. Read-only observations are not durable mutation permits or capabilities.

## Alternatives rejected

- Use normal CLI import/authentication for local checks: risks credential fallback or login behavior.
- Treat configured username or a successful quota call as verified account identity: insufficient.
- Put a token in arguments or a generated script/config: leaks through process metadata or files.
- Use default SDK redirects/environment settings: can forward or pre-read data beyond this scope.
- Rewrite all provider APIs or fork the SDK: unnecessary; retain public operations and a narrow guard.
- Return a stub dispatch-capable adapter or claim M1 go from offline fixtures: creates false capability.
- Stop all work until credentials exist: ignores the approved offline-work path and owner's request.

## Consequences and verification

Token-only environment references, canonical username subset, fixed production endpoint and
fixed client versions deliberately limit compatibility. The private session hook must be
reviewed and tested when the locked SDK changes. Numeric quota mapping, durable provider
configuration, unified TOML, staging/submission/observation/collection integration and remote
cleanup remain separate tasks. M1 live acceptance still blocks the batch-adapter go decision.

Go tests cover configuration, credential lifetime/order, process isolation/bounds, cancellation
and command opt-in. Python tests use the actual pinned SDK with mocked transport and separately
exercise redirect/body/JSON limits and standalone watchdog termination. The existing offline
Kaggle-client workflow watches helper changes; no live credentials or provider resource effects
are part of CI. Local harness results and pinned-stack CI are disclosed separately in PR #19.

The [preflight guide](../providers/kaggle-preflight.md) links the exact primary sources reviewed
on 2026-09-16, describes commands and limits, and preserves the historical M0/M1 evidence tiers.
Stop for owner review/merge after this PR; it neither starts M4-02 nor authorizes live compute.
