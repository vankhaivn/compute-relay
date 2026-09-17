# ADR-0022: Immutable admission profiles and one-shot application requests

- Status: accepted; PR #26 merged on 2026-09-17
- Date: 2026-09-17
- Task: M5-01b, a delivery slice of parent M5-01
- Requirements: API-01/03, DX-01; preserves SEC-02 and accepted job/receipt identity
- Extends: ADR-0009, ADR-0012 and ADR-0021

## Context

The merged local runtime can serve durable application handlers, but new installations have no
profile configuration command or application CLI. Test-only SQL/profile seeding is not a usable
operator interface. Adding clients must not duplicate admission logic, silently choose an attempt,
follow a redirect with credentials or turn a lost response into automatic request/compute replay.

M3 already stores immutable profile revisions, accepted bindings and original idempotency receipts.
M5-01a supplies exclusive local administrative ownership and safe HTTP lifecycle. These boundaries
can support the next local-product slice without enabling general provider workers or declaring
M4-06/M1 live acceptance complete.

## Decision

Expose local profile apply/show and explicit workspace grant/revoke through the existing executable,
operator parser and exclusive Host lifecycle. Reuse PutProfile, profile_revisions/profiles and the
workspace allowlist; add only a validated local read, not a second configuration database/migration.
Keep profile admission metadata distinct from complete provider configuration and M5-02 TOML.

Use a small flat, versioned private JSON document with exact fields, bounded policy and optional
credential reference. Reject unknown/duplicate/case-alias/null/invalid fields before opening state.
References remain inert metadata, never resolved by these commands. Store each revision immutably;
remapping requires a new revision, while enabled status is independent. Applying never grants
workspace access or enables dispatch. Grant/revoke changes only that workspace's allowlist and
preserves its enabled status. Do not delete historical revisions or retarget accepted jobs.

Output only bounded profile metadata, policy and snapshot identity, omitting account scope and
credential references. Explicitly report provider_checked=false and dispatch_enabled=false. The
snapshot digest covers private canonical policy but is not proof of provider availability.

Add application CLI commands for upload, contextual validation, admission/status and explicit
controls/current-operation reads through the running authenticated loopback API. They do not open
SQLite or construct providers. Retain the original local schema-only validate command separately.
Require explicit workspace, private token file, source files and target IDs; submit/control require
an explicit idempotency key, controls a source attempt, and retry a reason. Do not invent a key,
select an implicit attempt, refresh immutable input or silently select another account/provider.

Validate job files with the existing strict admission parser, then send canonical bytes. Hash upload
sources before and during transport, join transport source ownership, and require final local
EOF/Close/stability plus a matching object_id/workspace/size/digest receipt. Completed server-side
upload followed by failed client acknowledgement remains uncertain; it is not rolled back.

For each request use one fresh HTTP/1 Transport with no connection reuse, proxy, cookie jar,
redirect following or GetBody replay. Go's documented reused-connection retry path can include
Idempotency-Key requests; a fresh one-request transport removes that path rather than merely
omitting an application retry loop. Bound connection/header/context/response work. Require literal
loopback HTTP authority; remote/TLS configuration remains separate. The local host/service is
trusted, not authenticated by its IP address alone.

Check successful response framing, bounded strict JSON and original target fields before returning
raw metadata. Preserve compatible additional fields and unknown state text without normalizing them
to success. The checks are not a full future response-schema validator. Respect the existing
object_id field and retry_compute operation kind; retry returns a distinct new_attempt_id while
attempt_id still names its source. Current status and original receipt replay remain separate.

Expose only sanitized failure stage, HTTP status/code and conservative local-commit uncertainty.
Do not echo server exception/message/details, paths or tokens. Literal current-token rejection
includes escaped JSON strings but is not a universal secret detector. Never automatically repeat
an HTTP request, including observational requests and uploads. Nonzero exit or output failure is
not evidence of rollback; object uploads have no server idempotency-key contract and can duplicate
objects when explicitly repeated. No hidden receipt cache or automatic recovery loop is added.

## Alternatives rejected

- Seed a fake/default executable profile: conceals missing real provider configuration.
- Mutable profile contents under one revision: changes the meaning of accepted history.
- Apply and implicitly grant/activate workers: conflates configuration, authority and side effects.
- Direct database writes from application CLI: bypasses the running server's ownership/auth boundary.
- A shared default HTTP client: permits ambient proxies, redirects or transport replay behavior.
- Print token values or accept arbitrary headers/URLs: widens credential exposure and authority.
- Treat all HTTP 2xx or pipe EOF as success: admits wrong target or unacknowledged bytes.
- Convert unknown states to familiar terminal states: invents execution evidence.
- Reuse current active profile to replay an old request: violates original receipt/binding semantics.

## Consequences and limits

Local administration requires serve to stop. Apply/grant are separate transactions, not an atomic
multi-command deployment. Credential references and account scopes are operator assertions until a
separately configured provider validates them. Admission can queue work but this serve mode starts
no scheduler, provider, collector or sweeper. No runtime restart implicitly grants new compute.

Private token/source paths and callbacks remain trusted operator inputs. Regular-file and stability
checks are not hostile-host isolation. Joined request readers may delay shutdown when callbacks or
OS operations ignore cancellation. File growth or late error can invalidate a client receipt after
the server committed; original evidence must be retained. Buffer clearing is not secure memory/file
erasure. Fixed budgets and double hashing bound work without a throughput or partial-resume promise.

Unknown response fields remain untrusted data. The application CLI checks selected identity/type
boundaries rather than promising all future schemas or business semantics. Local-commit uncertainty
is not remote compute evidence. A scoped retry operation remains subject to server eligibility and
original frozen-input policy; its existence is not automatic compute retry.

## Verification and owner boundary

Pure JSON/argument tests cover ambiguous fields, bounds and inert help. Actual loopback clients test
one request, lost header/body acknowledgements, redirects, unknown states, exact control targets,
late Close, cancellation and request-reader joining. Source-growth-after-consumption and failed
stdout tests return no successful receipt and perform no automatic replay.

Real CLI/service/SQLite/blob integration uses actual profile apply/grant, private token issuance,
upload and contextual admission. A test proxy discards the response after the real transaction
commits; explicit replay recovers the same IDs. Reopen/remap/revoke and queued controls retain the
original profile, receipt and bytes, with zero remote submission/resource/publication records.
These are local tests, not a provider execution or additional live process-kill experiment.

Exact-head native/race/fault evidence and narrow local Go 1.23.2 client/JSON checks are recorded in
PR #26 and the [application guide](../application-cli.md). No temporary local module, substitute
driver, dependency/workflow/schema/migration change or provider credential is shipped. Stop for
owner review/merge after PR #26. Parent M5-01 results/workers, M5-02 and M4-06/M1 live gates remain.
