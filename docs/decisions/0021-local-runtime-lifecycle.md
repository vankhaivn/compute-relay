# ADR-0021: Explicit local administration and joined HTTP lifecycle

- Status: proposed; M5-01a implemented in PR #25, pending owner review/merge
- Date: 2026-09-16
- Requirements: API-01/03, DX-01; preserves SEC-02 and durable ownership
- Extends: ADR-0004/0008/0009/0012 without replacing their contracts

## Context

M4-06's fixed acceptance harness is merged, but no live provider acceptance was established.
M5-01 contains several independent product surfaces. A coherent first slice can make the local
installation and existing durable HTTP services executable without enabling unqualified workers
or claiming the whole parent task is done. The existing binary only exposed help/version/bundle.

The store and blob components have separate identities/locks. Opening a missing directory may
initialize a component, which is inappropriate when an operator expects to reopen an existing
runtime. Token metadata and a one-time private secret file also cannot be atomically committed
as one resource. Finally, a returned Serve/Close call is not sufficient proof of handler exit.

## Decision

Split M5-01a from later application-client, profile/provider and artifact/log route work.
Compose real local auth/object/admission/control services, not a production fake provider or a
nondurable fallback. Expose explicit init/state/workspace/token/validate/serve commands through
context-aware main routing. Keep parent M5-01 in progress and M4 live gates unverified.

Create only a new private installation with separate SQLite/input/result stores and an internal
canonical marker binding their identities. Require all original evidence on reopen; missing,
corrupt or swapped roots cannot silently create replacement state. Keep exclusive OS ownership
through all consumers. Administrative commands require the server to stop first; add no HTTP
administration endpoint. Preserve existing database migrations and backup limitations.

Use existing durable workspace/token services. Create workspaces with no profiles and never
upsert authority from a create operation. Require explicit scopes, bounded expiry and a new
private output file for tokens; emit only non-secret receipt metadata. Resolve parent aliases
before excluding protected store paths. Attempt independently bounded revocation after delivery
failure/panic, expose uncertainty honestly, and clear owned buffers without promising erasure.
A crash/output failure can follow a durable effect; neither exit status nor compensation is a
cross-resource transaction. Retain operator trust and bounded expiry as explicit constraints.

Serve only literal loopback and bind Host validation to the actual assigned port. Reuse existing
HTTP authorization/body/rate/timeout/idempotency semantics. Advertise local-admission-only mode
and dispatch disabled; readiness refers to local dependencies, not usable provider capacity.
Do not construct provider workers, schedulers, collectors, input fetchers or retention sweepers.
General profile setup remains later work; no synthetic default profile claims execution support.

Propagate interrupt/SIGTERM to lifecycle context. Stop accepting handler work, attempt bounded
graceful shutdown, force connection close when needed and join accepted handlers before closing
stores. The join intentionally favors ownership safety over pretending uncooperative callbacks
are hard-real-time. No hijacking/websocket surface exists here. Process death uses existing
durable recovery, not a claim that remote work has stopped.

Parse new local flags without side effects; reject duplicate/cross-command options and avoid
raw argument/secret/path diagnostics. Validate job files locally through the existing schema
without suggesting that unknown profiles/objects are admitted or provider-checked. Preserve the
older bundle/version surface and do not mix TOML, new wire contracts or provider fact changes.

## Alternatives rejected

- Auto-create missing component state on serve: can detach persisted ownership from its bytes.
- Seed a fake/default executable profile: misrepresents current runtime/provider composition.
- Return token secrets on stdout or accept them as arguments: expands accidental logging/history.
- Treat token delivery failure as automatic rollback: metadata and file writes are not atomic.
- Close stores immediately after Server.Close: active handlers may still use them.
- Permit public/wildcard bind for convenience: adds an unreviewed exposure/security surface.
- Mark all M5-01 complete from a local server: conceals missing application and provider workflows.

## Verification and limits

Unit and real loopback/SQLite/blob tests cover canonical state, exclusive reopen, private token
issuance/revocation, authority, uploaded bytes, receipt replay and local control with no remote
intent. Real child processes invoke the actual main and reopen after interrupt or Windows
process death. Standard-library parser/lifecycle tests also run locally in explicitly disclosed
isolated modules; full pinned integration/native/race results are recorded in PR #25.

No dependency, workflow, public schema or database migration change is required. Symlink/path
checks do not protect against the host owner racing replacement; disk permissions are not memory
attestation. Existing incomplete initialization is retained for investigation, not automatically
repaired. No profile-management CLI, worker composition, artifact/log client/routes, cleanup,
TOML, deployment, live acceptance or finished release is delivered in this slice.

The [local runtime guide](../local-runtime.md) records commands, defaults, token uncertainty,
HTTP lifecycle evidence and the standard-library source. The plan owns current milestone status.
Stop after PR #25 for owner review/merge before another slice.
