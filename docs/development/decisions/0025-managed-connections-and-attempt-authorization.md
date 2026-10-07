# ADR-0025: Managed connections and durable attempt authorization

- Status: accepted
- Date: 2026-10-07
- Refines: ADR-0021, ADR-0022, ADR-0024

## Context

A companion application needs to manage saved provider connections while the local runtime is
running. Offline profile commands and environment-only credentials do not provide asynchronous
account discovery, a write-only UI boundary or restart-safe per-attempt execution authorization.
Multiple configured credentials must not multiply one account's quota or redirect admitted work.

## Decision

Add optional authenticated workspace management and explicit execution scopes, durable
administration operations, adapter-owned descriptors and account discovery, and a Relay-owned
protected credential store. The initial managed backend is macOS Keychain; explicit standalone
environment credentials remain available. No application job, SQLite row, argument or log may
contain a provider secret. Secret-bearing replay uses a protected keyed fingerprint.

Publish unique immutable admission profiles for verified connection revisions. New selection
changes future admission only; old jobs retain their account and recovery access. Aggregate
capacity by canonical provider/account identity, not credential count. Keep quota uncertainty
visible and block unknown managed GPU capacity by default.

For managed profiles, persist one explicit finite authorization per attempt and consume it before
preparation mutations. Recovery can observe and collect without granting compute. A retry is a
new attempt requiring a new grant; startup never refills authorization. Existing standalone
bounded workers remain explicit and cannot bypass managed authorization.

The detailed API, cross-resource commit protocol and fault obligations are owned by
[managed connections](../managed-connections.md). Public job and artifact envelopes remain
compatible. Handlers acknowledge durable local operations and do not invoke provider APIs.

## Alternatives and consequences

Keeping provider credentials in consuming applications duplicates provider policy and prevents
Relay from enforcing account continuity. Plaintext persistence or secret hashes in SQLite violate
the credential boundary. Replacing immutable profiles with mutable aliases can retarget queued
work; process counters alone lose consent limits on restart.

The protected store and state database require explicit recovery because they are not one
transaction. Platform support must be qualified independently; an unavailable vault is an
explicit failure. Native and live-provider evidence remain separate from offline fixtures.
No automatic account rotation, billed fallback, hosted credential service or remote cancel
capability is introduced by this decision.
