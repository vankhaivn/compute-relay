# Managed connections and execution authorization

This is the accepted extension contract for a locally managed companion. The routes remain
**planned** until composed and advertised by the runtime. Existing environment-configured
profiles and the standalone finite-worker mode remain supported; a saved connection is not
proof of authentication, capacity or a runnable provider.

## Authority and API

All routes use the existing authenticated literal-loopback transport, workspace isolation,
strict JSON parsing, request limits and sanitized error envelope. No provider work occurs in
an HTTP handler. The optional management composition advertises `managed_connections`; the
optional durable-permit composition advertises `attempt_authorization`. Clients must check
both features instead of assuming support from the API version.

| Method and workspace-relative path | Scope | Semantics |
|---|---|---|
| `GET /providers` | read | Installed adapter descriptors and credential-store availability; local only. |
| `GET /connections` | read | At most 100 saved, sanitized connections; local only. |
| `GET /connections/{id}` | read | Current revision, availability, immutable selection and cached quota. |
| `POST /connections` | manage | Label, provider type and write-only credential fields; asynchronous account discovery. |
| `POST /connections/{id}/actions` | manage | Explicit check, credential replacement, disable, enable or removal. |
| `GET /connection-operations/{id}` | manage | Current durable administration status; no secret/reference or raw provider error. |
| `POST /jobs/{id}/authorize` | execute | Authorize exactly the explicit frozen attempt and wall-time bound. |
| `GET /jobs/{id}/authorizations/{id}` | execute | Read current durable authorization state. |

`manage` and `execute` are separate workspace scopes, issued only through local administrative
token issuance. `read`, `write` and `operate` grant neither. A management token cannot execute
unless also explicitly issued `execute`. Applications should keep management and execution
tokens separate from ordinary production job tokens. Token issuance is not an HTTP endpoint.

Every POST requires one existing-format `Idempotency-Key`. Exact replay returns the original
receipt, with `replay=true`, after current authority validation; changed input conflicts.
A GET returns current state. Connection actions also require `expected_revision`; concurrent
changes have one winner. Replay is checked before the revision test. A stale fresh operation
returns 409 and never silently refreshes its target. Duplicate JSON keys, case aliases, null
values for non-nullable fields and unknown fields are rejected. Credential-bearing requests
are bounded to 64 KiB overall, at most 16 fields and 16 KiB per field, with smaller adapter
limits allowed. Request bodies, raw helper output and credential values are never logged.

## Descriptors, verification and immutable selection

A descriptor defines credential field names, labels, required/write-only status and byte
bounds, plus capability support. Consumers render these definitions without importing provider
SDKs. The adapter rejects fields it does not declare. The initial Kaggle descriptor accepts an
API token and discovers the canonical account through authenticated read-only introspection;
the caller does not supply an account name. Discovery, quota inspection and credential-store
availability are separate facts. No check allocates compute or creates provider resources.

Every accepted mutation reserves a new monotonic connection revision and gates new admission
until its outcome is known. A successful verified connection publishes a unique immutable
profile name for that revision, returned as `selection.profile`. Existing job JSON remains
unchanged: the caller copies this exact name into `profile`. The generated alias is never
reused or repointed. Publishing a later revision disables the earlier admission alias in the
same transaction; previously admitted jobs retain their frozen original profile/account.
Admission checks alias eligibility transactionally, so a request using a stale selection
cannot race a connection change. An exact admission replay still returns its original receipt.

The account ID is an opaque Relay identity, not a credential or provider username. Provider
and canonical account together define the shared quota/capacity key. Two connections for the
same account do not create two slots or two quota balances. Quota projections carry observation
time and a conservative lower bound after reservations. Unknown/stale quota has null remaining
capacity, never a manufactured number. The managed default blocks new GPU authorization when
quota is unknown, stale or insufficient; cached availability alone never guarantees dispatch.
There is no account rotation, fallback, pooling or automated selection.

## Credential storage and crash recovery

Credentials remain in a Relay-owned protected store, initially macOS Keychain. SQLite stores
only opaque references, generations, verification outcomes and a keyed request fingerprint.
Explicit environment references remain supported in standalone mode. An unsupported or denied
vault does not fall back to plaintext files, SQLite, process arguments or ambient credentials.

The credential port uses bounded byte callbacks; values are cleared after use where possible.
The protected store also owns the installation-specific HMAC key used to compare secret-bearing
requests. An unkeyed hash of a low-entropy secret is not a safe idempotency fingerprint. Rotation
or loss of that key must not turn an old request into a new operation; fail closed and preserve
its receipt. No raw request body is persisted.

Creation/replacement reserves a durable operation and deterministic staging reference before
writing its secret. The handler stages the secret and commits a runnable operation before
acknowledging 202. A crash between those steps leaves a recoverable non-runnable intent; replay
with the same key can finish the write, and a worker cannot verify an absent secret. A staged
write followed by an uncertain database commit is resolved against the original operation,
never by overwriting the active generation. A failed write retains a sanitized failed operation.
The protected store and SQLite do not pretend to share an atomic transaction.

A worker verifies the staged generation using read-only provider calls. Only after successful
account discovery may one transaction publish the active generation, canonical account and
immutable profile. Replacing a credential must resolve to the original account or fail with
`account_changed`, preserving the previous verified generation and old job bindings. A new
account requires a new connection. Existing jobs retain a stable credential-slot reference;
rotation resolves that slot to the newest verified same-account generation. Superseded staged
secrets are deleted only by exact recorded references after the database outcome is known.
Restart recovery inspects pending intents without guessing which generation is active.

Disable prevents future admission but retains credentials and workers for observation, recovery
and collection of old jobs. Enable requires verified credentials and publishes a new selection.
Removal first fences new work and is rejected while any queued, active, ambiguous or retained
collection work depends on the connection. Successful removal is a tombstone plus exact vault
deletion; failure retains a retryable deletion intent and never reports completed removal.
A startup, list or status read neither verifies accounts nor creates compute authorization.

## Durable finite authorization

A managed job's local admission creates no compute permit. The explicit authorization POST
requires the exact attempt ID and a wall bound equal to the already frozen request. The server
freezes workspace, job, attempt, provider binding, canonical account, input identity, granting
token ID and time. There is at most one authorization per attempt, regardless of request keys.
The public receipt contains none of the private credential configuration.

Before the first preparation mutation, the dispatcher atomically consumes the authorization
with its fenced claim. It then uses existing write-ahead preparation/submission intents.
Consumption survives restart, sleep, lost responses and lease expiry. Restart cannot refill a
budget; changing a command-line flag cannot override a managed attempt's permit requirement.
A consumed permit authorizes only continuation of that attempt's already frozen operation;
it never creates another attempt or replays an ambiguous mutation. If the process dies after
consumption but before a remote intent, the original attempt may continue under the recorded
permit. If an intent exists, existing observational reconciliation rules decide what is safe.

Observe, reconcile and collect keep the original binding and require no new compute grant.
The explicit retry control creates a distinct attempt without a permit; `execute` authority
must grant that attempt separately. A new selected connection cannot retarget an old attempt.
Standalone explicitly configured process-budget workers remain a separate compatibility mode;
they cannot claim profiles owned by managed connections.

## Verification obligations

| Boundary | Required offline/native evidence |
|---|---|
| Contracts | Every semantic request has valid/invalid fixtures; old job examples still pass unchanged. |
| Authority | Ordinary tokens cannot manage or authorize; cross-workspace IDs remain inaccessible. |
| Replay/revision | Concurrent same/different payloads, stale selection and lost acknowledgement preserve one receipt. |
| Vault | Denial, stage/commit crashes, failed replacement, same-account rotation, canary scans and native Keychain qualification. |
| Capacity | Two distinct accounts coexist; duplicate-account connections share capacity; stale/unknown quota stays blocked. |
| Dispatch | Restart before/after consumption, lost provider response and explicit retry never duplicate compute. |
| Lifecycle | Disable preserves old recovery/collection; removal refuses dependent work; restart makes no account calls. |

Schema validation proves wire shape only. Runtime transactional/fault tests and native credential
storage qualification are independent gates; fixture providers are not live-provider evidence.
