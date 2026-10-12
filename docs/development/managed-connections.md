# Managed connections and execution authorization

The extension contract for a locally managed companion. The `--managed` and `--managed-python` serve
compositions advertise these routes and the `managed_connections`, `connection_configuration`
and `attempt_authorization` features; clients check the features they use instead of assuming
support from the API version. A saved connection is not proof of authentication, capacity or a
runnable provider.

## Authority and API

All routes use the existing authenticated literal-loopback transport, workspace isolation,
strict JSON parsing, request limits and sanitized error envelope. No provider work occurs in
an HTTP handler.

| Method and workspace-relative path | Scope | Semantics |
|---|---|---|
| `GET /providers` | read | Installed adapter descriptors and credential-store availability; local only. |
| `GET /connections` | read | At most 100 non-removed, sanitized connections; local only. |
| `GET /connections/{id}` | read | Current revision, availability, immutable selection and cached quota. |
| `POST /connections` | manage | Label, provider type and write-only credential fields; optional wall-time override; asynchronous account discovery. |
| `POST /connections/{id}/actions` | manage | Explicit check, credential replacement, configure, disable, enable, removal or image installation. |
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

Creation may include `configuration.max_remote_wall_seconds` (1–86400); omission keeps the
startup default. `"action":"configure"` requires `expected_revision` and that `configuration`
object; other actions reject `configuration`, and only credential replacement accepts credentials.
SSH configuration also accepts `max_active_attempts` (1–64) and `max_processes` (1–65536);
providers that cannot enforce these reject them. Omitted optional limits preserve saved values.
Configure commits the new policy, revision and a terminal `succeeded` receipt in one transaction,
without a provider call, authentication, enablement or restart. It is rejected while another
operation is pending, and an unverified connection still has no admission selection.

## Descriptors, verification and immutable selection

A descriptor defines credential field names, labels, required/write-only status and byte
bounds, plus capability support. Consumers render these definitions without importing provider
SDKs. The adapter rejects fields it does not declare. The initial Kaggle descriptor accepts an
API token and discovers the canonical account through authenticated read-only introspection;
the caller does not supply an account name. The SSH host descriptor accepts the endpoint and one
login; its canonical account pins the verified host key and login endpoint, so a replacement
that reaches another host fails as `account_changed`. Discovery, quota inspection and credential-store
availability are separate facts. No check allocates compute or creates provider resources.

Every accepted mutation reserves a new monotonic connection revision and gates new admission
until its outcome is known. A verified connection with all required runtime assets available publishes a unique immutable
profile name for that revision, returned as `selection.profile`. Existing job JSON remains
unchanged: the caller copies this exact name into `profile`. The generated alias is never
reused or repointed. Publishing a later revision disables the earlier admission alias in the
same transaction; previously admitted jobs retain their frozen original profile/account.
Admission checks alias eligibility transactionally, so a request using a stale selection
cannot race a connection change. An exact admission replay still returns its original receipt.
The published profile also freezes its adapter runtime configuration, including machine shape or installed image ID, platform and container limits;
changing startup defaults cannot reinterpret an existing job. Removed connections remain
addressable by ID as sanitized tombstones and are omitted from the bounded active list.

The wall-time override is persisted separately from the frozen profile and survives every other
action. Configure publishes a new immutable selection for future admissions when the connection
has one; existing jobs, attempts and grants keep their original profile and bound. The connection
projection returns the optional `configuration` object, omitted when the startup default applies.
Provider session and capacity limits still apply independently. SSH defaults to one concurrent
attempt and 4096 processes per container. Retained aliases share their lowest configured
capacity limit; active attempts also retain older, lower frozen limits.

The account ID is an opaque Relay identity, not a credential or provider username. Provider
and canonical account together define the shared quota/capacity key. Two connections for the
same account do not create two slots or two quota balances. Quota projections carry observation
time and a conservative lower bound after reservations. Unknown/stale quota has null remaining
capacity, never a manufactured number. The managed default blocks new GPU authorization when
quota is unknown, stale or insufficient; cached availability alone never guarantees dispatch.
There is no account rotation, fallback, pooling or automated selection.

## Explicit image installation

An adapter can expose image installation after read-only account verification. The SSH adapter
publishes `host.host_key_fingerprint`, `host.os`, `host.architecture` and `host.image` as the last
successful evidence. A failed recheck retains that evidence; it does not prove current reachability.
Image evidence includes the recipe SHA256 and, when installed, its immutable ID. A missing image
leaves `selection` null.

`install_image` requires manage scope, the expected connection revision and an idempotency key.
Acceptance freezes the recipe and architecture. Before sending a build, the worker durably
consumes the operation's single installation permit. Recovery only inspects the exact operation
image; an absent or unreadable result is `image_installation_unknown`, with no automatic rebuild.
An explicit later `check` can observe a completed image. A worker deadline is not evidence that
the engine has stopped. This action never creates a job execution permit.

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
never by overwriting the active generation. A failed write retains a sanitized operation.
Missing-secret intents stay recoverable by exact request replay; background recovery defers them
without inventing a secret or expiring the key.
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
The managed composition currently requires uploaded immutable objects. Unresolved HTTPS input
sources are rejected at authorization; it never silently downloads or changes the frozen inputs.

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
