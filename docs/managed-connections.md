# Managed connections

Managed mode lets a local application administer provider connections while the server is
running. The adapters are Kaggle and [SSH hosts](ssh-hosts.md). New installations store credentials
in a private local file; macOS Keychain remains optional.
Ordinary `serve` and the explicit environment-configured [Kaggle mode](kaggle-runtime.md)
remain separate modes. Managed startup does not check an account or authorize compute.

## Start and discover

Initialize an installation and workspace using [Local runtime](local-runtime.md). Before starting
serve, issue separate private tokens for ordinary job operations, connection management
(`read`, `manage`) and owner-approved execution (`execute`). Keep management and execution
tokens outside an ordinary job-producing process. Token issuance remains local administration
and requires the server to be stopped.

For SSH-only use, `compute-relay serve --root /absolute/private/runtime --managed`
uses the selected credential backend and needs no Python. It defaults to CPU and advertises only
SSH. It refuses to start if non-removed Kaggle connections are saved in that installation.

For Kaggle and SSH together, use an absolute Python executable containing the repository's
locked Kaggle client:

```sh
compute-relay serve --root /absolute/private/runtime \
  --managed-python /absolute/provider-runtime/bin/python3
```

Optional policy flags are `--managed-machine-shape cpu|NvidiaTeslaT4|NvidiaTeslaP100`,
`--managed-max-wall-seconds 1..86400`, `--managed-max-workers 1..16`, and
`--managed-allow-internet`. Defaults are T4, 1800 seconds, two workers and internet disabled.
They configure future profiles; each verified connection revision freezes its own policy.
Use `--managed-machine-shape cpu` for CPU-only execution. It sends GPU disabled and no GPU
machine shape to Kaggle. This is an explicit configuration, never a fallback from an unavailable
GPU. Changing the service flag does not rewrite saved connections or jobs; explicitly check a
connection to publish a new selection under that configuration.
Managed flags cannot be mixed with standalone Kaggle flags. No flag grants an attempt permit.
The machine shape applies only to Kaggle. SSH host connections are always CPU, and the wall and
internet flags apply to both types.

Check authenticated `/v1/info` for `managed_connections`, `connection_configuration` and
`attempt_authorization`, then
`GET /v1/workspaces/{w}/providers` for installed descriptors and protected-store availability.
Unsupported storage is reported explicitly; it never falls back to another backend. A supported
backend can still deny an actual operation. Local readiness is not provider authentication.

## Choose credential storage

The first managed start records its backend in `state/credential-store`. New installations default
to `file`; pass `--managed-credential-store keychain` to choose macOS Keychain, or
`--managed-credential-store file` to select the file backend explicitly. Later starts reuse the
recorded choice. Installations that already saved credentials with an older version retain Keychain.
An explicit different backend is rejected: this flag is not a credential migration command.

The file backend stores provider credentials and the request-fingerprint key in
`credentials/secrets.env` under the runtime root. Relay creates the directory with private Unix
permissions (`0700`), files with `0600`, or private inherited Windows ACLs. Values are base64 encoded
to preserve private keys and arbitrary bytes; **they are not encrypted**. Only filesystem access
protects this store. The local operator and processes running as that user remain trusted.

Use the connection API to add, replace and remove credentials. Do not edit or source `secrets.env`,
export it into a process environment, put it in Git or include it in a job bundle. Relay reads it as
data and updates it atomically. Passwords are never returned by the API or written to SQLite.
The file backend does not depend on an executable's name or macOS identity: stop the old process,
install the new binary and restart against the same root.

With Keychain selected, SSH-only mode uses the native macOS API; the Python mode uses its existing
bridge. Both use the same item names and bytes, disable interactive prompts while serving, and
never broaden access lists or recreate inaccessible credentials. See Apple’s
[per-app Keychain access instructions](https://support.apple.com/en-gb/guide/mac-help/kychn002/26/mac/26).

Keychain keeps its existing per-application access controls; replacing the executable or its
Python interpreter can require a new access grant. Neither backend falls back, exports credentials
to the other store, or recreates a missing fingerprint key after initialization. Preserve a damaged
installation and restore its original files or Keychain items. Automatic backend migration is not
implemented.

For a file-backed backup, stop Relay and preserve `credentials/` and `state/credential-store`
alongside the installation identity, database and both blob stores. A database-only backup does
not include credentials. Restoring only one side can invalidate recovery and request replay.

## Connection lifecycle

Render the descriptor's write-only credential fields in the application's local secret-entry
surface. Send credential JSON only in an authenticated POST body; never place it in a URL,
command argument, diagnostic output or job bundle. The Kaggle descriptor takes a token and
discovers the account itself. The application does not need to ask for a provider username.
The SSH descriptor takes the host, user and one login; see [SSH hosts](ssh-hosts.md).

`POST /connections` accepts `provider_type`, `label` and `credentials`, with an optional
`configuration` containing `max_remote_wall_seconds` from 1 through 86400. If omitted, the
managed adapter's existing startup default applies. Creation still discovers and verifies the
account asynchronously.

Under the same workspace prefix, `POST /connections/{id}/actions` accepts an `expected_revision`
plus `check`, `replace_credential`, `configure`, `disable`, `enable`, `remove`, or the SSH
`install_image` action. Only replacement
accepts credentials. Kaggle and SSH configuration also accept `max_active_attempts` (1–64,
default 1). SSH additionally accepts `max_processes` (1–65536); omission preserves saved limits. `configure` requires a `configuration`
object with `max_remote_wall_seconds`;
for example:

```json
{
  "action": "configure",
  "expected_revision": 2,
  "configuration": {
    "max_remote_wall_seconds": 7200
  }
}
```

Use a fresh non-secret idempotency key per intended operation and preserve its receipt. Configure
returns a durable `succeeded` receipt without a provider call or restart; it does not authenticate
or enable a connection, and it is rejected while another operation is pending. When a selection
exists, it publishes a new immutable profile for future admissions; existing jobs, attempts and
grants keep their original values. Provider session and capacity limits still apply independently.

A newly verified connection returns an exact `selection.profile` and `selection.accelerator`.
Use both in the existing job specification; admission rejects a different resource. Legacy
profiles omit the accelerator; request a connection check rather than inferring it. A later
connection change can invalidate an old selection for new admission;
refresh explicitly after a conflict. Previously admitted jobs keep their frozen provider/account
binding. Credential replacement must verify the same account; a different account needs a new
connection. Two credentials for one account share capacity and do not create extra quota.

Disable stops new admission while preserving old recovery and collection. Removal refuses
dependent queued, active, uncertain or retained collection work, then deletes exact protected
items before recording a tombstone. Removed IDs remain readable but disappear from the active
list. A failed storage operation leaves a recoverable intent, not a reported success.

## Configure and observe concurrency

Read `capabilities.concurrency` in the provider descriptor before offering a setting. It contains
`support`, `scope`, `minimum`, `maximum` and `default`. Kaggle uses `scope=account`; SSH uses
`scope=resource`. Omission means support is unknown. The range describes Relay's local policy,
not the number of sessions the provider will allocate. Set `configuration.max_active_attempts`
through the same create/configure actions, then use the returned immutable selection for new jobs.
Explicit Kaggle mode remains one worker and one active attempt per account.

Connection `concurrency` reports:

| Field | Meaning |
|---|---|
| `configured_limit` | This connection's selected limit, default 1. |
| `effective_limit` | Intersection of current shared policy, retained aliases, active older limits and the runtime ceiling; null when unknown. |
| `resource` | Verified opaque `{kind, resource_id}` for a shared machine, or null. Compare this ID across connections; never infer grouping from labels or addresses. |
| `active_attempts` | Claimed preparation, prepared holds and possibly active executions in the scheduling scope; null when its extent is unknown. |
| `wait_reason` | Current local capacity gate, or null. It does not decide a particular job's authorization, quota or FIFO position. |

A limit of 2 permits two independently authorized attempts to claim when workers and provider
capacity allow it. A third waits. Retained aliases across workspaces share the lowest applicable
limit, including disabled aliases; removing an alias is explicit and guarded against active work.
Reducing the limit does not terminate existing attempts. Increasing it does not widen an active
job's older lower limit. The top-level legacy `active_attempts` also counts unconsumed grants;
use `concurrency.active_attempts` to display execution reservations.

For queued or locally blocked managed jobs, job status includes `scheduling.reason`. Common values
are `execution_authorization_required`, `workspace_fifo`, `worker_limit`, `account_capacity`,
`resource_capacity`, `resource_identity_unverified` and `resource_reservation_unresolved`.
`eligible` means the current local gates allow selection, not that compute has started.
Unknown or stale GPU quota can impose a separate wait. These reads are local and never contact
providers, load credentials or authorize work. Missing fields on older runtimes mean unknown.

Prepared holds and uncertain remote activity occupy capacity until resolved. Confirmed remote
inactivity frees an execution slot even when manual result collection is pending. A local timeout,
expired worker lease or cancellation request cannot free a possibly active remote attempt.
`--managed-max-workers` bounds local workers independently of the remote execution budget.

## Read quota details

SSH returns an empty `quotas` array; its host limits are in `concurrency`.
Connection quota entries expose optional `limit`, `used`, `local_reserved` and `reset_at`.
All durations are seconds: total allowance is rounded down and provider usage up.
`remaining` retains its conservative meaning after provider and local reservations;
`local_reserved` reports only Relay's finite GPU reservations, not provider usage.
These figures are account-scoped and do not grant compute. Read responses make no provider calls.
Older servers may omit every new field; missing values are unknown, not zero or unlimited.
Stale observations may retain total/usage metadata but never expose fresh remaining capacity.
`reset_at` is returned only from provider evidence. The current Kaggle adapter does not report
an authoritative reset timestamp, so clients must not infer one from weekdays or elapsed time.

## Authorize one attempt

Upload immutable input objects, admit the job and retain its explicit attempt ID. Admission
does not authorize provider staging or execution. After an owner approves that exact job, send
`POST /jobs/{job_id}/authorize` using the separate execute token and an idempotency key:

```json
{
  "attempt_id": "att_EXAMPLE_REPLACE_WITH_RECEIPT",
  "max_remote_wall_seconds": 300,
  "authorize_private_staging": true,
  "authorize_compute": true
}
```

The wall bound must equal the frozen job request. New GPU work requires known, fresh, sufficient
cached quota after existing reservations. Unknown or stale GPU quota requires an explicit
connection check. Explicit CPU work does not require or reserve GPU seconds. Its unquantified
CPU allowance remains unknown and produces a scheduling warning; known CPU exhaustion still
blocks execution. All resources share the same account concurrency limit and finite consent.
Unresolved HTTPS inputs are not enabled here; there is no account fallback or rotation.

The permit is consumed durably before provider mutations. Restarting or changing startup flags
cannot create another permit. Recovery and collection use the original binding. An explicit
retry creates a new attempt that needs its own authorization. Current authority, capacity and
one-shot intent guards still apply after a permit is granted.

See the [wire and recovery contract](development/managed-connections.md) and
[OpenAPI](../api/openapi.json) for exact schemas, revisions and error semantics, and
[current status](status.md) for live evidence.
