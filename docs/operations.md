# Operations

How to run, administer, back up, upgrade and recover a Relay installation.

## Serve

```sh
./compute-relay serve --root /abs/private/runtime --listen 127.0.0.1:7331
```

| Flag | Values | Default |
|---|---|---|
| `--listen` | Literal loopback address; port 0 picks a free port | `127.0.0.1:7331` |
| `--managed-machine-shape` | `cpu`, `NvidiaTeslaT4`, `NvidiaTeslaP100` (Kaggle only; SSH is always CPU) | `cpu` |
| `--managed-max-wall-seconds` | 1–86400; an SSH connection can raise its own limit to 604800 | 1800 |
| `--managed-max-workers` | 1–16 local workers | 2 |
| `--managed-allow-internet` | Allows jobs that request remote internet | off |
| `--managed-credential-store` | `file` or `keychain`; see [credentials](#credentials) | `file` for new installations |

Flags only shape *future* connection selections; admitted jobs keep their values, and no flag
authorizes compute. With no connections, Relay makes no provider calls. DNS names, wildcard and
remote listeners are rejected; there is no TLS or remote-exposure option.

To upgrade, stop `serve`, replace the binary and start it on the same root. There is no installer.

Shutdown (Ctrl-C / SIGTERM) gives handlers ten seconds, then closes connections and waits for
workers. Stopping Relay never cancels remote work.

Provider and transport failures become attempt or operation evidence and never stop `serve`. A
corrupted or inconsistent store does: `serve` exits 1 and prints one line naming the worker and a
closed error class, for example `Runtime stopped: service=dispatch class=state_corrupt.`

## Local administration

These commands take the installation lock, so stop `serve` first:

```sh
./compute-relay state --root /abs/private/runtime
./compute-relay workspace show|enable|disable --root /abs/private/runtime --id app
./compute-relay token issue --root /abs/private/runtime --workspace app --scope read --ttl 24h --output /abs/private/new-token
./compute-relay token revoke --root /abs/private/runtime --id tok_FROM_RECEIPT
```

Opening the state applies pending database migrations, even for `state`.

### Tokens

| Scope | Allows |
|---|---|
| `read` | Job status, logs, artifacts, connections, readiness, health, storage view |
| `write` | Upload objects, validate and admit jobs |
| `operate` | Cancel, retry, reconcile, collect, cleanup (and its preview) |
| `manage` | Create and change connections, host watchdog, orphan cleanup |
| `execute` | Authorize attempts, prepare/start grants, workload secrets |

TTL is 1 minute to 30 days (default 24 h). Revoking a token or disabling a workspace blocks future
requests; it does not cancel remote work. If writing the token file fails, Relay tries to revoke the
token and reports `revoked-after-delivery-error` or `revocation-unconfirmed`.

## Credentials

The first `serve` records the vault choice in `state/credential-store`; later starts reuse it.
Passing a different backend is refused — the flag is not a migration tool.

| Backend | Where | Notes |
|---|---|---|
| `file` (default) | `credentials/secrets.env` under the root, `0700`/`0600` | Base64, **not encrypted**; protected only by file permissions. Do not edit, source or copy it. |
| `keychain` | macOS Keychain (native API) | A replaced binary may need macOS access permission for the existing items. Relay never prompts, widens access or recreates items. |

Relay never falls back to the other backend or recreates a missing credential. If access fails, keep
the original binary and vault and fix access. Credentials are added, replaced and removed only
through the [connection API](connections.md); they never appear in API responses or SQLite.

## Backup and restore

There is no backup command. Back up a **stopped** installation as a whole:

1. Stop Relay and wait for the lock to release.
2. Copy the entire runtime root: SQLite and its WAL/sidecars, both blob stores,
   `.compute-relay-runtime`, the `.retention-id` and format markers, `state/credential-store` and
   `credentials/` (file vault). Keychain items stay in the Keychain. Keep the matching binary.
3. Restore all of it together into a private location. Do not `init` over it, edit markers or
   force it open with SQL.
4. Check restored tokens and grants against newer revocations. Never run the original and the
   restored copy at the same time against the same provider account or host.

A live copy of the database file can miss committed WAL data. A database-only backup lacks blobs and
credentials. A backup does not stop remote work or bring back deleted bytes.

Requirements: Linux or macOS with normal Unix permissions, a dedicated private directory, local
disk. Network filesystems and arbitrary power loss are not qualified.

## Upgrades

Migrations are numbered and checksummed. Newer, foreign, edited or gapped state is refused, and
downgrade is unsupported. Keep a stopped backup before upgrading. Schema v14 and v22 roots are
tested to upgrade with their jobs, receipts, connections and artifacts intact.

## Local retention

The input store holds at most 32 GiB and the result store 8 GiB; an upload that would exceed its
store fails with `disk_limit_exceeded` until retention frees space.

| Data | Minimum age before deletion |
|---|---|
| Unreferenced input objects | 24 hours |
| Verified results and safe completed references | 7 days |

Anything still in use (active or unknown execution, pending collection or operations, shared
references) is kept regardless of age. Expiry writes a tombstone first; history and metadata stay,
and new downloads return HTTP 410. The sweep runs at startup and hourly. Local retention never
deletes remote data — see [remote retention](results-and-cleanup.md#automatic-remote-retention).
An explicit [consumer release](results-and-cleanup.md#release-local-results) can waive the age
window for an exact verified result. It keeps the same active-work guards and durable tombstones;
the hourly sweep also recovers any pending file deletion from that release.
Exact collected attempts can also [release their inputs](results-and-cleanup.md#release-local-inputs)
through preview and confirmation, retaining every shared-reference and active-work guard.
A cancelled, inactive attempt whose explicit uncollected discard completed with all remote
resource deletions/absences confirmed no longer pins inputs for result recovery. Its seven-day
input window starts at that cleanup completion; its historical attention/result state remains.
The same input-only rule covers a terminal preparation that never submitted compute once exact
staging cleanup is confirmed. It preserves pending readiness and the failed history; discovery
alone, failed cleanup and unknown cleanup keep inputs pinned.

## Local storage inventory

`GET /v1/workspaces/{w}/local-storage` requires read scope and the `local_storage` capability.
It returns `inputs_bytes`, `results_bytes`, `database_bytes`, `other_bytes`, `total_bytes` and
`measured_at`. Sizes are logical regular-file bytes under this Relay installation; database bytes
include WAL/SHM files, other bytes include remaining state/markers, and symlinks are not followed.
No paths, file contents or credentials are exposed. The snapshot is cached for one minute;
`?refresh=true` forces a fresh measurement. A disappearing file during measurement is omitted;
other filesystem errors return 503 without a partial total. Applications must use this API rather
than walking or deleting Relay's runtime directories.


## Recovery

First rule: keep the original receipt, idempotency key, attempt ID, runtime root and binary. Never
reset state, delete rows, clear capacity or submit a replacement because a response went missing.
These are separate facts — one does not imply another:

```mermaid
flowchart LR
  a[202 receipt] --> b[Remote execution] --> c[Results verified and published] --> d[Remote data cleaned up]
  b --> e[Hardware released]
```

| Situation | What to do |
|---|---|
| Lost response to a request | Resend the identical request with the **same** key; then read state. |
| Error before Relay recorded intent | Nothing external happened; check state, then retry the local step. |
| Remote call uncertain after intent was recorded | Relay observes the original resource only. Wait; do not resubmit. |
| Temporary fault before staging intent | Relay defers the original queued attempt for up to ten minutes, starting at a 15-second backoff floor. Permanent input or identity faults still stop it. |
| Host health warning or unknown | New preparation and execution wait for safe observations; the original queued attempt is retained. Running work follows the separate critical-health guard. |
| Staging stalled before submission | `POST /jobs/{id}/reconcile` observes the original staging. Without a confirmed resource, Relay observes for a durable ten-minute grace from the first preparation fault, then ends with `staging_unconfirmed` if still unresolved. Crash recovery resumes reads; a new attempt requires explicit intent. Confirmed pending staging stays distinct. |
| Submission rejected before reaching the provider | Read `problem.details.reason`, fix the cause, retry explicitly. |
| Temporary observation outage after confirmed submission | Relay keeps observing the same run with backoff, including after restart. No resubmission. |
| Remote state active or unknown | Capacity stays reserved. Do not delete; reconcile if the attempt requires attention. |
| Cancellation ended before its provider stop invocation began | Restore connectivity or credentials, then explicitly cancel the same attempt with a new key. Relay preserves the old receipt and verifies the frozen identity again. |
| Cancellation may already have invoked the provider | Reconcile the original attempt. A fresh cancel key cannot grant another stop invocation. |
| Cancellation unsupported or target missing | Termination stays manual-required/unknown. |
| Finished, results missing, or `collection.state=awaiting_request` | `POST /jobs/{id}/collect` for the same attempt — not a compute retry. |
| Collection failed and committed | One new collect request with a new key. |
| Download failed after writing a local file | Inspect that file and the request; never overwrite it automatically. |
| Results expired | History stays. Do not rerun compute to get them back. |
| Identity, version or root mismatch | Stop and keep evidence; do not adopt a replacement or edit markers. |

An orderly stop drains a preparation whose intent was already committed, including its lease
heartbeat, within that preparation's existing finite timeout, followed by at most five seconds
of best-effort SSH staging cleanup when needed. It does not proceed to Start.
A hard process kill cannot drain; recovery observes the recorded resource without recreating it.
Provider transfer retries may refill the same verified staging target, but never repeat resource
creation or compute submission. An identity mismatch is not proof that old compute stopped, so
its capacity remains reserved while remote activity is unknown.

An exact container-not-found observation on the same verified SSH host and Docker engine is
different: Relay records the frozen remote reference and observation time, leaves the final
execution outcome unknown, and marks remote activity inactive. The attempt needs attention but
no longer holds compute capacity. An engine change, unreachable host or Kaggle permission error
does not establish this absence.

HTTP 429 responses advertise safe operation retry and a cooldown. HTTP 503 responses include
`Retry-After: 5`; the error's retry hint still determines whether a mutation can be retried.
A route deadline reports `REQUEST_TIMEOUT`, not an input-download failure. Preserve the original
request key and inspect state after a timeout; the deadline alone cannot prove a mutation failed.
An unexpected handler panic reports `RUNTIME_UNAVAILABLE` without exposing private error text.

For a bug, keep raw state and logs private. Report the binary revision, exact command, actual
result, possible remote effects and a minimal sanitized reproduction.
