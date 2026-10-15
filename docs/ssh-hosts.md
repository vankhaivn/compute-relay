# SSH hosts

Run finite CPU jobs in Docker on a machine you own, such as a Mac on your LAN or a VPS reached by
a Tailscale address. Relay logs in over SSH, runs each attempt in its own container and deletes
remote data only when you request cleanup. It is a [managed connection](managed-connections.md)
type, so the usual connection, admission and per-attempt authorization rules apply.

## Prepare the host

- An SSH server for the account Relay uses. On macOS, enable System Settings → General →
  Sharing → Remote Login. Password and private-key logins both work; macOS password prompts
  (keyboard-interactive) are supported.
- A Docker engine that this account can use without `sudo`, such as Docker Desktop, OrbStack or
  Docker Engine on Linux. The engine must report `linux` with `amd64` or `arm64` architecture, as Docker on macOS does.
- Linux must expose a valid unique `/etc/machine-id`; macOS must expose `IOPlatformUUID`.
  These are read over authenticated SSH and hashed before being stored or returned. Cloned
  machines must initialize their own native identity.
- The selected Docker endpoint must be a local Unix socket. Docker contexts targeting remote
  TCP/SSH engines are unsupported; connect Relay to the machine owning the engine directly.
- Keep the engine running for that user. A non-interactive SSH session does not start Docker
  Desktop.
- On a host where `docker` is not in `/usr/local/bin`, `/opt/homebrew/bin` or `/usr/bin`, note its
  absolute path for the `docker_path` field.

Docker access is administrative access to the host. Use an account meant for running jobs.

Read the host key fingerprint on the host itself and put it in the `host_key_fingerprint`
field. Verification refuses any other key and publishes the pinned fingerprint in connection
status as `host.host_key_fingerprint`:

```sh
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## Create the connection

Start the managed runtime:

```sh
compute-relay serve --root /absolute/private/runtime
```

It includes SSH and Kaggle without a local Python interpreter. New installations use private file
credentials. Existing Keychain items retain their native identity; a changed executable may need
macOS access permission. The SSH descriptor appears in `GET /v1/workspaces/{w}/providers` with
type `ssh`. Every field is
write-only:

| Field | Required | Meaning |
|---|---|---|
| `host` | yes | Host name or IP address, for example a Tailscale `100.x.y.z` address. |
| `username` | yes | SSH user. |
| `password` | one login | Password for that user. |
| `private_key` | one login | OpenSSH or PEM private key; `private_key_passphrase` if it is encrypted. |
| `port` | no | Defaults to 22. |
| `host_key_fingerprint` | recommended | `SHA256:…` value. Without it, the key presented at verification is pinned unchecked. |
| `docker_path` | no | Absolute Docker CLI path. |
| `cpus`, `memory_mib` | no | Per-job limits, for example `2` and `4096`. Without them the engine's own limits apply. |

```json
{
  "provider_type": "ssh",
  "label": "Mac Intel",
  "credentials": {
    "host": "100.64.0.7",
    "username": "relay",
    "password": "REPLACE_IN_THE_SECRET_ENTRY_SURFACE"
  }
}
```

Verification logs in, checks the host key, native machine identity and Docker engine identity and
platform, and inspects the repository image. `concurrency.resource.resource_id` identifies the
shared scheduling resource across connection paths. Status returns `host.os`, `host.architecture` and `host.image`. A missing image reports
`host.image.status=missing` with no admission selection. Verification does not pull images,
build images or start containers. A later credential replacement must reach the same
host key, user, host and port, and the same native machine. A changed native identity reports
`resource_changed`; a different machine or a reinstalled host needs a new connection. Rejected logins and changed host keys both report `credential_rejected`.

## Install the execution image

Send this action with a management token, an `Idempotency-Key` and the current revision:

```http
POST /v1/workspaces/{w}/connections/{id}/actions
{"action":"install_image","expected_revision":1}
```

The worker builds the repository's [Dockerfile](../internal/provider/sshhost/image/Dockerfile)
from its pinned base digest. The build context contains only that file; the recipe has no
package installation or job code. The engine may fetch the base image from its registry.
The worker has a five-minute deadline. Poll `GET /v1/workspaces/{w}/connection-operations/{id}`
for the outcome, then reload the connection. A successful installation reports `host.image.id`
and publishes a CPU selection that freezes that exact image ID, recipe and architecture.

An interrupted build may continue in the engine. `image_installation_unknown` means its result
could not be proved; it does not mean the engine stopped. Restart and exact replay never rebuild
an uncertain operation. After checking the host, an explicit `check` can discover a completed
image. A new installation requires a new explicit action and idempotency key.

After upgrading from digest-only SSH selections, run `check`, then `install_image` as needed.
The migration retires old admission aliases while retaining their profiles and runtime policy
for jobs already accepted. Old jobs continue recovery with their original image policy.

## Configure limits and run a job

The default is one concurrent attempt per verified machine and 4096 processes per container. To set limits for future jobs, send a configuration action:

```json
{
  "action": "configure",
  "expected_revision": 2,
  "configuration": {
    "max_remote_wall_seconds": 1800,
    "max_active_attempts": 2,
    "max_processes": 256
  }
}
```

Concurrency accepts 1–64; the process limit accepts 1–65536. Omitted limits preserve saved values.
LAN IP, Tailscale IP/name, DNS aliases, ports and password/key logins reaching the same machine
share one resource budget across workspaces. Configure every retained alias to 2 to permit two
parallel attempts; an alias left at 1 keeps the shared limit at 1. Distinct login accounts remain
pinned separately, and their stricter limits still apply. An active job's older lower limit remains
effective until it releases capacity. `concurrency` reports the resource ID, configured/effective
limits and reservations; job status reports the actual queue reason. The runtime's
`--managed-max-workers` still bounds local workers.

Budget CPU, memory and processes across all concurrent containers, including other local engines
on the same machine, and within the engine/VM's limits. Raising concurrency does not guarantee
better throughput. SSH reports no provider quota. Separate Relay installations cannot coordinate
this budget; use one installation for all connections sharing the host.

After upgrading a connection that lacks resource identity, run `check` to publish a new selection.
Accepted jobs retain their original identities. Already-active legacy work conservatively blocks
new machine claims until confirmed inactive; rechecking a connection cannot locate a historical
reservation. A legacy queued job can claim after verification once those holds are resolved.
Do not clear state to bypass `resource_reservation_unresolved`. Prepared-but-not-started containers
and uncertain cancellation also retain slots. Confirmed inactivity releases execution capacity
before manual result collection.

Use the latest `selection.profile` with `"resources": {"accelerator": "cpu"}` and authorize the
attempt as usual. Jobs may include an immutable absolute `timeouts.execution_deadline` only when
the host advertises `absolute_execution_deadline`. Before start, SSH requires engine clock skew
of at most five seconds and rejects skewed or unverifiable checks. The runner observes UTC and
Linux `CLOCK_BOOTTIME`; deadlines, setup and payload supervision are described in the
[execution deadline contract](execution-gates.md#execution-deadlines). Host sleep, VM clock behavior and reboot
remain unqualified pending explicit live tests.

Each attempt gets one container named `compute-relay-cr-…`. Preparation checks
the frozen image ID, recipe, platform, machine and Docker daemon identity; it fails if they are unavailable and never installs an
image. Historical jobs admitted under the original digest-based policy retain that policy.

For `execution_control: "prepare_then_start"`, preparation checks the installed pinned image and
verifies the staged input, runtime and manifest bytes plus container policy while the container is
stopped; it does not start workload code. A missing or changed prepared resource requires a new
explicitly admitted attempt. Relay does not restage it or pull an image automatically. See
[preparation gates](execution-gates.md) for the readiness and start requests.

Hosts advertising `ephemeral_workload_secrets` can supply declared slot values after readiness.
Values are base64 in the request and held only in process memory and a private 128 KiB tmpfs. Relay
never stores them in the durable job or SQLite, or injects them through argv or environment. The
`file_and_local_process_arguments` exposure lets the authorized workload pass a value to its own
local tool arguments. Limits are eight slots, 8 KiB each, 32 KiB total and at most 24 hours,
bounded by the execution deadline. After a Relay restart before start, explicitly replay the exact
supply request; after consumption, replay cannot restore bytes and there is no automatic
reinjection. Exact-known-value log and artifact filtering is best-effort protection against
accidental exposure, not malicious encoding.
See [workload secrets](workload-secrets.md) for the request and receipt contract.

Jobs run as an unprivileged user with no network unless the connection allows internet and the
job requires it. CPU, memory and process limits are frozen with the selection. Only Python's
standard library is preinstalled; declare pinned wheels in jobs that need more and whose network
policy allows downloads. Cancellation is confirmed from Docker's state. Logs stream the payload's
standard output; standard error is collected with results.

## Host health and protection

For a verified native Intel Mac, Relay collects a bounded, read-only host snapshot and uses it to
protect new and running work. Native Intel is established with `sysctl -n hw.cputype`; a process
that reports `x86_64` under Rosetta is not enough. Read the latest snapshot, guard level,
machine-readable reasons and bounded history with
`GET /v1/workspaces/{w}/connections/{id}/health`. The response includes `schema_version`,
`resource_id`, `latest`, `guard` (`level`, `reasons`, optional `diagnostics`, `refuse_new`, `stop`
and optional `critical_since`), newest-first `history`, and the effective `policy`. Each snapshot
metric includes its status and either a typed value or a fixed reason. Connections that resolve to
the same machine share one snapshot and history, including LAN, Tailscale and alias connections.
The guard is evaluated separately for each connection using that connection's frozen policy, so
aliases with different thresholds can show different guard decisions for the same snapshot.
See the [health response schema](../api/schemas/connection-health.v1alpha1.schema.json) and
[valid response example](../api/examples/connection-health.ok.valid.json).

Each metric carries a value or an explicit `unavailable` or `unsupported` status. The inspected
native Intel source set supports these fields:

| Metrics | Status and source |
|---|---|
| Thermal level; CPU speed limit | Available from `sysctl machdep.xcpm.cpu_thermal_level` and `pmset -g therm`. The thermal level is a raw integer, not Apple's separate pressure enum. |
| CPU count, load average, current CPU use | Available from `sysctl` and `top`. |
| Memory total, used, and system-wide free percentage | Available from `sysctl`, `top` and `memory_pressure`. The categorical memory-pressure field is `unavailable`; the inspected source did not expose a bounded normal/warning/critical value. |
| GPU utilization | Available when `ioreg` exposes the allowlisted accelerator utilization scalar. It read 0% throughout the current idle baseline; other hardware may not expose it. |
| Docker CPU and memory budget | Available from `docker info` on the verified local Docker engine. |
| Docker data free space | `unavailable`; `docker system df` reports usage/reclaimable bytes, not free capacity in Docker's VM. Host filesystem free space is not a substitute. |
| Power source, battery charge and charging state | Available from `pmset -g batt`. |
| Battery temperature, cycle count and health percentage | Available from allowlisted scalar `ioreg` properties. Battery health is full-charge capacity relative to design capacity. |
| Battery condition text | `unavailable`; the inspected source did not expose a condition string. |
| Whether sleep is enabled on the current power source | Available from `pmset -g custom`. |
| CPU/GPU die temperature and fan RPM | `unavailable` unless the narrow privileged sampler succeeds. |

Linux and Apple Silicon hosts report `unsupported`; a failed or stale sample on a supported host
reports `unknown`, not `unsupported`.

Collection caps each source at 64 KiB and each command at four seconds, with a 25-second total
collection bound. It emits typed scalar values and fixed reason codes, never raw host output or
hardware identifiers. While a machine has queued, preparing or running work, Relay samples it
every 30 seconds. A health request for an idle machine samples on demand, with a 15-second shared
cache. History is limited to 120 rows and 24 hours. A sample older than 90 seconds is stale.
History survives a Relay restart, but persisted samples are not current evidence until the new
process reobserves the machine. Until then the level is `unknown`.
An idle health request can take up to 35 seconds while Relay samples; the HTTP deadline is bounded
to 60 seconds. The health route applies to SSH connections. Linux and Apple Silicon SSH hosts
return `unsupported`; other provider types have no host identity for this feature and return HTTP
400 `UNSUPPORTED_CAPABILITY` with `host health is unsupported for this provider`.

### Optional temperature and fan readings

The default configuration does not grant root access. The host can expose die temperatures and
fan RPM through Apple's `/usr/bin/powermetrics` only if the SSH account has the exact passwordless
sudo permission below. Replace `relay` with the account used by the SSH connection and add this
one line through the host owner's normal sudoers editor (for example, `visudo`):

```sudoers
relay ALL=(root) NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i 1000
```

Relay classifies the grant as `absent`, `narrow` or `broader`. It invokes only
`sudo -n /usr/bin/powermetrics --samplers smc -n 1 -i 1000`; `-n` prevents a prompt, and Relay
never sends a password to `sudo`. Without a matching grant those optional metrics remain
`unavailable`. A broader grant is reported as a diagnostic warning. To remove the grant, use the
same host-side sudoers editor and delete the exact line above; Relay never edits sudoers.

### Guard and thresholds

The current defaults are shown below; live idle and sustained-load qualification on the Intel host
is pending. Do not treat these values as qualified thermal limits:

| Threshold | Current default |
|---|---:|
| Thermal warning / critical | raw `machdep.xcpm.cpu_thermal_level` 1 / 2 |
| CPU speed warning / critical | at or below 90% / 50% |
| CPU or GPU die temperature warning / critical | at or above 90°C / 100°C |
| Low battery | at or below 10% while drawing from battery |
| Sustained critical duration | 60 seconds; configurable from 1 to 900 seconds |

Thermal levels here are raw `machdep.xcpm.cpu_thermal_level` values, not Apple's separate
thermal pressure enum. Comparisons are inclusive. Missing optional die-temperature readings do
not erase available core thermal and power readings.

Thresholds are set with the existing `configure` connection action. Include
`max_remote_wall_seconds` in the configuration object, even when leaving that value unchanged;
`host_health` is a patch, so omitted threshold fields keep their saved values. The effective
policy is frozen into each new connection revision and running attempt. Configure each alias that
reaches the same machine if it should use the same thresholds for future attempts; snapshots and
history are shared, but each connection's guard uses its own policy.

For example, this patch changes the critical duration while retaining the current wall-time limit
and all other saved host-health thresholds:

```json
{
  "action": "configure",
  "expected_revision": 3,
  "configuration": {
    "max_remote_wall_seconds": 1800,
    "host_health": {"critical_duration_seconds": 120}
  }
}
```

| Guard level | New admission, preparation and start | Existing running work |
|---|---|---|
| `ok` | Allowed under normal admission rules. | Continues. |
| `warning` | Admission returns HTTP 422 `RESOURCE_REQUIREMENT_UNSATISFIED`; inspect the health route for machine-readable reasons. A retained pre-submit attempt reports `host_unhealthy`. | Continues. |
| `unknown` | Admission returns HTTP 422 `RESOURCE_REQUIREMENT_UNSATISFIED`; inspect the health route for machine-readable reasons. A retained pre-submit attempt reports `host_unhealthy`. | Continues; health uncertainty never stops work. |
| `critical` | Refused. | Continues until critical evidence remains continuously measured for the configured duration, then Relay requests stop through its existing stop path and records `host_critical_stopped`. A confirmed battery-power reading at or below the low-battery threshold stops immediately. |
| `unsupported` | Not blocked solely because Intel-specific metrics do not apply. | Continues; watchdog installation is unsupported. |

An enabled sleep setting is reported as a diagnostic; Relay never changes it. The critical duration
uses consecutive fresh observations. A healthy observation, missing core metric, stale gap, clock
change or Relay restart resets the measured interval; Relay does not infer sustained danger from
elapsed time alone. An unknown sample never stops a running attempt. Health refusal or stop never
creates a retry, resubmits work, switches hosts or changes provider. The original attempt and stop
evidence remain available for inspection.

The admission error uses Relay's existing code and fixed message
`host health does not permit new work; inspect connection health`; it does not contain the specific
health reason. The health route is the source of that reason. When an already admitted attempt is
refused at preparation or start, its job status carries `problem.details.reason: "host_unhealthy"`.
After a critical stop is confirmed, job status carries `host_critical_stopped`.

### Host watchdog

On a supported native Intel Mac with the verified Docker engine, an explicit watchdog action
installs a user-level `launchd` agent; it does not require root. Watchdog controls have a separate
connection route and receipt type, outside the ordinary connection-operation action enum. Use a
management token and a fresh `Idempotency-Key` for each new action:

- `POST /v1/workspaces/{w}/connections/{id}/host-watchdog/actions/install_host_watchdog`
- `POST /v1/workspaces/{w}/connections/{id}/host-watchdog/actions/remove_host_watchdog`
- `GET /v1/workspaces/{w}/connections/{id}/host-watchdog`
- `GET /v1/workspaces/{w}/host-watchdog-operations/{operation_id}`

Each POST body contains only `expected_revision`. Poll the dedicated operation route for the
result; do not use the ordinary `/connection-operations/{id}` route. The initial `queued` receipt
is immutable and keyed by the idempotency key. The dedicated operation reports `queued`,
`succeeded`, `unknown` or `failed`; its `problem` is normally null, and a failed operation reports
`stale_revision` or `authority_revoked` when its queued authority became invalid before mutation.
An `unknown` result means the remote mutation could not be confirmed and is never repeated by exact
replay. After an unknown install has finished or its bounded worker lease expires, use a separate
explicit remove action to reconcile it; the old receipt remains `unknown`. The status GET uses a
read token and returns either a null status or a bounded version, last-run time and last-action
record. See the [watchdog operation schema](../api/schemas/host-watchdog-operation.v1alpha1.schema.json),
[queued receipt example](../api/examples/host-watchdog-operation.queued.valid.json),
[status schema](../api/schemas/host-watchdog-status.v1alpha1.schema.json) and
[status example](../api/examples/host-watchdog-status.idle.valid.json).

Install freezes the current critical thresholds. To change the watchdog policy, remove it and
install it again after configuring the connection. Its versioned script, launchd plist, manifest,
status and stop ledger live under one Relay-owned directory in the SSH user's home. File digests
and directory permissions are checked before launch. The agent runs in the user's GUI launchd
domain, and uses the fixed `powermetrics` invocation only when that exact grant was available at
installation.

The watchdog filters for containers carrying the complete Relay installation ownership labels on
the verified Docker engine. It stops only those exact container IDs and never stops or removes
unlabelled containers or images, or changes unrelated launch agents or host settings. Its stop
ledger records intent before requesting a stop and allows at most 64 distinct container IDs during
one installation. It
does not retry an ambiguous stop. `stop_requested` records a committed stop request;
`stop_unknown` means inactivity was not proved; `confirmed_inactive` requires an immediate
exact-ID inspection showing a stopped container. Absence from a filtered running list is not
proof of inactivity. If the ledger is full, explicitly remove and reinstall the watchdog to start
a new ledger; Relay never clears it automatically.

The watchdog cannot enforce a rule while the Mac is asleep or Docker is stopped. Those conditions
are limits, not reasons to change host settings; Relay never changes sleep or power settings or
controls fans. The watchdog refuses unsupported platforms and ambiguous installations. A
null `status` means no status file was returned; it does not prove installation is absent because
the agent may not have written its first status yet. Check the operation receipt and read status
again. An uncertain install or remove remains an operation receipt to inspect; exact replay does
not repeat its remote mutation.

## Delete remote data

Containers, their staged inputs and their outputs stay on the host after the attempt. Nothing on
the host is deleted automatically or by restarting Relay. To delete one attempt's container,
send an authenticated request with an `operate` token and an `Idempotency-Key`:

```http
POST /v1/workspaces/{w}/jobs/{job_id}/cleanup
{"attempt_id": "att_…"}
```

Preview the same route with `GET .../cleanup?attempt_id={attempt_id}` first to inspect recorded
targets, eligibility and retained reasons. The 202 receipt starts as `pending`; read it with
`GET /v1/workspaces/{w}/operations/{id}` until it reports `remote_deleted` or `already_absent`.
The attempt must be finished and its results
collected; add `"discard_uncollected": true` only to delete results you never collected. Relay
removes only the original stopped container by its full ID on the recorded engine, after matching
its creation receipt and ownership labels. It never forces removal. Host files, volumes and the
standard image stay on the host. Receipts without an original engine ID remain retained. See
[remote cleanup](remote-cleanup.md) for eligibility and failure receipts.

## Troubleshooting

| Symptom | Check |
|---|---|
| Verification `credential_rejected` | User, password or key; for a changed host, compare its fingerprint and create a new connection. |
| Verification `provider_unavailable` | Address and port reachability, Docker running for that user, `docker_path`, native machine identity and a local Unix-socket context. |
| Verification `resource_changed` | Preserve old jobs and their original host; create a separate connection for the new machine identity. |
| Job waits with `resource_reservation_unresolved` | Resolve already-active legacy work to confirmed inactivity; connection recheck alone cannot locate it. |
| No selection and `host.image.status=missing` | Request `install_image` and inspect its operation receipt. |
| `image_installation_unknown` | Check the host, registry access and non-interactive Docker credential helpers; use `check` to discover a completed image. Do not assume the remote build stopped. |
| Preparation fails after deleting an image | Restore the exact frozen image for retained jobs. Installing a new image only changes future selections. |
| Credential store unavailable | Preserve the runtime and its selected store. For file storage, check the original private file and permissions; for Keychain, grant the intended binary access to the existing items or retain the original working binary. Do not reset the runtime. |

The [Dockerfile](../internal/provider/sshhost/image/Dockerfile) pins the base digest; its SHA-256
identifies the repository recipe. The [Docker adapter](../internal/provider/sshhost/docker.go)
implements the bounded operations. [Compatibility](compatibility.md) describes qualification limits;
provider contract fixtures do not replace an authorized live host test.
