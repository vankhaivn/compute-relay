# SSH hosts

Run finite CPU jobs in Docker on a machine you own — a Mac on your LAN, a VPS over Tailscale.
Relay logs in over SSH and runs each attempt in its own container. Related:
[storage and cache](ssh-storage.md), [host health](ssh-host-health.md),
[prepare and start](prepare-start.md).

## Prepare the host

- SSH server for the account Relay uses (macOS: System Settings → General → Sharing → Remote
  Login). Password, keyboard-interactive and private-key logins work.
- Docker usable by that account without `sudo` (Docker Desktop, OrbStack, Docker Engine), reporting
  `linux/amd64` or `linux/arm64`, on a **local Unix socket** (remote Docker contexts are refused).
  Keep it running: an SSH session does not start Docker Desktop.
- A unique native machine ID (`/etc/machine-id` on Linux, `IOPlatformUUID` on macOS). Cloned
  machines must regenerate it.
- Note the absolute `docker` path if it is not in `/usr/local/bin`, `/opt/homebrew/bin` or `/usr/bin`.

Docker access equals admin access to the host; use an account meant for jobs. Read the host key
fingerprint on the host:

```sh
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## Create the connection

`POST /v1/workspaces/{w}/connections` with `provider_type: "ssh"`. All fields are write-only:

| Field | Required | Meaning |
|---|---|---|
| `host` | yes | Host name or IP, for example a Tailscale `100.x.y.z` address |
| `username` | yes | SSH user |
| `password` or `private_key` | one | `private_key_passphrase` if the key is encrypted |
| `port` | no | Default 22 |
| `host_key_fingerprint` | recommended | `SHA256:…`; without it the key seen at first verification is pinned |
| `docker_path` | no | Absolute Docker CLI path |
| `cpus`, `memory_mib` | no | Per-container limits, for example `2` and `4096` |

```json
{
  "provider_type": "ssh",
  "label": "Mac Intel",
  "credentials": {"host": "100.64.0.7", "username": "relay", "password": "ENTERED_IN_A_SECRET_FIELD"}
}
```

Verification checks the host key, machine ID, Docker engine ID and platform, and reports `host.os`,
`host.architecture` and `host.image`. It never pulls, builds or starts anything. A later credential
replacement must reach the same host key, user, address, port and machine.

## Install the execution image

```http
POST /v1/workspaces/{w}/connections/{id}/actions
{"action": "install_image", "expected_revision": 1}
```

Relay builds the repository's [Dockerfile](../internal/provider/sshhost/image/Dockerfile) (pinned
Python 3.11 base, no extra packages) within five minutes. Poll
`GET /v1/workspaces/{w}/connection-operations/{id}`; success reports `host.image.id` and publishes a
CPU selection pinned to that image. `image_installation_unknown` means the build may still be
running in Docker; check the host, then use `check` to pick up a finished image. Deleting an image
that admitted jobs still use breaks their preparation.

## Limits and capacity

Defaults: one attempt at a time per machine, 4096 processes per container. Change them with
`configure` (`max_active_attempts` 1–64, `max_processes` 1–65536; see [Connections](connections.md)).

Every connection that reaches the same machine — LAN IP, Tailscale address, DNS alias, different
login — shares one capacity budget, and the **lowest** limit wins. To run two attempts in parallel,
set every retained connection to that machine to 2. Use one Relay installation per host: separate
installations cannot coordinate.

Budget CPU, memory and processes across all parallel containers and the Docker VM's limits; more
parallelism does not guarantee more throughput. Jobs run as an unprivileged user, without network
unless allowed, with only the Python standard library preinstalled. Logs stream stdout; stderr
arrives with the results.

## Long attempts

An SSH connection can allow `max_remote_wall_seconds` up to 604800 (7 days); the `serve` default
stays at most 86400. Budgets are finite: Relay never extends or resubmits.

| Concern | During a multi-day attempt |
|---|---|
| Workload secrets (≤24 h) | Bound only the start; delivered files stay. |
| Observation | Every ~15 s (backing off to 120 s). Five failures in a row (≈7–12 min outage) mark the attempt `needs_attention`: it keeps running and holding capacity, but observation stops until `POST /reconcile`. |
| Capacity | Held until confirmed inactive, regardless of age. |
| Inputs, cache, images | Pinned while the attempt is not finished. |
| Logs | First 20 MiB per stream kept, 16 MiB mirrored to Docker logs. Keep output sparse. |
| Application tokens | Must outlive the attempt if your app polls (max TTL 30 days). |

## Remote data

Containers with their staged inputs and outputs stay on the host until
[cleanup](results-and-cleanup.md#cleanup) or a [retention rule](results-and-cleanup.md#automatic-remote-retention)
removes them. Restarting Relay deletes nothing.

## Troubleshooting

| Symptom | Check |
|---|---|
| `credential_rejected` | User, password or key. A changed host key also reports this: verify the fingerprint and create a new connection. |
| `provider_unavailable` | Reachability, Docker running for that user, `docker_path`, machine ID, local socket context. |
| `resource_changed` | Different machine. Keep old jobs; create a new connection for the new machine. |
| No selection, `host.image.status=missing` | Run `install_image`. |
| `image_installation_unknown` | Check the host, registry access and Docker credential helpers; then `check`. |
| Job waits on `resource_reservation_unresolved` | Older active work must be confirmed inactive first. Do not clear state. |
| Preparation fails after deleting an image | Restore the exact image; a new image only affects future selections. |
| Credential store unavailable | Keep the runtime and vault; fix file permissions or Keychain access. Do not reset. |

Job preflight verification reports provider-neutral problem codes separately from connection
check reasons above:

| Problem code | Meaning |
|---|---|
| `PROVIDER_UNREACHABLE` | SSH transport could not reach the pinned host. |
| `PROVIDER_AUTH_FAILED` | SSH credentials were refused or could not be read from the selected vault. |
| `REMOTE_IDENTITY_MISMATCH` | Frozen binding, host key, or confirmed machine/Docker engine identity differs. Intervention is required. |
| `PROVIDER_STATE_UNKNOWN` | Docker or identity reads could not establish remote state; this is not proof of a changed identity. |

Network and Docker outages do not establish an authentication failure. Unknown state retains
capacity; only confirmed inactivity releases compute capacity, independently of remote cleanup.
