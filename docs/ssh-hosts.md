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

Start SSH-only managed serve without Python:

```sh
compute-relay serve --root /absolute/private/runtime --managed
```

For installations that also use Kaggle, use `--managed-python` as described in
[managed connections](managed-connections.md). New installations use private file credentials.
When Keychain is selected, existing Python-created entries may require explicit macOS approval
for the Go binary before switching modes. The SSH descriptor appears in `GET /v1/workspaces/{w}/providers` with type `ssh`. Every field is
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

Verification logs in, checks the host key and Docker engine platform, and inspects the repository
image. Status returns `host.os`, `host.architecture` and `host.image`. A missing image reports
`host.image.status=missing` with no admission selection. Verification does not pull images,
build images or start containers. A later credential replacement must reach the same
host key, user, host and port. A different machine or a reinstalled host needs a new
connection. Rejected logins and changed host keys both report `credential_rejected`.

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

The default is one concurrent attempt per canonical host account and 4096 processes per
container. To set limits for future jobs, send a configuration action:

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
Aliases of the same canonical account, including across workspaces, share the lowest retained
connection limit. An active job's older, lower limit also remains effective until it releases
capacity. The runtime's `--managed-max-workers` still bounds local workers. Host identity includes
the verified key, login user, address and port; separate endpoint identities are separate accounts.

Use the latest `selection.profile` with `"resources": {"accelerator": "cpu"}` and authorize the
attempt as usual. Each attempt gets one container named `compute-relay-cr-…`. Preparation checks
the frozen image ID, recipe and platform; it fails if they are unavailable and never installs an
image. Historical jobs admitted under the original digest-based policy retain that policy.

Jobs run as an unprivileged user with no network unless the connection allows internet and the
job requires it. CPU, memory and process limits are frozen with the selection. Only Python's
standard library is preinstalled; declare pinned wheels in jobs that need more and whose network
policy allows downloads. Cancellation is confirmed from Docker's state. Logs stream the payload's
standard output; standard error is collected with results.

## Delete remote data

Containers, their staged inputs and their outputs stay on the host after the attempt. Nothing on
the host is deleted automatically or by restarting Relay. To delete one attempt's container,
send an authenticated request with an `operate` token and an `Idempotency-Key`:

```http
POST /v1/workspaces/{w}/jobs/{job_id}/cleanup
{"attempt_id": "att_…"}
```

The 202 receipt starts as `pending`; read it with `GET /v1/workspaces/{w}/operations/{id}` until it
reports `remote_deleted` or `already_absent`. The attempt must be finished and its results
collected; add `"discard_uncollected": true` only to delete results you never collected. Relay
removes only the stopped container labelled for that attempt and never forces removal. The
standard image stays on the host. See [remote cleanup](development/operations.md#remote-cleanup)
for eligibility and failure receipts.

## Troubleshooting

| Symptom | Check |
|---|---|
| Verification `credential_rejected` | User, password or key; for a changed host, compare its fingerprint and create a new connection. |
| Verification `provider_unavailable` | Address and port reachability, Docker running for that user, `docker_path`. |
| No selection and `host.image.status=missing` | Request `install_image` and inspect its operation receipt. |
| `image_installation_unknown` | Check the host, registry access and non-interactive Docker credential helpers; use `check` to discover a completed image. Do not assume the remote build stopped. |
| Preparation fails after deleting an image | Restore the exact frozen image for retained jobs. Installing a new image only changes future selections. |
| Credential store unavailable after switching modes or binaries | Grant the intended binary access to this installation's existing Keychain items, or keep the previously working mode; do not reset the runtime. |

The Dockerfile pins the base digest; its SHA256 identifies the repository recipe.
The legacy base reference remains in [docker.go](../internal/provider/sshhost/docker.go). See the [SSH host reference](development/providers/ssh-host.md)
for exact Docker behavior and [current status](status.md) for evidence.
