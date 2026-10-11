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
  Docker Engine on Linux. The engine must report a `linux` server OS, as Docker on macOS does.
- Keep the engine running for that user. A non-interactive SSH session does not start Docker
  Desktop.
- On a host where `docker` is not in `/usr/local/bin`, `/opt/homebrew/bin` or `/usr/bin`, note its
  absolute path for the `docker_path` field.

Docker access is administrative access to the host. Use an account meant for running jobs.

Optionally record the host key fingerprint on the host itself, then compare it with the one
Relay pins:

```sh
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

## Create the connection

Start managed serve as described in [managed connections](managed-connections.md). The SSH
descriptor appears in `GET /v1/workspaces/{w}/providers` with type `ssh`. Every field is
write-only:

| Field | Required | Meaning |
|---|---|---|
| `host` | yes | Host name or IP address, for example a Tailscale `100.x.y.z` address. |
| `username` | yes | SSH user. |
| `password` | one login | Password for that user. |
| `private_key` | one login | OpenSSH or PEM private key; `private_key_passphrase` if it is encrypted. |
| `port` | no | Defaults to 22. |
| `host_key_fingerprint` | no | `SHA256:…` value. Without it, the key presented at verification is pinned. |
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

Verification logs in, checks the host key and that Docker answers, and returns a CPU selection.
It does not pull images or start containers. A later credential replacement must reach the same
host key, user, host and port. A different machine or a reinstalled host needs a new
connection. Rejected logins and changed host keys both report `credential_rejected`.

## Run a job

Use `selection.profile` with `"resources": {"accelerator": "cpu"}` and authorize the attempt
as usual. Each attempt gets one container named `compute-relay-cr-…`. The first preparation on a
host pulls the Relay standard image, a digest-pinned Python 3.11 image. The job runs as an
unprivileged user with no network unless the connection allows internet and the job requires it.
Only Python's standard library is preinstalled; declare pinned wheels in the job when it needs
more and its network policy allows downloads.

One attempt runs at a time per host. Cancellation stops the container and is confirmed from
Docker's state. Logs stream the payload's standard output; standard error is collected with the
results.

## Delete remote data

Containers, their staged inputs and their outputs stay on the host after the attempt. Request
cleanup for the job through the Relay API; it deletes only that attempt's stopped container.
Nothing on the host is deleted automatically or by restarting Relay.

## Troubleshooting

| Symptom | Check |
|---|---|
| Verification `credential_rejected` | User, password or key; for a changed host, compare its fingerprint and create a new connection. |
| Verification `provider_unavailable` | Address and port reachability, Docker running for that user, `docker_path`. |
| Preparation fails on a new host | Run `docker pull` for the standard image on the host itself. Docker Desktop credential helpers can refuse non-interactive sessions. |

The standard image reference is `sshhost.StandardImage` in
[docker.go](../internal/provider/sshhost/docker.go). See the [SSH host reference](development/providers/ssh-host.md)
for exact Docker behavior and [current status](status.md) for evidence.
