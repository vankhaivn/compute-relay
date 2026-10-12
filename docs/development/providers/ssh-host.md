# SSH host provider

The `ssh` adapter (`internal/provider/sshhost`) runs one finite CPU attempt per Docker container
on an operator-owned host. [ADR-0028](../decisions/0028-ssh-docker-host-provider.md) records the
decision and [SSH hosts](../../ssh-hosts.md) the operator procedure.

## Connection entry and host identity

The vault entry is versioned JSON written by `EncodeFields`. It holds the host, port (default 22),
user, exactly one login method, an optional `SHA256:` host-key fingerprint, an optional absolute
Docker CLI path and optional `cpus` (up to two decimals) and `memory_mib` (64–1048576) limits.
Unknown, empty or ambiguous fields are rejected, and parsing is strict.

The canonical account is `ssh-<host key hex>-<endpoint digest>`. The endpoint digest covers the
user, host and port. Every adapter call recovers the pinned key from the frozen account, refuses a
configured fingerprint that differs, re-derives the account and only then dials. The host-key
callback runs before authentication, so a changed key never receives the password or key.
`ErrHostKeyMismatch` and rejected logins verify as `credential_rejected`.

Commands are fixed argument arrays, each single-quoted. Environment values are restricted to a
safe set. macOS non-interactive sessions omit `/usr/local/bin`. The adapter therefore runs an
absolute Docker CLI, either configured or the first executable candidate, with its directory
first on `PATH` so credential helpers resolve.

## Attempt lifecycle

| Port | Docker evidence |
|---|---|
| Check / VerifyBinding | Exact frozen binding, pinned login and `docker version`. Verification also requires the engine OS to be `linux`. |
| Validate | CPU accelerator, wall bound and internet policy from the frozen profile; GPU and other unsupported required capabilities are refused. |
| Prepare | Pull the digest-pinned image if missing. Refuse an existing container name, then `container create` with the hardened specification and identity labels. Stream one tar archive into it: the root-owned runner, `manifest.json`, and the bundle and inputs, each length- and digest-checked while streamed. Write `/relay/staged.json` last. Any failure removes the container this call created. |
| ReconcilePreparation | Found only for the labelled container whose marker matches the operation, plan and container ID. |
| Submit | Inspect, then `container start` by ID. A missing or mismatched container is rejected. An already-started container, or a failure after the start command, is unknown. |
| ReconcileSubmission | Found only when Docker reports a start time. |
| Observe | `created` → queued, running → running, `exited`/`dead` → terminal. Exit 0 → succeeded, 124 (host timeout) → timed out, nonzero after a recorded cancellation request → cancelled, otherwise failed. Other states are errors, not guesses. |
| Cancel | Too late if exited. Otherwise record `/relay/cancel-requested.json`, `container stop --time 30`, and confirm only when Docker reports the container exited. |
| ListArtifacts / FetchArtifact | Exited container only. Lists `control/` runner files with measured digests and `outputs/` entries declared by the runner result. A fetch copies one regular file within the listed length and verifies its digest; links fail. |
| ReadLogs | `container logs` stdout, which the runner mirrors from the payload. Pages are complete lines, and the cursor binds the remote identity and a line offset. Standard error is collected as `control/stderr.log`. |
| Cleanup | Only the stopped, labelled container whose preparation label equals the ledger creation operation. Dry run reports `WouldDelete`; apply runs `container rm` without force. A missing container is `AlreadyAbsent`. |

The container command is `timeout -k 30 <wall+60> python3 -E -s /relay/runner/run.py`. Its
arguments are the manifest, staging root, a new work root under `/relay`, and the network mode
that matches the frozen policy. The runner enforces the job budget, and the host-side timeout
bounds a runner that stops responding.

## Capabilities and limits

Supported with offline evidence: batch execution, Python, shell, private input staging,
execution timeout, remote network control, strong identity, remote cancellation, and logs while
running and after completion. Unsupported: GPU, quota reporting, custom containers and
retained sessions.

- One attempt per canonical account at a time; capacity does not grow with credentials.
- The engine's own VM or cgroup bounds an attempt without operator limits.
- No automatic retry, cleanup, image rebuild or host substitution.
