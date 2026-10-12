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

Machine capacity is separate from the pinned login account. Native Linux machine ID or macOS
platform UUID is read through SSH and converted to an application-specific HMAC-SHA256 identity;
raw IDs never enter durable or public state. Local Unix-socket Docker endpoints are resolved and
pinned in memory for the operation, with `DOCKER_CONTEXT`/`DOCKER_HOST` cleared for subsequent
commands. Remote Docker contexts are refused. A derived daemon ID freezes the actual engine,
while all local engines on the native host share its resource budget. Changed machine identity
fails connection recheck as `resource_changed`; changed machine/daemon identity cannot retarget
an accepted attempt. See [ADR-0033](../decisions/0033-shared-scheduling-resource-concurrency.md).

## Image installation

The embedded [Dockerfile](../../../internal/provider/sshhost/image/Dockerfile) pins its base digest
and contains only image metadata. Its content hash is the recipe identity. Verification inspects
a recipe-specific tag and publishes the fingerprint, engine platform and image status; a missing
image cannot produce a new selection.

`install_image` freezes the recipe and architecture in its durable operation. The worker commits
an `installing` barrier before sending one build with a Dockerfile-only tar context, fixed labels,
fixed tags, `--pull=false`, `--network=none` and an explicit platform. `--network=none` governs
build steps; fetching a missing base still needs registry access. The worker deadline is five
minutes. A lost outcome only permits inspection of that operation's tag and labels. Absence
remains `image_installation_unknown`, never permission to build again. An explicit `check` can
later discover the recipe image without mutation.

Version-3 runtime policy freezes the resulting image ID, recipe SHA256, engine architecture,
CPU/memory bounds and derived machine/daemon IDs. Versions 1 and 2 remain readable for historical
jobs. Migration retires legacy admission aliases without rewriting frozen profiles; recheck and
installation publish version-3 selections. An unresolved active legacy attempt keeps a conservative
machine reservation until confirmed inactive, even after metadata recheck. A legacy queued attempt
records its resource at the first verified post-upgrade claim. Recovery continues against original
identities. Changing the recipe, credential or current Docker context never reinterprets a frozen job.
Docker images and build cache are retained; attempt cleanup removes only its container.

The build-context and image-inspection contracts follow the Docker documentation reviewed
2026-10-12: [tar contexts](https://docs.docker.com/build/concepts/context/),
[build options](https://docs.docker.com/reference/cli/docker/image/build/) and
[image inspection](https://docs.docker.com/reference/cli/docker/image/inspect/).

## Attempt lifecycle

| Port | Docker evidence |
|---|---|
| Check / VerifyBinding | Exact frozen binding, pinned login and `docker version`. Connection verification records a `linux` engine with `amd64` or `arm64` architecture. |
| Validate | CPU accelerator, wall bound and internet policy from the frozen profile; GPU and other unsupported required capabilities are refused. |
| Prepare | Require the frozen installed image ID, recipe and architecture; never install. Legacy version-1 profiles retain digest-based pulling. Refuse an existing container name, then `container create` with the hardened specification and identity labels. Stream one tar archive into it: the root-owned runner, `manifest.json`, and the bundle and inputs, each length- and digest-checked while streamed. Write `/relay/staged.json` last. Any failure removes the container this call created. |
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

- One attempt per machine by default; configurable up to 64, with stricter account limits retained.
  All retained machine aliases share the lowest limit across workspaces; active attempts retain
  older lower limits. Local worker occupancy is a separate preparation gate.
- Process limits default to 4096 and accept 1–65536. CPU, memory and process limits are frozen.
- Budget the sum of per-container limits across concurrent work against host and engine/VM
  capacity. Without operator limits, only the engine's VM or cgroup bounds apply.
- No automatic retry, cleanup, image rebuild or host substitution.
