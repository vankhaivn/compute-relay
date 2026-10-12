# ADR-0028: Operator-owned Docker hosts over SSH

Status: accepted (2026-10-12). Refines PRD-04, PRD-08 and ADR-0025. Related requirements:
PRV-01, PRV-02, PRV-03, SEC-01, MGT-01–MGT-04, SSH-01–SSH-04.

## Decision

Add a second production provider for the first release: a managed `ssh` connection runs finite
CPU attempts in Docker on a machine the operator owns, reached over SSH by LAN or VPN address.
The owner approved this scope change; it adds a provider and does not weaken the Kaggle path.

- **Credential boundary.** The connection's write-only fields hold the endpoint and exactly one
  login: a password (including macOS keyboard-interactive) or a private key with an optional
  passphrase. The whole entry lives only in the Relay-owned vault.
- **Host identity.** Verification pins the presented host key, or checks a configured SHA256
  fingerprint, into the canonical account. Every later call refuses another key before offering
  credentials, and the existing same-account rule refuses a credential replacement that changes
  the host key or login endpoint.
- **Isolation.** Each attempt owns one labelled container created from a digest-pinned standard
  image installed through an explicit management action. The resulting image ID and platform
  are frozen with the selection. The container runs as an unprivileged user with all capabilities dropped, no new
  privileges, a PID limit, no restart policy, no network unless the frozen policy allows
  internet, and optional operator CPU and memory limits.
- **Contract mapping.** Prepare creates the stopped container and stages the runner, manifest,
  bundle and inputs with a completion marker. Submit starts it exactly once: a failure before the
  start command is a rejection, and any later failure is unknown and reconciled from Docker's own
  start evidence. Observation, cancellation, artifacts, logs and cleanup read only the exact
  container named, identified and labelled for the attempt.
- **Remote data.** Cleanup deletes only that stopped container, through the provider cleanup port
  and only on an explicit [ADR-0027](0027-explicit-remote-cleanup.md) request. Nothing is deleted
  automatically.
- **Policy.** Profiles use the `operator_managed` cost class and the CPU accelerator. The host
  reports no quota, so CPU work runs with an unknown-quota warning. One account defaults to one
  active attempt; concurrency and process limits are configurable and frozen for admitted work.
  Retained aliases share the lowest account limit, and active jobs retain older lower limits.

[SSH host reference](../providers/ssh-host.md) owns the detailed mapping and
[SSH hosts](../../ssh-hosts.md) the operator procedure.

## Reason and alternatives

The operator wants compute on their own machine without manual SSH sessions or leftover
data. Running jobs directly in a remote shell leaves files, processes and network access to the
login account and has no exact remote identity. Arbitrary build inputs would let the execution
environment drift. Relay instead embeds a metadata-only Dockerfile with a pinned base digest,
records the recipe hash and freezes the verified result ID. Trust-on-first-use without pinning would let a changed host
receive the login. A Relay-side agent on the host was rejected because it adds a second
long-running service and its own credentials.

## Consequences

- Docker's daemon is trusted: a user who can run Docker can control the host. The connection
  should use an account intended for this purpose.
- The standard image has only Python's standard library; jobs install pinned wheels during setup
  when their network policy allows it.
- An explicit `install_image` action may fetch the pinned base and build the repository image.
  Its durable one-shot barrier permits only read-only recovery after uncertainty; job preparation
  cannot reinstall a missing version-2 image. Original version-1 jobs preserve their earlier policy;
  their admission aliases are retired on upgrade until recheck and installation produce a new selection.
- Connection status publishes the pinned fingerprint and verified Linux engine architecture.
- SSH-only `serve --managed` uses macOS Security.framework directly without Python or CGo.
  Existing Keychain access control still applies; changing executables may require operator trust.
- Live qualification on an operator host is separate from offline tests and local-engine runs.
