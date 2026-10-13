# ADR-0033: Shared concurrency for verified scheduling resources

Status: proposed. Refines the capacity scope in ADR-0025 and ADR-0028. Related requirements:
OPS-01, DUR-03, MGT-03, SSH-01–SSH-04.

## Decision

Reuse the managed connection's `max_active_attempts` and immutable profiles. Kaggle and SSH
support local limits from 1 through 64, defaulting to 1. Provider descriptors advertise the
supported range and scope. A connection reports its configured limit, effective shared limit,
execution reservations and capacity-wait reason; queued jobs report their scheduling reason.
These are local policy observations, not upstream session entitlements, free quota or a compute
permit. Explicit Kaggle serve remains serial.

Preserve the pinned SSH login account, and separately associate it with one verified machine
resource. Read Linux `/etc/machine-id` or macOS `IOPlatformUUID` through authenticated SSH.
Normalize and derive an HMAC-SHA256 identifier with a fixed application namespace and OS-specific
input tag. Persist only the derived identity. Login names, addresses, ports, credentials and
host keys do not determine resource equality. The association is durable and cannot move to
another resource; a changed native identity requires a new connection.

All retained connections to that resource, across workspaces and local Docker engines, share
the most restrictive configured limit. An active attempt's older lower frozen limit remains
applicable. Account policy and the runtime ceiling are additional bounds. Local worker occupancy
is a separate preparation gate. Prepared holds, unconfirmed submission and uncertain cancellation
retain reservations after lease expiry. Confirmed remote inactivity releases execution capacity
before manual result collection; a cancellation request alone does not.

New SSH runtime policies also freeze a derived Docker daemon ID. Resolve only local Unix-socket
contexts, then pin that socket in memory for every command in the operation. Verify machine and
daemon before touching the attempt. A remote TCP/SSH Docker context cannot prove that the engine
belongs to the SSH machine and is refused. Neither changing Docker context nor checking a new
selection retargets an accepted attempt. Socket paths and native IDs are not public or durable
configuration.

## Reason and alternatives

An endpoint-derived account correctly pins authentication but gives LAN, Tailscale, DNS and
alternate login paths different account IDs. Treating those IDs as capacity creates extra slots
on one machine. Labels and copied host keys also cannot establish machine identity. Daemon ID
alone is insufficient: separate local engines on one host still compete for the host's resources.
Native machine identity provides the shared budget while the daemon identity preserves the
execution destination. This relies on trusted operator hosts maintaining unique native IDs;
cloned OS images must initialize a unique ID before use. There is no remote resident agent,
operator-supplied arbitrary grouping label or automatic capacity probe.

## Consequences

- Upgrade retires legacy SSH admission aliases and records unresolved account associations.
  Recheck publishes a new selection without rewriting old jobs, profiles, credentials or grants.
  An already-held legacy attempt has no historical resource receipt: even a successful metadata
  recheck cannot locate it, so it conservatively blocks new machine claims until confirmed inactive.
  A legacy queued job can claim after verification and resolution of those holds; its resource
  receipt is written before preparation. Original recovery identities remain unchanged.
- Reducing a limit or adding a restrictive alias gates future claims; it never kills accepted work.
  Disabled aliases remain restrictive until explicitly removed. Separate runtime installations do
  not coordinate capacity; use one installation for connections that share a resource budget.
- Per-container CPU, memory and process limits remain frozen. Operators must budget their aggregate
  across concurrent containers and the engine/VM; a larger concurrency limit promises no speedup.
- SSH exposes no provider quota. Kaggle GPU quota checks and finite per-attempt authorization remain
  separate from account concurrency. No retry, provider substitution or paid fallback is added.
- Offline migration, adapter and scheduling evidence does not qualify parallel live workloads.
  Bounded Kaggle and Intel-host LAN/Tailscale qualification remains required by
  [issue 93](https://github.com/vankhaivn/compute-relay/issues/93).

Metadata sources reviewed 2026-10-13: systemd's [machine-id contract](https://github.com/systemd/systemd/blob/main/man/machine-id.xml)
requires unique IDs and application-specific keyed derivation; Apple's
[IOPlatformUUID key](https://developer.apple.com/documentation/iokit/kioplatformuuidkey) identifies
the macOS property. Docker's [contexts](https://docs.docker.com/engine/manage-resources/contexts/)
and [system info](https://docs.docker.com/reference/cli/docker/system/info/) document endpoint
selection and daemon ID inspection. These sources establish the metadata mechanism, not a live
support claim. The [managed connection reference](../managed-connections.md) owns API details and
[SSH host reference](../providers/ssh-host.md) owns provider recovery behavior.
