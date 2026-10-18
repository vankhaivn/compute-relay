# SSH host health (Intel Mac)

On a native Intel Mac SSH host, Relay samples read-only health metrics and uses them to refuse new
work and, in sustained critical conditions, stop its own attempts. Linux and Apple Silicon hosts
report `unsupported` and are not blocked. Relay never changes sleep, power or fan settings.

## Read health

`GET /v1/workspaces/{w}/connections/{id}/health` (read token) returns `latest` snapshot, `guard`
(`level`, `reasons`, `refuse_new`, `stop`, `critical_since`), newest-first `history` and the
effective `policy`. Connections to the same machine share snapshots, but each evaluates its own
thresholds. Non-SSH connections get HTTP 400 `UNSUPPORTED_CAPABILITY`. Schema: [connection-health](../api/schemas/connection-health.v1alpha1.schema.json).

| Metric | Source |
|---|---|
| Thermal state (0 nominal … 3 critical), CPU speed limit | Foundation `thermalState`, `pmset -g therm` |
| CPU count, load, CPU and memory use | `sysctl`, `top`, `memory_pressure` |
| GPU utilization | `ioreg`, when exposed |
| Docker CPU/memory budget | `docker info` |
| Power source, battery charge, temperature, cycles, health | `pmset -g batt`, `ioreg` |
| Sleep enabled | `pmset -g custom` (diagnostic only) |
| CPU/GPU die temperature, fan RPM | Only with the optional sudo grant below |

Sampling: every 30 s while the machine has work, on demand otherwise (15 s cache, can take up to
35 s). A sample older than 90 s is stale. History keeps 120 rows / 24 h. After a Relay restart the
level is `unknown` until a new sample arrives.

### Optional die temperature and fan readings

Add exactly this line with `visudo` on the host (replace `relay` with the SSH user):

```sudoers
relay ALL=(root) NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i 1000
```

Relay runs only `sudo -n` with that exact command and never sends a password or edits sudoers. A
broader grant is reported as a warning. Remove the line to revoke.

## Thresholds

Defaults for new connections (existing revisions and running attempts keep theirs):

| Threshold | Warning | Critical |
|---|---|---|
| Thermal state | fair (1) | serious (2) |
| CPU speed limit | ≤90% | ≤50% |
| CPU or GPU die temperature | ≥90 °C | ≥95 °C |
| Low battery (on battery power) | — | ≤10% (stops immediately) |
| Sustained critical before stopping | — | 30 s (configurable 1–900 s) |

On the tested Intel Mac a full single-core load reached 101 °C while the thermal state still said
fair, so the temperature threshold is the important one. Measure your own workload before relying
on the defaults.

Change thresholds with `configure`; `host_health` is a patch, but `max_remote_wall_seconds` must be
included:

```json
{
  "action": "configure",
  "expected_revision": 3,
  "configuration": {"max_remote_wall_seconds": 1800, "host_health": {"critical_duration_seconds": 120}}
}
```

## Guard levels

| Level | New admission, preparation, start | Running work |
|---|---|---|
| `ok` | Allowed | Continues |
| `warning`, `unknown` | Refused: HTTP 422 `RESOURCE_REQUIREMENT_UNSATISFIED`; a queued attempt reports `host_unhealthy` | Continues |
| `critical` | Refused | Stopped after critical readings stay continuous for the configured duration (`host_critical_stopped`); low battery stops at once |
| `unsupported` | Not blocked | Continues |

Any healthy, missing, stale reading or restart resets the critical timer. Unknown health never stops
work. A health stop never retries, resubmits or switches hosts.

## Host watchdog

An optional user-level `launchd` agent on the host that enforces the critical thresholds even when
Relay is unreachable. It stops only containers carrying this installation's labels, never retries an
ambiguous stop, and records at most 64 stops per installation (reinstall to reset).

| Request (paths under `/v1/workspaces/{w}`) | Token |
|---|---|
| `POST /connections/{id}/host-watchdog/actions/install_host_watchdog` | manage |
| `POST /connections/{id}/host-watchdog/actions/remove_host_watchdog` | manage |
| `GET /connections/{id}/host-watchdog` | read |
| `GET /host-watchdog-operations/{op}` | read |

POST bodies contain only `expected_revision`, plus an `Idempotency-Key` header. Operations report
`queued`, `succeeded`, `unknown` or `failed`; for `unknown`, keep polling, and reconcile with an
explicit remove if needed. Install freezes the current thresholds — to change them, remove and
reinstall. The watchdog cannot act while the Mac sleeps or Docker is stopped.
