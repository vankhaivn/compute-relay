# Execution gates and deadlines

Managed SSH connections support optional `"execution_control": "prepare_then_start"` in a job.
Check runtime feature `separate_prepare_start` and the connection provider's
`separate_prepare_start: "supported"` capability. Existing jobs keep the combined `/authorize`
flow. Kaggle does not support separate preparation/start.

Upload immutable bundle and input objects, then admit the job normally. Admission grants no
remote work. These endpoints use `/v1/workspaces/{w}/jobs/{job_id}` as their base. Each POST
requires current `execute` authority and its own `Idempotency-Key`.

## Grant preparation

`POST /prepare` accepts these required fields:

| Field | Meaning |
|---|---|
| `attempt_id` | The original active attempt from admission. |
| `max_preparation_seconds` | Provider preparation call limit, 1–3600 seconds. The runtime's lower limit still applies. |
| `ready_until` | Future whole-second UTC timestamp ending in `Z`, no more than one hour away and no later than a job execution deadline. |
| `max_staged_bytes` | Maximum sum of uploaded bundle and input bytes, 1 through 4,399,824,896. |
| `authorize_private_staging` | Must be `true`; grants staging only. |

A `202` receipt records permission, not completed preparation. All input bytes must already be
immutable local objects. The worker verifies the installed pinned image, creates a stopped
container, stages the fixed runtime and ordinary input bytes, then hashes them on the host.
No workload, setup command or application probe runs. The byte limit covers bundle/input content;
the fixed runner, manifest and control files add bounded overhead. No expanded bundle or workload
output exists yet.

A fixed verifier hashes the stopped container's files on the host, so only digests and sizes
cross SSH, before readiness and again before Start. Budget mainly for the upload: a gigabyte input
can need more than ten minutes on a slow link. Applications must explicitly request the larger
budget (for example, 1800 seconds); existing 600-second grants stay unchanged. Keep `ready_until`
far enough ahead for preparation and the second verification before Start. The hold, any earlier parent
deadline and the finite execution deadline still apply; a larger preparation grant never starts
compute or retries a failed transfer.

At most four outstanding preparation reservations per canonical account and sixteen globally
are admitted. Remote resources retain reservations after expiry, cancellation or uncertainty
until exact cleanup confirms deletion/absence. A never-staged terminal attempt releases its
reservation locally. Ready containers also reserve account capacity, but release the worker slot
while waiting. Choose a short hold and explicitly clean up unused preparations.

## Read readiness

`GET /readiness` requires `read` authority. It reads durable local evidence without provider calls.
`state: "ready"` supplies `revision`, `observed_at`, `ready_until` and verified `staged_bytes`.
Before verification, staged bytes are zero and no revision is available. Upload progress and a
preparation receipt do not establish readiness.

`required_actions` names the next explicit action. States include `preparation_required`,
`queued`, `preparing`, `ready`, `start_authorized`, `starting`, `started`, `expired`, `prevented`,
`invalidated` and `uncertain`. A dated ready observation may become stale; the start worker checks
the original resource and bytes again. `started` refers to the original start identity, not
current payload liveness or receiver health. Use normal job status for execution facts.

## Grant one start

`POST /start` accepts these required fields:

| Field | Meaning |
|---|---|
| `attempt_id` | The exact prepared attempt. |
| `readiness_revision` | The exact lowercase SHA-256 revision from readiness. |
| `max_remote_wall_seconds` | At most the admitted wall budget (itself at most the provider's advertised maximum: 86400 for Kaggle, 604800 for SSH), and greater than setup plus finalization budgets. |
| `execution_deadline` | Future whole-second UTC timestamp, at most this many wall seconds from authorization and no later than an original job deadline. |
| `authorize_compute` | Must be `true`; grants one start only. |

Preparation does not need the final execution deadline. Start tightens the execution window
without rewriting the accepted job or staged manifest. Start must happen before both
`ready_until` and `execution_deadline`, and before the expiry of any required
[workload secret](workload-secrets.md). Hold and secret expiry only bound start, not a running
workload, so an SSH attempt may run until its deadline of up to seven days.
Remote enforcement follows the [execution deadline contract](#execution-deadlines).

Before start, the worker rechecks image, container policy and complete staged bytes, then
atomically consumes start authority and records submission intent. It writes a small non-secret
control bound to the original manifest, nonce, attempt and revision, then starts that container
once. The runner validates the control before executing work. Connection policy, cancellation,
account limits and quotas are checked at the durable barrier.

## Replay, recovery and abandonment

Read either current receipt at `GET /gate-authorizations/{authorization_id}` with `execute`
authority. Matching POST replay returns the original receipt with `replay: true`, even after
consumption or expiry. It cannot grant another preparation or start. Changed requests under the
same key and second grants for the same attempt conflict.

Relay restart preserves readiness without inferring start consent. Missing or changed resources
invalidate readiness without restaging, pulling an image or creating replacements. Lost
preparation responses use observation of the original preparation. Once a start-control write
or start command may have happened, recovery only observes that identity. A crash after the
durable claim can leave uncertainty even if no command reached Docker; replay cannot repeat it.

Cancel a ready attempt with normal `/cancel` using `operate` authority. Expiry before start reports
timeout with execution `not_submitted`; hold expiry uses reason `preparation_expired`. Then
request `/cleanup` for the exact inactive container. Never-started preparations need no result
download. Cleanup still verifies ownership and inactivity; it does not complete an external
session. New work requires an explicit new attempt.

Shapes and static examples are in the
[execution gate schema](../api/schemas/execution-gate.v1alpha1.schema.json). Compute timestamps
when requesting authority; a static example timestamp is not reusable permission.

## Execution deadlines

An optional `timeouts.execution_deadline` limits time-sensitive work to an immutable UTC end
time. It belongs to the accepted job, including any later explicitly requested attempt. It does
not schedule a start or authorize staging, compute, cancellation or cleanup.

```json
"timeouts": {
  "remote_wall_seconds": 3600,
  "setup_seconds": 120,
  "finalization_grace_seconds": 30,
  "execution_deadline": "2026-10-12T20:00:00Z"
}
```

Use a future timestamp with this exact whole-second UTC spelling. Offsets, fractional seconds,
empty strings and null are rejected. The version is the existing `compute-connector/v1alpha1`
job contract. Omission retains the existing relative-only behavior and canonical request bytes.
The finite relative wall budget and profile ceiling still apply; neither can exceed the provider's
advertised maximum (24 hours on Kaggle, seven days on an SSH Docker host).

## Admission and start

Discover `absolute_execution_deadline` before submitting a deadline job. SSH implements it;
unsupported adapters reject the requirement before preparation. Offline validation cannot prove
host availability or remote clock accuracy.

New admission, execution authorization and explicit retry are refused after expiry with
`REMOTE_TIMEOUT`. An exact idempotency replay returns its original receipt even after expiry.
The worker checks before preparation and at the durable one-shot submission barrier. SSH checks
again immediately before sending Docker start; the remote runner checks before workload code.
Queueing, staging and delayed start delivery do not restart the absolute budget.

Before a deadline start, SSH reads the Docker engine's `SystemTime` without launching code.
The entire measured request interval must fit within five seconds of the controller's UTC
clock; a check taking more than five seconds also fails. An unavailable or skewed clock produces
the sanitized reason `clock_skew_unverified`, with no start sent. Operators must keep the host,
Docker VM and controller clocks synchronized. This is a bounded check, not clock synchronization
or protection against an administrator falsifying time.

## Remote enforcement

The earlier of the absolute deadline and `runner start + remote_wall_seconds -
finalization_grace_seconds` bounds preparation, setup and payload. Setup also retains its own
`setup_seconds` ceiling. Both UTC and an elapsed ceiling fixed at runner startup are checked.
The elapsed ceiling uses Linux `CLOCK_BOOTTIME`, so a backward wall-clock step cannot extend
the already fixed budget and elapsed suspend time counts. A forward UTC step may stop work early.

Each setup, probe and payload command has its own process group. An independent watchdog checks
at 50 ms intervals, sends TERM and then KILL, and does not depend on the runner's output drains,
Relay or the SSH connection. Allow up to one second of termination grace on a schedulable host,
plus the permitted five-second initial clock uncertainty. Continuing stdout or media progress
cannot extend either bound.

Result finalization is separate. It may run until the earlier of the original relative wall end
and the absolute deadline plus `finalization_grace_seconds`. The SSH container's PID 1 supervisor
bounds a blocked runner with at most two additional seconds, then exits; Docker terminates the
remaining processes in that container's private PID namespace. A hard stop can prevent a result
file from being written. A workload that deliberately escapes its command's process group can
outlive the payload grace until container teardown; trusted workload code and host administrators
are inside the trust boundary.

Linux cannot schedule a stop while the host is powered off, suspended or stuck in uninterruptible
kernel I/O. After resume, an expired deadline is acted on at the next schedulable watchdog poll;
termination then has the same grace. A Docker VM whose clocks stop or reset inconsistently does
not satisfy the clock assumptions. Sleep/resume, engine restart and reboot require separate live
qualification; a synthetic clock test is not evidence for a specific hypervisor or power policy.
Containers use `--restart no`; Relay never automatically starts them again.

## Status and recovery

Job status includes the frozen `execution_deadline`. A never-submitted expiry reports
`orchestration=timed_out`, `execution=not_submitted`, `deadline_exceeded=true` and reason
`absolute_deadline_expired`. It preserves any preparation identity for inspection and explicit
cleanup. It does not pretend that a payload ran.

After a start may have reached Docker, expiry does not establish inactivity. The original
submission stays in reconciliation, its capacity reservation remains conservative, and no permit
is rearmed. Use status and logs for the exact attempt, then wait for provider termination evidence.
`deadline_exceeded` records a proven prevented start or observed timeout; simply losing contact
after the UTC end is not remote stop evidence. Never replace the attempt from a local PID, timeout
or missing response alone.

Runner provenance records the requested and effective UTC bound, clock basis, applicable bound
(`absolute_deadline` or `relative_budget`), termination reason, process outcomes and separate
finalization grace. Absolute deadline timeouts use exit 124 and the result's `timed_out` field.
Result availability, container inactivity and hardware release remain separate facts. Relay stops
execution only; an application still owns any external service's session completion.

The relevant primary references are Python's [Linux clock semantics](https://docs.python.org/3.11/library/time.html#time.CLOCK_BOOTTIME)
and Docker's [engine information command](https://docs.docker.com/reference/cli/docker/system/info/),
reviewed 2026-10-12. See [compatibility](compatibility.md) for the qualification boundary.
