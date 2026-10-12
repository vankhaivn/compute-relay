# Execution deadlines

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
The finite relative wall budget and profile ceiling still apply; neither can exceed 24 hours.

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
reviewed 2026-10-12. See [current status](status.md) for the actual live qualification boundary.
