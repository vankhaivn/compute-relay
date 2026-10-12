# Prepare an attempt before starting it

Managed SSH connections support optional `"execution_control": "prepare_then_start"` in a job.
Check runtime feature `separate_prepare_start` and the connection provider's
`separate_prepare_start: "supported"` capability. Existing jobs keep the combined `/authorize`
flow. Standalone dispatch and Kaggle do not support separate preparation/start.

Upload immutable bundle and input objects, then admit the job normally. Admission grants no
remote work. These endpoints use `/v1/workspaces/{w}/jobs/{job_id}` as their base. Each POST
requires current `execute` authority and its own `Idempotency-Key`.

## Grant preparation

`POST /prepare` accepts these required fields:

| Field | Meaning |
|---|---|
| `attempt_id` | The original active attempt from admission. |
| `max_preparation_seconds` | Provider preparation call limit, 1–600 seconds. The runtime's lower limit still applies. |
| `ready_until` | Future whole-second UTC timestamp ending in `Z`, no more than one hour away and no later than a job execution deadline. |
| `max_staged_bytes` | Maximum sum of uploaded bundle and input bytes, 1 through 4,399,824,896. |
| `authorize_private_staging` | Must be `true`; grants staging only. |

A `202` receipt records permission, not completed preparation. All input bytes must already be
immutable local objects. The worker verifies the installed pinned image, creates a stopped
container, stages the fixed runtime and ordinary input bytes, then reads back and hashes them.
No workload, setup command or application probe runs. The byte limit covers bundle/input content;
the fixed runner, manifest and control files add bounded overhead. No expanded bundle or workload
output exists yet.

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
| `max_remote_wall_seconds` | At most the admitted wall budget, and greater than setup plus finalization budgets. |
| `execution_deadline` | Future whole-second UTC timestamp, at most this many wall seconds from authorization and no later than an original job deadline. |
| `authorize_compute` | Must be `true`; grants one start only. |

Preparation does not need the final execution deadline. Start tightens the execution window
without rewriting the accepted job or staged manifest. Start must happen before both
`ready_until` and `execution_deadline`; hold expiry only bounds start, not a running workload.
Remote enforcement follows the [execution deadline contract](execution-deadlines.md).

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
