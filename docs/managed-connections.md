# Managed connections

Managed mode lets a local application administer provider connections while the server is
running. The initial adapter is Kaggle and its protected credential backend is macOS Keychain.
Ordinary `serve` and the explicit environment-configured [Kaggle mode](kaggle-runtime.md)
remain separate modes. Managed startup does not check an account or authorize compute.

## Start and discover

Initialize an installation and workspace using [Local runtime](local-runtime.md). Before starting
serve, issue separate private tokens for ordinary job operations, connection management
(`read`, `manage`) and owner-approved execution (`execute`). Keep management and execution
tokens outside an ordinary job-producing process. Token issuance remains local administration
and requires the server to be stopped.

Use an absolute Python executable containing the repository's locked Kaggle client:

```sh
compute-relay serve --root /absolute/private/runtime \
  --managed-python /absolute/provider-runtime/bin/python3
```

Optional policy flags are `--managed-machine-shape cpu|NvidiaTeslaT4|NvidiaTeslaP100`,
`--managed-max-wall-seconds 1..86400`, `--managed-max-workers 1..16`, and
`--managed-allow-internet`. Defaults are T4, 1800 seconds, two workers and internet disabled.
They configure future profiles; each verified connection revision freezes its own policy.
Use `--managed-machine-shape cpu` for CPU-only execution. It sends GPU disabled and no GPU
machine shape to Kaggle. This is an explicit configuration, never a fallback from an unavailable
GPU. Changing the service flag does not rewrite saved connections or jobs; explicitly check a
connection to publish a new selection under that configuration.
Managed flags cannot be mixed with standalone Kaggle flags. No flag grants an attempt permit.

Check authenticated `/v1/info` for `managed_connections` and `attempt_authorization`, then
`GET /v1/workspaces/{w}/providers` for installed descriptors and protected-store availability.
Unsupported storage is reported explicitly; it never falls back to plaintext. A supported
backend can still deny an actual operation. Local readiness is not provider authentication.

## Connection lifecycle

Render the descriptor's write-only credential fields in the application's local secret-entry
surface. Send credential JSON only in an authenticated POST body; never place it in a URL,
command argument, diagnostic output or job bundle. The initial Kaggle descriptor takes a token
and discovers the account itself. The application does not need to ask for a provider username.

`POST /connections` accepts `provider_type`, `label` and `credentials`. Under the same workspace
prefix, `POST /connections/{id}/actions` accepts an `expected_revision` plus `check`,
`replace_credential`, `disable`, `enable` or `remove`. Only replacement accepts credentials.
Use a fresh non-secret idempotency key per intended operation and preserve its receipt. Poll
the returned connection-operation URI for the current outcome; repeating a POST returns its
original receipt. All provider verification happens asynchronously outside the HTTP handler.

A newly verified connection returns an exact `selection.profile` and `selection.accelerator`.
Use both in the existing job specification; admission rejects a different resource. The optional
accelerator field is absent from legacy profiles. A resource-selecting client must request a
connection check rather than infer old resource policy from the current provider descriptor.
A later connection change can invalidate an old selection for new admission;
refresh explicitly after a conflict. Previously admitted jobs keep their frozen provider/account
binding. Credential replacement must verify the same account; a different account needs a new
connection. Two credentials for one account share capacity and do not create extra quota.

Disable stops new admission while preserving old recovery and collection. Removal refuses
dependent queued, active, uncertain or retained collection work, then deletes exact protected
items before recording a tombstone. Removed IDs remain readable but disappear from the active
list. A failed storage operation leaves a recoverable intent, not a reported success.

## Authorize one attempt

Upload immutable input objects, admit the job and retain its explicit attempt ID. Admission
does not authorize provider staging or execution. After an owner approves that exact job, send
`POST /jobs/{job_id}/authorize` using the separate execute token and an idempotency key:

```json
{
  "attempt_id": "att_EXAMPLE_REPLACE_WITH_RECEIPT",
  "max_remote_wall_seconds": 300,
  "authorize_private_staging": true,
  "authorize_compute": true
}
```

The wall bound must equal the frozen job request. New GPU work requires known, fresh, sufficient
cached quota after existing reservations. Unknown or stale GPU quota requires an explicit
connection check. Explicit CPU work does not require or reserve GPU seconds. Its unquantified
CPU allowance remains unknown and produces a scheduling warning; known CPU exhaustion still
blocks execution. All resources share the same account concurrency limit and finite consent.
Unresolved HTTPS inputs are not enabled here; there is no account fallback or rotation.

The permit is consumed durably before provider mutations. Restarting or changing startup flags
cannot create another permit. Recovery and collection use the original binding. An explicit
retry creates a new attempt that needs its own authorization. Current authority, capacity and
one-shot intent guards still apply after a permit is granted.

See the [wire and recovery contract](development/managed-connections.md) and
[OpenAPI](../api/openapi.json) for exact schemas, revisions and error semantics. Offline/native
tests qualify local behavior; this mode has no new live-provider qualification claim.
