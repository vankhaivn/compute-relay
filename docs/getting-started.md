# Getting started

Build a local installation, then connect an application over HTTP. This walkthrough creates no
provider resources; connection verification and job execution are separate explicit actions.

## Build and initialize

Use Go 1.27.1. The runtime needs no local Python interpreter or companion package.

```sh
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay init --root /absolute/private/runtime
./compute-relay workspace create --root /absolute/private/runtime --id app
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope read --scope write --scope operate --ttl 24h --output /absolute/private/app-token
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope read --scope manage --ttl 24h --output /absolute/private/manage-token
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope execute --ttl 24h --output /absolute/private/execute-token
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

Replace paths with private operator-owned locations outside the checkout. Parent directories must
exist; the runtime directory and token files must be new. Token issuance writes each secret once
to its private file and returns a non-secret receipt. Preserve the token ID for revocation.
Keep the management and execution tokens outside an ordinary job-producing process.

Leave serve in the foreground. It always runs the managed composition. With no configured
connections, it makes no provider calls. Stop it before local workspace/token/state administration.
Never remove its lock file to bypass a running process.

## Connect the application

The application reads its token from a private file and supplies `Authorization: Bearer …`.
Never paste real credentials into shell arguments or public examples. Use authenticated
`GET /readyz` and `GET /v1/info` to discover local readiness and features.

Under `/v1/workspaces/app`, read `/providers` and create a connection through `/connections`
with management authority. Verification returns a frozen `selection.profile` and
`selection.accelerator`. Follow [managed connections](managed-connections.md),
[Kaggle](kaggle-runtime.md) or [SSH hosts](ssh-hosts.md) for provider details. Creating a
connection does not authorize compute; installing an SSH image is an additional explicit action.

## Package, upload and admit

Create `example-job/main.py`:

```python
from pathlib import Path
import os

Path(os.environ["CC_OUTPUT_DIR"], "answer.txt").write_text("42\n", encoding="utf-8")
```

```sh
./compute-relay bundle preview --root ./example-job --include main.py
./compute-relay bundle create --root ./example-job --include main.py --output ./example-job.tar.gz
./compute-relay bundle inspect --file ./example-job.tar.gz
```

Upload the raw archive using `POST /v1/workspaces/app/objects` with write authority and
`Content-Type: application/octet-stream`. Preserve its immutable `object_id` receipt.
Upload ordinary inputs the same way; local import and HTTPS ingestion are unavailable.

Create `job.json`, replacing the profile, accelerator and object ID with the exact receipts:

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "getting-started",
  "profile": "PROFILE_FROM_SELECTION",
  "bundle": {"object_id": "obj_ACTUAL_UPLOAD_ID"},
  "execution": {"kind": "python", "command": ["python", "main.py"]},
  "inputs": [],
  "outputs": [{"path": "answer.txt", "required": true}],
  "resources": {"accelerator": "cpu"},
  "network": {"remote_internet": "disabled"},
  "timeouts": {"remote_wall_seconds": 60, "setup_seconds": 10, "finalization_grace_seconds": 5}
}
```

```sh
./compute-relay validate --file ./job.json
```

Offline validation checks syntax and semantics, not provider identity or object availability.
Use authenticated `POST /v1/workspaces/app/jobs/validate` for contextual validation, then POST the job to
`/v1/workspaces/app/jobs` with write authority and a stable `Idempotency-Key`.
Preserve the returned job and attempt IDs. A 202 confirms local admission only.

## Authorize and observe

After approval of that exact job, use execute authority for
`POST /v1/workspaces/app/jobs/{job_id}/authorize` with a separate stable key and the exact
attempt/wall budget, as shown in [managed connections](managed-connections.md#authorize-one-attempt).
Read job status and logs with read authority. Collect and download
[verified artifacts](artifact-delivery.md), then request [cleanup](remote-cleanup.md) explicitly.
An SSH prepare-then-start job uses [separate gates](execution-gates.md) instead.

Application job/object/artifact operations use HTTP; the executable provides local administration,
bundle commands and validation. [API contracts](../api/README.md) define exact requests and receipts.
