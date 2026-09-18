# Getting started

This walkthrough takes a fresh clone to a working **local admission runtime**. At the end you will
have built the binary, initialized private state, created a workspace/token, applied an admission
profile, started the loopback server, packaged/uploaded code, and admitted one job.

**This walkthrough intentionally uses the safe admission-only mode.** It does not consume provider
quota or run remote compute. After you understand the local flow, continue with
[Kaggle runtime](kaggle-runtime.md) to enable bounded GPU dispatch explicitly.

## 1. Build the binary

Use the Go version pinned in [`go.mod`](../go.mod).

```sh
git clone https://github.com/vankhaivn/compute-relay.git
cd compute-relay
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay help
```

On Windows build/use `compute-relay.exe` and use absolute Windows paths below.

## 2. Choose private runtime paths

Runtime state and token files must live outside the source checkout in directories you control.
In the examples below:

```text
/absolute/private/runtime
/absolute/private/runtime/app-token
```

The parent `/absolute/private` must already exist. The runtime directory and token output file
must not already exist when created.

## 3. Initialize the runtime and application identity

```sh
./compute-relay init --root /absolute/private/runtime
./compute-relay workspace create --root /absolute/private/runtime --id app
./compute-relay token issue \
  --root /absolute/private/runtime \
  --workspace app \
  --scope read \
  --scope write \
  --scope operate \
  --ttl 24h \
  --output /absolute/private/runtime/app-token
```

The secret is written to the private token file, not printed. Keep the returned token ID if you
will need to revoke it later.

## 4. Create and grant an admission profile

Create `/absolute/private/profile.json`:

```json
{
  "schema_version": 1,
  "name": "local-cpu",
  "instance_id": "operator_instance",
  "revision": "rev1",
  "account_scope": "operator_account",
  "enabled": true,
  "cost_class": "free_allowance",
  "allow_remote_internet": false,
  "max_remote_wall_seconds": 60,
  "max_bundle_bytes": 104857600,
  "max_input_bytes": 4294967296
}
```

Apply and grant it before starting the server:

```sh
./compute-relay profile apply --root /absolute/private/runtime --file /absolute/private/profile.json
./compute-relay profile grant --root /absolute/private/runtime --workspace app --name local-cpu
```

A profile is an immutable admission policy. It is **not** complete provider configuration and
does not enable dispatch.

## 5. Start the local server

```sh
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

Leave this process in the foreground. The startup report should identify the runtime as
`local-admission-only` with `dispatch_enabled=false`.

Local administrative commands acquire exclusive installation locks, so stop `serve` before
running workspace/token/profile administration.

## 6. Package a small job

In another terminal, create a directory such as `example-job/` with a `main.py` file:

```python
from pathlib import Path
import json

Path("answer.json").write_text(json.dumps({"answer": 42}) + "\n", encoding="utf-8")
```

Package only the file you intend to send:

```sh
./compute-relay bundle preview --root ./example-job --include main.py
./compute-relay bundle create --root ./example-job --include main.py --output ./example-job.tar.gz
./compute-relay bundle inspect --file ./example-job.tar.gz
```

For the full bundle rules see [Bundles and import](packaging-and-import.md).

## 7. Upload the bundle

With `serve` still running:

```sh
./compute-relay object upload \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./example-job.tar.gz
```

Copy the returned `object_id`; the next step references that exact immutable upload.

## 8. Validate and admit a job

Create `job.json`, replacing `obj_ACTUAL_UPLOAD_ID` with the upload receipt:

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "getting-started",
  "profile": "local-cpu",
  "bundle": {"object_id": "obj_ACTUAL_UPLOAD_ID"},
  "execution": {"kind": "python", "command": ["python", "main.py"]},
  "inputs": [],
  "outputs": [{"path": "answer.json", "required": true}],
  "resources": {"accelerator": "cpu"},
  "network": {"remote_internet": "disabled"},
  "timeouts": {
    "remote_wall_seconds": 60,
    "setup_seconds": 10,
    "finalization_grace_seconds": 5
  }
}
```

First validate syntax/semantics locally, then validate against runtime authority/policy, then admit:

```sh
./compute-relay validate --file ./job.json

./compute-relay job validate \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./job.json

./compute-relay job submit \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./job.json \
  --idempotency-key getting-started-001
```

Copy the returned job/attempt IDs and inspect the job:

```sh
./compute-relay job status \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --id job_ACTUAL_RECEIPT_ID
```

The job is durably admitted, but this admission-only `serve` process does not execute it. Do not
interpret `202 Accepted`, a queued attempt, or a control receipt as remote execution.

## 9. What to do next

For day-to-day commands use the [Operator runbook](runbook.md). Detailed references are
[Local runtime](local-runtime.md), [Application CLI](application-cli.md),
[Artifact delivery](artifact-delivery.md), and [Recovery](recovery.md).

To run an application job on Kaggle, continue with the user-facing
[Kaggle runtime](kaggle-runtime.md). Contributors changing provider internals or repeating the
fixed qualification experiment should instead start with
[Development documentation](development/README.md).
