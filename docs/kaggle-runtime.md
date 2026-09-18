# Run jobs on Kaggle

This is the end-to-end operator path for running bounded GPU jobs through the normal
`compute-relay serve` process.

Provider mode is **explicit and pre-release**. With no provider flags, `serve` remains
local-admission-only and cannot consume provider quota. With the complete provider flag set below,
the server performs a read-only account check before it starts workers, then runs one durable
dispatch worker and one collection worker.

The fixed Kaggle acceptance experiment has live-qualified the underlying private-staging,
Tesla-T4, restart/reconciliation and artifact-publication components. The integrated normal-server
path is implemented and covered by offline/native CI, but it still requires operator
re-qualification before making a new environment/account support claim.

## 1. Install the locked Kaggle client

From the repository root, install `uv` 0.12.13 and create/check the repository-owned client
environment:

```sh
./scripts/kaggle-client.sh sync
./scripts/kaggle-client.sh check
```

PowerShell and CMD wrappers are documented in
[tools/kaggle-client](../tools/kaggle-client/README.md).

The provider config below must use the absolute Python executable from this locked environment.
Do not point it at an unrelated Kaggle installation.

## 2. Configure the credential outside Git

Provision the Kaggle token in an explicitly named environment variable, for example
`CR_KAGGLE_TOKEN`. Do not put the token value in the provider JSON, profile, command line,
job specification, logs or repository.

Create a private provider file outside the checkout, for example
`/absolute/private/kaggle.json`:

```json
{
  "instance_id": "kaggle-personal",
  "revision": "runtime-1",
  "account_name": "your_canonical_username",
  "credential_ref": "env:CR_KAGGLE_TOKEN",
  "python_executable": "/absolute/path/compute-relay/tools/kaggle-client/.venv/bin/python"
}
```

The file contains provider identity/configuration, not the token value. The configured account,
instance, revision and credential reference must exactly match the admission profile in the next
step. The runtime performs an authenticated read-only account check before enabling workers.

## 3. Apply the matching admission profile

Stop `serve` before local administration. Create
`/absolute/private/kaggle-t4-profile.json`:

```json
{
  "schema_version": 1,
  "name": "kaggle-t4",
  "instance_id": "kaggle-personal",
  "revision": "runtime-1",
  "account_scope": "your_canonical_username",
  "credential_ref": "env:CR_KAGGLE_TOKEN",
  "enabled": true,
  "cost_class": "free_allowance",
  "allow_remote_internet": false,
  "max_remote_wall_seconds": 120,
  "max_bundle_bytes": 104857600,
  "max_input_bytes": 4294967296
}
```

Apply and grant it to the application workspace:

```sh
./compute-relay profile apply \
  --root /absolute/private/runtime \
  --file /absolute/private/kaggle-t4-profile.json

./compute-relay profile grant \
  --root /absolute/private/runtime \
  --workspace app \
  --name kaggle-t4
```

Provider mode accepts only the `free_allowance` cost class. There is no CPU, second-provider,
paid-capacity or account fallback.

## 4. Start provider-enabled serve

Choose a finite number of **distinct new attempt identities** that this process may authorize for
provider mutation. Start with one when proving a new environment:

```sh
./compute-relay serve \
  --root /absolute/private/runtime \
  --listen 127.0.0.1:7331 \
  --provider-config /absolute/private/kaggle.json \
  --provider-profile kaggle-t4 \
  --provider-machine-shape NvidiaTeslaT4 \
  --max-provider-attempts 1 \
  --allow-private-staging \
  --allow-gpu
```

A successful startup reports:

```text
mode = kaggle-workers
dispatch_enabled = true
```

All provider options are required together. Omitting them starts the safe local-admission-only
mode instead; supplying a partial set is rejected.

`--max-provider-attempts` is a per-process mutation budget, not a quota claim. Once that many
distinct attempts have received a new staging/submission permit, the dispatcher stops claiming
additional queued jobs. Recovery/observation of already-started attempts and collection of their
results remain available. Starting another provider-enabled process with a fresh budget is a new
operator authorization; resolve uncertain remote activity first.

Stopping the local process does **not** cancel remote compute or prove hardware release.

## 5. Package and upload a GPU job

For a minimal smoke workload, create `gpu-job/main.py`:

```python
from pathlib import Path
import json
import torch

x = torch.tensor([6, 7], device="cuda")
answer = int((x[0] * x[1]).item())
Path("answer.json").write_text(
    json.dumps({"answer": answer}) + "\n",
    encoding="utf-8",
)
```

Package and upload it while provider-enabled `serve` is running:

```sh
./compute-relay bundle preview --root ./gpu-job --include main.py
./compute-relay bundle create --root ./gpu-job --include main.py --output ./gpu-job.tar.gz

./compute-relay object upload \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./gpu-job.tar.gz
```

Keep the returned `object_id`.

The current normal provider composition does not enable direct HTTPS input fetching. Upload code
and input objects first and reference their immutable object IDs in the job.

## 6. Validate and submit

Create `gpu-job.json`, replacing `obj_ACTUAL_UPLOAD_ID`:

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "kaggle-gpu-smoke",
  "profile": "kaggle-t4",
  "bundle": {"object_id": "obj_ACTUAL_UPLOAD_ID"},
  "execution": {
    "kind": "python",
    "command": ["python", "main.py"]
  },
  "inputs": [],
  "outputs": [
    {"path": "answer.json", "required": true}
  ],
  "resources": {
    "accelerator": "gpu",
    "minimum_gpu_count": 1
  },
  "network": {
    "remote_internet": "disabled"
  },
  "timeouts": {
    "remote_wall_seconds": 120,
    "setup_seconds": 30,
    "finalization_grace_seconds": 10
  }
}
```

Then:

```sh
./compute-relay validate --file ./gpu-job.json

./compute-relay job validate \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./gpu-job.json

./compute-relay job submit \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./gpu-job.json \
  --idempotency-key kaggle-gpu-smoke-001
```

Save the returned job and attempt IDs. `202 Accepted` proves durable local admission only; it is
not remote-success evidence.

For GPU jobs the runtime requires a known positive free-quota observation before a new provider
mutation. Unknown or insufficient quota fails closed rather than selecting CPU, another account,
another provider or paid capacity.

## 7. Observe the original attempt

Poll the same job identity:

```sh
./compute-relay job status \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --id job_ACTUAL_RECEIPT_ID
```

The durable engine stages private inputs once, submits once after its write-ahead intent, observes
the exact remote identity and gives recovery priority after restart. It does not silently create a
replacement execution after response loss.

A terminal provider state is not yet the same fact as a published local result. Collection pins
and verifies the original result set before publication.

## 8. Download the published result

After the attempt has a committed publication:

```sh
./compute-relay job artifacts \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --id job_ID \
  --attempt att_ID \
  --limit 100

./compute-relay artifact download \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --job job_ID \
  --attempt att_ID \
  --id art_ID \
  --output /absolute/private/results/answer.json
```

Artifact downloads are create-only and independently verify the bytes and final stream
acknowledgement. See [Artifact delivery](artifact-delivery.md).

## Recovery and uncertainty

Preserve the runtime root, provider config/profile revision, original binary where needed,
job/attempt IDs and idempotency keys.

- A lost submit HTTP response may follow a committed local admission. Replay only the **identical**
  application request with its original idempotency key to recover that receipt.
- Once the durable provider submission intent exists, restart recovery observes/reconciles the
  original provider identity; it does not authorize another provider submission.
- Do not create another runtime root, clear intent rows or submit a replacement job merely because
  the remote outcome is uncertain.
- Cancellation remains manual-required when Kaggle does not expose a verified session target.
  Cancellation intent, local shutdown, terminal execution and hardware/accounting release are
  different facts.
- Remote cleanup apply is not shipped. Preserve exact owned resources and resolve remote activity
  before any separately authorized manual cleanup.

See [Safe recovery](recovery.md) for the general recovery model.

## Current support boundary

This integrated path is intentionally narrow:

- Kaggle is the configured provider; T4/P100 are the accepted machine-shape values.
- The normal server runs one dispatch worker and one collection worker.
- Provider config is explicit JSON; strict runtime TOML/doctor remain future product work.
- Direct HTTPS input preparation is not enabled in this composition; use uploaded immutable inputs.
- Provider quota/log components exist internally, but there is no public provider quota/log HTTP
  API in the normal server yet.
- Provider timeout enforcement beyond local/runner budgets, exact hardware release and remote
  cleanup remain unverified or unsupported as documented in [Current status](status.md).

Offline/native tests do not replace live provider evidence. Re-qualify the intended account,
provider/toolchain version and workload boundary before promoting a new support claim.
