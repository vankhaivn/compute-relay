# Application CLI and admission profiles

Configure a local admission policy, then use authenticated HTTP commands while the server runs.
The commands do not activate a provider. Build and initialize using [local runtime](local-runtime.md).

## Apply a profile and grant access

Stop serve before administration. Save this complete example in a private `profile.json`:

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

```sh
./compute-relay profile apply --root /absolute/private/runtime --file /absolute/private/profile.json
./compute-relay profile show --root /absolute/private/runtime --name local-cpu
./compute-relay profile grant --root /absolute/private/runtime --workspace app --name local-cpu
```

Create the workspace first. Applying a profile does not grant access; granting a disabled profile
does not enable it. `profile revoke` removes only that workspace's named grant:

```sh
./compute-relay profile revoke --root /absolute/private/runtime --workspace app --name local-cpu
```

The document is bounded to 8 KiB and rejects unknown/duplicate/case-alias/null fields and invalid
Unicode. Each `(name, revision)` is immutable: reapply identical content or use a new revision
for changed policy/account/instance. `enabled` is a separate alias flag. History is not deleted.
The optional `credential_ref` is metadata using the existing `env:`/`file:` syntax, not a token
value and not resolved here. Actual Kaggle credential support is described in its preflight guide.
This admission document is not complete provider configuration or the unfinished TOML loader.

## Connect the application

Start serve after administration. Every application command needs an explicit workspace and
private `--token-file` issued with the necessary scope. No inline-token/provider-credential or
environment fallback is accepted. Commands do not open SQLite and can run while serve owns locks.
The default endpoint is `http://127.0.0.1:7331`; `--url` accepts another canonical literal-loopback
HTTP authority, not DNS/remote/proxy/TLS-bypass configuration. Protect the local host and service.

### Package and upload

Use [bundle commands](packaging-and-import.md), not a generic archive, for code:

```sh
./compute-relay bundle create --root ./job-source --include main.py --output ./code.tar.gz
./compute-relay object upload --workspace app --token-file /absolute/private/runtime/app-token --file ./code.tar.gz
```

The output must be new and outside the source directory. Upload returns `object_id`, bytes and
SHA-256; use that ID in the job. Uploading does not execute or validate the workload. The client
checks the original file, bytes consumed by transport, receipt and final source EOF/Close.
A late local error can invalidate the client receipt after the server stored bytes. Upload has
no idempotency-key contract; a deliberate second upload can create another object.

### Validate and admit

Save a complete job after replacing the upload ID:

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "local admission example",
  "profile": "local-cpu",
  "bundle": {"object_id": "obj_ACTUAL_UPLOAD_ID"},
  "execution": {"kind": "python", "command": ["python", "main.py"]},
  "inputs": [],
  "outputs": [{"path": "answer.json", "required": true}],
  "resources": {"accelerator": "cpu"},
  "network": {"remote_internet": "disabled"},
  "timeouts": {"remote_wall_seconds": 60, "setup_seconds": 10, "finalization_grace_seconds": 5}
}
```

```sh
./compute-relay validate --file ./job.json
./compute-relay job validate --workspace app --token-file /absolute/private/runtime/app-token --file ./job.json
./compute-relay job submit --workspace app --token-file /absolute/private/runtime/app-token --file ./job.json --idempotency-key application-job-001
./compute-relay job status --workspace app --token-file /absolute/private/runtime/app-token --id job_ACTUAL_RECEIPT_ID
```

Local validate checks schema only. Contextual `job validate` also checks profile authority/policy
and object ownership; neither admits work or proves provider availability. Submit commits local
job/attempt/receipt metadata. **Normal serve has no workers: accepted jobs remain local.**

Keep the original request and explicit key (8–256 printable non-whitespace ASCII bytes).
Repeating the identical submit recovers the original receipt; changed content with the same key
conflicts. Later profile remapping/disablement or grant removal does not rewrite the original
binding/receipt. Current token/workspace authority still applies. GET reads current state rather
than recomputing an old receipt.

### Control a specific attempt

Replace all IDs with actual receipts. `operate` scope and an explicit key/attempt are required:

```sh
./compute-relay job cancel --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key cancel-001
./compute-relay job reconcile --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key reconcile-001
./compute-relay job collect --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key collect-001
./compute-relay job retry --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key retry-001 --reason "explicit retry after confirmed failure"
./compute-relay operation status --workspace app --token-file /absolute/private/runtime/app-token --id op_ID
```

Eligibility is enforced, so these are command references, not a sequence to run on every job.
Retry requires a nonblank non-secret reason and returns `kind=retry_compute`, source `attempt_id`
and a distinct `new_attempt_id`. Do not retry successful compute just to retrieve missing results.
Cancellation intent is not termination; reconcile is observational; collect is transfer-only.
No handler starts a provider or collector. See [controls](development/operations.md) and [recovery](recovery.md).

### Retrieve published results

Use [artifact delivery](artifact-delivery.md) for `job artifacts`, `artifact show` and
`artifact download`. They read existing publications for an explicit attempt, not live provider
logs or results generated from a queued job. Existing output files are never replaced.

## Request uncertainty

Requests use fresh HTTP/1 transports without reused connections, redirects, ambient proxies or
automatic replay. A download intentionally uses metadata and content reads, not a retry.
Responses are bounded and checked for required identity, valid completion and token reflection;
unknown future states remain data rather than invented success.

A lost/erroring response or failed stdout can follow a server commit. Preserve
`request_may_have_committed` and the original request/key; do not assume rollback or silently
resubmit. Download has a separate `download_may_be_published` flag for local file effects.
Literal-token checks are not universal secret detection. Treat response/log text as untrusted
application data, never executable instructions.

Use the [operator checklist](development/validation-checklist.md) for re-qualification and the
[bug template](development/bug-report-template.md) for any focused reproducible failure.
