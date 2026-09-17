# Admission profiles and application CLI

> **Task:** M5-01b, implemented in PR #26; pending owner review/merge.
> **Requirements:** API-01/03, DX-01; preserves SEC-02 and durable receipt/binding identity.
> **Boundary:** local admission and authenticated application requests, not provider activation.

M5-01a's local installation and HTTP lifecycle are merged in PR #25. This slice adds real
operator commands for admission profiles and application commands using the running HTTP API.
It removes the need for test-only profile seeding to exercise local admission. It does not
configure a complete provider, start workers, run workloads or establish live Kaggle support.
Parent M5-01, M5-02 TOML, artifact/log routes and general worker composition remain separate.

## Local profile administration

Build the main executable with the pinned toolchain as described in [local runtime](local-runtime.md).
Use an existing initialized private installation. **Stop serve before profile administration**;
these commands acquire its existing exclusive locks. No administrative HTTP endpoint is added.

Save this complete example as a private file such as `/absolute/private/runtime/profile.json`.
Choose the instance/account/revision labels deliberately for the operator's intended binding:

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

This is an admission-policy document, not the normalized runtime configuration or M5-02 TOML.
`credential_ref` is optional and, when present, must satisfy the existing explicit `env:` or
`file:` reference syntax. It is stored as metadata and never resolved by profile commands or
the application client. Do not put a token value in any document field. The example has no
credential reference and does not install a synthetic or executable provider.

```text
./compute-relay profile apply --root /absolute/private/runtime --file /absolute/private/runtime/profile.json
./compute-relay profile show --root /absolute/private/runtime --name local-cpu
./compute-relay profile grant --root /absolute/private/runtime --workspace app --name local-cpu
./compute-relay workspace show --root /absolute/private/runtime --id app
```

Create the workspace first through the existing workspace command. Apply and grant are separate:
applying a profile never grants a workspace access or enables dispatch. Grant requires an existing
profile; granting a disabled profile does not make it usable for new admission. Revoke removes
only the named allowlist entry and does not delete the historical revision or enable a workspace:

```text
./compute-relay profile revoke --root /absolute/private/runtime --workspace app --name local-cpu
```

The document is at most 8 KiB, with exact required field names and explicit booleans. Unknown,
duplicate, case-alias, null, malformed Unicode and trailing fields/documents fail before state
is opened. Names and policy limits use the existing admission validation. Maxima are 86,400
wall seconds, 100 MiB bundle bytes and 4 GiB input bytes; these are local ceilings, not provider
capacity promises. Paid policy and malformed credential references are rejected.

The existing `PutProfile` transaction stores each `(name, revision)` snapshot immutably and
selects the active revision for future admissions. Reapplying identical content is permitted;
changing content under the same revision conflicts. Use a new revision for changed policy,
account or instance. `enabled` is a separate alias flag and may change without changing the
snapshot. There is no revision deletion or history rewrite. Grant/apply are separate local
operations, not an atomic multi-command configuration deployment.

Receipts show the profile name, instance, revision, snapshot digest, limits and whether a
credential reference exists. They omit the account scope and credential reference itself and
explicitly keep `provider_checked=false` and `dispatch_enabled=false`. A digest is not permission
or proof that a configured provider/account exists. Treat operator labels/digests as private
metadata when sharing reports.

## Application requests use the running server

Start `serve` after local administration. Application commands do not open the database and
can run while it holds the installation lock. Every command requires explicit workspace and
`--token-file`; use a token issued with the necessary read/write/operate scopes. The file must
be a private regular file, with exactly the issued 47-byte token and an optional single final LF.
No environment-token fallback, inline token value, provider credential or secret-output flag exists.

The default endpoint is `http://127.0.0.1:7331`. `--url` can select another exact literal-loopback
HTTP authority, including `[::1]`; no DNS, userinfo, path prefix, query, fragment, scoped/mapped
alias, remote address, TLS bypass or proxy is supported. The selected local service is trusted:
loopback restriction alone does not authenticate a malicious same-user service. Protect the host,
port and token file. Remote/TLS client configuration belongs to later work.

### Upload and validate

```text
./compute-relay object upload --workspace app --token-file /absolute/private/runtime/app-token --file ./code.tar.gz
./compute-relay validate --file ./job.json
./compute-relay job validate --workspace app --token-file /absolute/private/runtime/app-token --file ./job.json
```

Create a valid code bundle with the existing bundle commands, then use the returned **`object_id`**
in `bundle.object_id` in the job specification. An upload does not execute or validate the bundle's
workload. The client hashes the source before sending, hashes the bytes actually read by HTTP,
and checks the server's object/workspace/length/digest receipt plus final local source EOF and
Close before reporting success. Missing/changed/symlink/nonregular/oversized input fails; even a
source change after server consumption invalidates the client receipt without undoing server data.

A local `validate` checks schema/semantics only. `job validate` also asks the authenticated server
to verify current profile permission/policy and referenced object ownership. Neither admits a job,
fetches pending HTTPS inputs or proves account/capability/bundle readiness. Server warnings and
remaining verification requirements are returned intact.

A complete small admission-only example, after replacing the object ID with the upload receipt:

```json
{
  "api_version": "compute-connector/v1alpha1",
  "name": "local admission example",
  "profile": "local-cpu",
  "bundle": {"object_id": "obj_REPLACE_WITH_UPLOAD_ID"},
  "execution": {"kind": "python", "command": ["python", "main.py"]},
  "inputs": [],
  "outputs": [{"path": "answer.json", "required": true}],
  "resources": {"accelerator": "cpu"},
  "network": {"remote_internet": "disabled"},
  "timeouts": {"remote_wall_seconds": 60, "setup_seconds": 10, "finalization_grace_seconds": 5}
}
```

### Admission and current status

```text
./compute-relay job submit --workspace app --token-file /absolute/private/runtime/app-token --file ./job.json --idempotency-key application-job-001
./compute-relay job status --workspace app --token-file /absolute/private/runtime/app-token --id job_REPLACE_WITH_RECEIPT_ID
```

IDs in examples are placeholders to replace, not existing resources. Submit requires an explicit
8–256-byte printable non-whitespace ASCII key. Keep the original file, endpoint, workspace and
key. A 202 receipt identifies durable local admission; it is not execution or job success.
`serve` remains `local-admission-only / dispatch_enabled=false`, so accepted work remains queued
until a separately implemented/authorized worker path exists.

Explicitly repeating the identical submit recovers the original receipt, not a new attempt.
Changed specification with the same key conflicts. Profile remapping, profile disablement or
removal of the workspace's profile grant do not retarget an already accepted job or rewrite its
receipt. Current token/workspace authority is still required. GET returns current state; the
client retains unknown state strings and future response fields without inventing terminal meaning.

### Explicit controls

```text
./compute-relay job cancel --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key cancel-001
./compute-relay job retry --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key retry-001 --reason "explicit retry after confirmed failure"
./compute-relay job reconcile --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key reconcile-001
./compute-relay job collect --workspace app --token-file /absolute/private/runtime/app-token --id job_ID --attempt att_ID --idempotency-key collect-001
./compute-relay operation status --workspace app --token-file /absolute/private/runtime/app-token --id op_ID
```

Each control requires explicit source attempt and key. Retry also requires a nonblank reason;
all reasons are bounded non-secret text. Eligibility remains enforced by existing M3 services.
The retry route returns `kind=retry_compute`, preserves `attempt_id` as the source and supplies a
**distinct `new_attempt_id`**. It is not collection retry or implicit provider switching.

Original control receipt replay and current operation reads remain different. Cancellation intent
is not remote termination, reconcile does not resubmit compute, and an accepted collect ticket
is not verified artifact availability. This local server does not start the provider/control or
collection worker that later consumes such work. Use the appropriate command, not a generic retry
loop. No command selects an implicit active attempt or automatically generates another key.

## One request and explicit uncertainty

Each invocation creates one fresh HTTP/1 transport, sends one request and closes it. There is no
connection reuse, redirect following, ambient proxy, cookie jar, GetBody replay or automatic retry.
The [Go Transport contract](https://pkg.go.dev/net/http#Transport) permits some retries on reused
connections, including requests with Idempotency-Key; the fresh transport prevents that path.
The transport's request-body closure is joined before the caller reuses/closes an upload source.

Responses require bounded identity-encoded JSON, successful EOF/Close and the expected HTTP status
and target fields. Duplicate/ambiguous JSON and raw/escaped current-token reflection are rejected.
These checks are not a full future-response schema validator or business-success classifier.
Compatible extra fields and unknown status strings remain raw JSON data, not commands, HTML or
trusted log instructions. Literal-token rejection is not universal secret detection.

Errors expose only bounded stage/status/code and `request_may_have_committed`, never the raw server
message, exception, source path or token. The uncertainty flag is conservative; it does not prove a
commit occurred or that remote compute started. `automatic_retry_attempted=false` records client
behavior. Exit 0 means the checked response was written, 2 means invalid arguments, and 1 means a
request/file/output operation failed. Nonzero after mutation or stdout failure does not undo it.

For ambiguous submit/control, inspect retained information and explicitly recover the original
request/key where appropriate. **Object upload has no server idempotency-key contract**: repeating
it can create another object, even with identical bytes. The client neither retries it nor silently
replaces the returned ID. A failed output sink can already contain a partial JSON prefix; never
consume that as a successful receipt. No receipts are secretly persisted by this CLI.

Limits are 1 MiB job JSON/response, 2 GiB upload, 64 bytes token-file framing, 32 KiB response headers,
30 seconds for metadata calls and two minutes for upload including prehashing. Connect/header
budgets are 5/10 seconds. Partial upload resume is not implemented. Large files can exceed the
budget safely; there is no throughput guarantee. Contexts and joined readers rely on cooperative
callbacks/OS operations and are not a hostile-host or hard-real-time sandbox.

## Verification and next gate

```text
go test -race ./internal/appclient ./internal/appcli ./internal/jsonwire ./internal/operatorcli ./internal/runtimehost ./internal/cli
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Real loopback client tests cover lost headers/body, redirects, malformed/late-close responses,
source/receipt identity, cancellation and unknown fields. Real CLI/service/SQLite/blob integration
uses the actual profile apply/grant commands, upload and contextual validation, then loses a response
**after real admission commit** and recovers the original receipt explicitly. Reopen/remap/revoke,
queued cancellation/control replay, current operation reads and token revocation retain one job/
attempt, original profile and input bytes, with zero remote submission/resource/publication records.
New parser/main-route tests check inert help and invalid flags; these are not additional OS-process
or live-provider experiments. Existing main-entry subprocess and M3 fault suites remain separate.

Local Go 1.23.2 runs the exact standard-library client/JSON packages with race repetition and vet.
This is not full local pinned Go/modernc/CLI integration. Exact-head native, full race and fault-matrix
evidence is recorded in PR #26. No dependency downgrade, replacement driver or temporary local test
module is shipped. Earlier fixture corrections are not relabeled as changing the public contract.

See [ADR-0022](decisions/0022-immutable-profiles-and-application-client.md), [local runtime](local-runtime.md),
[admission](admission.md), [controls](operations.md) and the [implementation plan](implementation-plan.md).
Stop after PR #26 for owner merge; parent M5-01 result/worker work, M5-02 and M4-06/M1 live acceptance
are not completed by these local commands.
