# Operator runbook

Use this page after the initial installation is configured. It is a task-oriented runbook for the
normal local runtime; detailed semantics live in the linked reference guides.

`serve` is **local-admission-only by default**. For bounded GPU execution through the normal
runtime, use the explicit [Kaggle runtime](kaggle-runtime.md) startup instead. See
[current status](status.md) for the current support boundary.

## Before every session

Use the original runtime root and token files. Do not recreate state because a command failed.

```text
runtime root: /absolute/private/runtime
workspace:    app
token file:   /absolute/private/runtime/app-token
server:       http://127.0.0.1:7331
```

Keep credentials, runtime databases, private inputs, raw provider responses and signed URLs
outside the repository.

## Start and stop the runtime

Start:

```sh
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

For this default command, expected mode is `local-admission-only` and
`dispatch_enabled=false`. Provider-enabled startup is documented separately in
[Kaggle runtime](kaggle-runtime.md).

Stop with the foreground process's interrupt/SIGTERM. Let graceful shutdown complete. Do not
delete lock files to make another command run.

**Rule:** stop `serve` before local administration such as `state`, workspace/token changes,
or profile changes. Application HTTP commands are the commands intended to run while `serve`
owns the installation.

## Inspect local state

With `serve` stopped:

```sh
./compute-relay state --root /absolute/private/runtime
./compute-relay workspace show --root /absolute/private/runtime --id app
```

Do not manually edit SQLite rows, identity markers, locks or store directories.

## Workspace and token administration

Create another workspace:

```sh
./compute-relay workspace create --root /absolute/private/runtime --id another-app
```

Issue a scoped token to a new private file:

```sh
./compute-relay token issue \
  --root /absolute/private/runtime \
  --workspace another-app \
  --scope read \
  --scope write \
  --ttl 24h \
  --output /absolute/private/runtime/another-app-token
```

Disable/enable a workspace or revoke a token using the real receipt IDs:

```sh
./compute-relay workspace disable --root /absolute/private/runtime --id app
./compute-relay workspace enable --root /absolute/private/runtime --id app
./compute-relay token revoke --root /absolute/private/runtime --id tok_ACTUAL_RECEIPT_ID
```

Authority changes affect subsequent authorized work; they are not proof of remote cancellation.

## Apply or change an admission profile

With `serve` stopped:

```sh
./compute-relay profile apply --root /absolute/private/runtime --file /absolute/private/profile.json
./compute-relay profile show --root /absolute/private/runtime --name local-cpu
./compute-relay profile grant --root /absolute/private/runtime --workspace app --name local-cpu
```

A `(name, revision)` is immutable. Change policy by applying a new revision rather than editing
history. See [Application CLI](application-cli.md).

## Package and upload code

Review exactly what will be included:

```sh
./compute-relay bundle preview --root ./job-source --include main.py --include src
./compute-relay bundle create --root ./job-source --include main.py --include src --output ./code.tar.gz
./compute-relay bundle inspect --file ./code.tar.gz
```

Upload while `serve` is running:

```sh
./compute-relay object upload \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --file ./code.tar.gz
```

Keep the returned `object_id`. Upload is storage, not execution. See
[Bundles and import](packaging-and-import.md).

## Validate, admit and inspect a job

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
  --idempotency-key application-job-001

./compute-relay job status \
  --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --id job_ACTUAL_RECEIPT_ID
```

Use an idempotency key you can preserve with the original request. Replaying the identical submit
recovers the original receipt; changing the request under the same key conflicts.

**Do not submit again merely because the response was lost.** See [Recovery](recovery.md).

## Explicit job controls

Controls require the exact attempt and `operate` scope. They are not a sequence to run on every
job:

```sh
./compute-relay job cancel --workspace app --token-file /absolute/private/runtime/app-token \
  --id job_ID --attempt att_ID --idempotency-key cancel-001

./compute-relay job reconcile --workspace app --token-file /absolute/private/runtime/app-token \
  --id job_ID --attempt att_ID --idempotency-key reconcile-001

./compute-relay job collect --workspace app --token-file /absolute/private/runtime/app-token \
  --id job_ID --attempt att_ID --idempotency-key collect-001

./compute-relay job retry --workspace app --token-file /absolute/private/runtime/app-token \
  --id job_ID --attempt att_ID --idempotency-key retry-001 \
  --reason "explicit retry after confirmed failure"

./compute-relay operation status --workspace app \
  --token-file /absolute/private/runtime/app-token --id op_ID
```

Cancellation is intent, reconcile is observation, and collect is transfer-only. Normal handlers do
not instantiate a provider or collector. A retry is a deliberate new compute attempt and must not
be used to recover a missing HTTP response.

## Read already published artifacts

If the attempt already has a committed publication:

```sh
./compute-relay job artifacts --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --id job_ID --attempt att_ID --limit 100

./compute-relay artifact show --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --job job_ID --attempt att_ID --id art_ID

./compute-relay artifact download --workspace app \
  --token-file /absolute/private/runtime/app-token \
  --job job_ID --attempt att_ID --id art_ID \
  --output /absolute/private/results/chosen-name.bin
```

Downloads are create-only and verify bytes plus final stream acknowledgement. They do not fetch
provider output or make a queued job produce results. See [Artifact delivery](artifact-delivery.md).

## When a command result is uncertain

Preserve the original request, idempotency key, receipts, runtime root and executable. Do not
delete state, reset an attempt, or create a replacement submission to make the state look clean.

Use [Safe recovery](recovery.md) to distinguish:

- lost local response vs committed operation;
- cancellation intent vs remote termination;
- local timeout/shutdown vs remote cancellation;
- terminal provider status vs published artifact availability;
- transfer retry vs a new compute retry.

Unknown stays unknown until the original attempt is observed or reconciled.

## If you are trying to run the Kaggle qualification path

That workflow consumes provider/account capability and has separate authorization and evidence
rules. It is intentionally not part of this normal runbook. Start with
[Development documentation](development/README.md) and the
[provider qualification docs](development/providers/README.md).
