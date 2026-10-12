# Attempt-scoped workload secrets

Managed SSH jobs can declare required workload secret slots and supply their values after
[remote preparation](preparation-gates.md). This requires the provider capability
`ephemeral_workload_secrets` and `execution_control: "prepare_then_start"`. Provider login
credentials remain separate; a job cannot resolve or reuse a saved connection credential.

```json
"workload_secrets": [
  {"name": "ingest", "exposure": "file_and_local_process_arguments"}
]
```

Names match `[a-z][a-z0-9_]{0,31}` and are unique. `file` exposes a read-only file to the authorized
workload. `file_and_local_process_arguments` additionally records explicit consent for that
workload to pass its value to a local tool that only accepts an argument. Relay's own SSH
commands, Docker configuration, environment, labels and staged files never carry the values.
The exposure declaration is an application trust policy, not a sandbox between the workload
and its own child processes.

## Supply and start

Use the workspace's `execute` scope for all secret endpoints, including receipt reads. Supply
and revoke require an `Idempotency-Key`; `read` access alone can see required slot names and
availability through readiness but cannot read a secret authorization. Values are write-only.

1. Upload ordinary immutable inputs, admit the job, authorize preparation and wait for
   `/jobs/{job}/readiness` to report `ready` with a verified `revision`.
2. POST `/v1/workspaces/{workspace}/jobs/{job}/workload-secrets` using a fresh stable key:

   ```json
   {
     "attempt_id": "attempt_1",
     "readiness_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
     "expires_at": "2099-10-12T12:00:00Z",
     "values": {"ingest": "c3ludGhldGljLWV4YW1wbGU="}
   }
   ```

   This illustrative timestamp must be replaced with a future canonical UTC timestamp no more
   than 24 hours away. `values` maps every declared slot, and no other slot, to standard base64.
   Maximums are eight slots, 8 KiB decoded per slot, 32 KiB decoded in total and a 48 KiB JSON
   body. Supply authorizes neither staging nor compute. Its `202` receipt contains only identity,
   expiry and slot names; its `Location` supports a metadata-only GET.
3. POST the explicit `/start` grant with the same attempt/readiness revision and the returned
   `secret_authorization_id`. The required `execution_deadline` must be no later than the secret
   expiry. Missing, revoked, expired or unavailable material prevents start.
4. Read the original attempt and its logs/results. A consumed secret receipt means the unique
   delivery/start intent was committed; it does not prove successful delivery or execution.

Supply bytes through a protected client process and request body. Do not place a real value in
job JSON, command lines, shell history, URLs used to call Relay, checked-in examples or debug
request logs. Base64 is encoding, not encryption; protect the Relay HTTP transport as described
in the [HTTP API reference](../api/README.md).

## Restart, replay and revocation

There is one immutable supply authorization per attempt. Repeating its exact logical request
with the original key returns the original receipt. Changing values, expiry, revision or key
cannot replace an existing grant. After Relay loses process memory, readiness reports
`material_lost`: an explicit exact replay may restore the original **unused** material. Merely
reading status or replaying the start grant does not restore it. A missing installation replay
key with existing receipts fails closed; restore consistent protected installation state.

Revoke unused material with POST
`/v1/workspaces/{workspace}/jobs/{job}/workload-secrets/{authorization_id}/revoke` and
`{"attempt_id":"attempt_1"}`. Revocation has its own stable idempotency key. Revocation cannot
undo consumed authority. Cancel the exact attempt and confirm remote inactivity if it may
have started. Revoked, expired, cancelled or consumed material cannot be rearmed by supply
replay. A new attempt needs new authority, subject to the ordinary retry and inactivity rules.

The start transaction consumes secret and start authority together with the original submission
intent. A crash or lost acknowledgement afterward keeps that intent uncertain. Recovery
inspects the original container; it never resends values, repeats delivery or starts a replacement.
An incomplete delivery causes the trusted remote bootstrap to fail within 30 seconds, bounded
also by the absolute deadline, before setup, dependency probes or payload execution.

## Storage and exposure bounds

Unused values live in a bounded Relay memory pool (at most 16 batches). Cancellation, expiry,
revocation, consumption or shutdown clears owned buffers; a one-second sweep removes material
whose attempt is no longer eligible. This is logical buffer clearing, not a claim of forensic
memory erasure. SQLite stores only non-secret metadata, receipts and a keyed replay fingerprint.
The 32-byte replay key is a protected installation file outside SQLite and is not a workload value.

A stopped prepared container has an empty private 128 KiB tmpfs. Only the unique authorized
start can send the packet through fixed Docker exec stdin. The receiver verifies attempt, nonce,
manifest, authorization, expiry and slot names, then writes files with mode `0400` under
`CC_SECRET_DIR` (a non-secret path). Its one-shot marker prevents replacement. Slot files are
removed before result export; the tmpfs disappears when the container stops. Docker documents
that tmpfs may be written to swap; host memory, VM snapshots and administrative access remain
operator concerns. [Docker tmpfs documentation](https://docs.docker.com/engine/storage/tmpfs/)
(reviewed 2026-10-12).

Captured and mirrored logs redact known raw values plus URL and JSON string forms, including
matches across capture chunks. Result collection rejects required artifacts containing known
values and omits optional ones, including matching paths. Control output is scrubbed before
publication. Suppress sensitive tool diagnostics in the workload as well. These checks reduce
accidental disclosure; they cannot stop an authorized workload from encoding or transmitting its
own input, and they do not conceal values from host administrators or local process inspection
when argument exposure was authorized. [Docker exec lifetime and stdin](https://docs.docker.com/reference/cli/docker/container/exec/)
(reviewed 2026-10-12).

See [execution deadlines](execution-deadlines.md) for clock and shutdown bounds. Stopping a
payload does not revoke an external credential or complete an external session; the application
owns those actions and must plan for its own unavailability.
