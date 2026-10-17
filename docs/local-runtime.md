# Local runtime

Relay is a single Go executable with a loopback HTTP/JSON API, SQLite state and separate private
input/result stores. `serve` always starts the managed composition. No connection means no
provider calls. Connection verification and every execution permit remain explicit.
See [getting started](getting-started.md) for installation commands.

## Serve policy

```sh
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

Optional policy flags are `--managed-machine-shape cpu|NvidiaTeslaT4|NvidiaTeslaP100`,
`--managed-max-wall-seconds 1..86400` (a startup default for every provider type; an SSH
connection configuration may allow up to 604800), `--managed-max-workers 1..16`,
`--managed-allow-internet` and `--managed-credential-store file|keychain`.
Defaults are CPU, 1800 seconds, two workers, internet disabled and file credentials for a new
installation. Policy changes affect future selections; they do not rewrite admitted jobs.
The machine shape applies to Kaggle; SSH stays CPU. No flag grants attempt authorization.

The default listener is literal loopback `127.0.0.1:7331`; port 0 requests an assigned port.
DNS names, wildcard and remote listeners are rejected. There is no remote-exposure or TLS-bypass
flag. Install the executable yourself and run it against the existing private root when upgrading;
there is no companion installer. The embedded Python runner executes remotely.

## Local administration and authority

Application HTTP requests run concurrently with serve. Stop serve before local administrative
commands because each acquires the installation's exclusive root lock:

```sh
./compute-relay state --root /absolute/private/runtime
./compute-relay workspace show --root /absolute/private/runtime --id app
./compute-relay workspace disable --root /absolute/private/runtime --id app
./compute-relay workspace enable --root /absolute/private/runtime --id app
./compute-relay token revoke --root /absolute/private/runtime --id tok_ACTUAL_RECEIPT_ID
```

Token scopes are `read`, `write`, `operate`, `manage`, `execute`; TTL defaults to 24 hours and is
bounded from one minute to thirty days. Management and execution are separate from ordinary job
production. Revocation/disable affects future authority checks; it does not cancel remote work.

Token delivery requires a new file in a private stable parent outside protected storage.
Database insertion and file delivery are separate effects. A delivery failure attempts revocation
and reports `revoked-after-delivery-error` or `revocation-unconfirmed` with the token ID.
A stdout failure can follow a committed effect; inspect state before retrying.

## State and credentials

Only `init` creates an installation. The host owns one top-level `runtime.lock`; state and blob
stores share that lock. `.compute-relay-runtime` and the database installation row bind the
installation, while blob format markers and separate `.retention-id` files bind input/result roots.
New hosted roots omit `state/.installation` and child locks. An existing legacy `.installation`
is checked and removed only after complete host validation; existing lock files are preserved.
Missing, corrupt or swapped stores/identity markers are rejected rather than recreated.
Do not unlink locks, edit markers or seed SQL manually. Opening supported state applies sequential
migrations, including through local inspection. A state read is therefore not a byte-for-byte
read-only filesystem operation.

Credentials belong in the installation's selected [vault](managed-connections.md#choose-credential-storage).
New installations use a private unencrypted file; existing Keychain identity is retained through
the native macOS API. Changed executables may need macOS access permission. Preserve the original
binary and vault if access is denied; no fallback or token re-entry is needed to retain identity.

## HTTP and worker lifecycle

Only `/healthz` is public. Readiness, info and application routes enforce authentication, workspace,
Host/Origin, body/rate/concurrency and idempotency guards. `/readyz` proves local HTTP/storage
readiness, not account validity, GPU allocation or job success. A 202 is a durable local receipt.
See the [HTTP reference](../api/README.md).

Dispatch, collection, cleanup and other background workers share an error policy: transient
store errors back off and resume while HTTP keeps serving; provider and transport failures become
attempt or operation evidence, or wait for a later observation; invariant/corruption errors stop the
runtime with `ErrState`. Serve then exits 1 and adds one stderr line after the generic diagnostic,
naming the stopping worker and a closed error class, such as
`Runtime stopped: service=dispatch class=state_corrupt.`; it never includes provider, path or OS
text. Renewed fenced leases prevent a stale worker from committing progress.
A lease expiring does not prove its callback stopped or release unknown remote capacity.

Interrupt/SIGTERM stops new handlers and allows ten seconds for graceful completion, then closes
connections on timeout and reports failure. Store closure and lock release still join admitted
handlers and workers. A callback ignoring cancellation can delay shutdown; closed connections
alone do not prove side effects stopped. Local shutdown never means remote cancellation.

Use [recovery](recovery.md) for coordinated backups, retention and upgrade behavior;
[compatibility](compatibility.md) states filesystem/platform limits.
