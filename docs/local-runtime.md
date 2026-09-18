# Local runtime

Build and operate a private installation. `serve` is admission-only by default; an explicitly
configured [Kaggle runtime](kaggle-runtime.md) can additionally start bounded provider dispatch and
collection workers. See [current status](status.md) before depending on a capability.

## Build and initialize

From the repository root with the `go.mod` toolchain:

```sh
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay help
./compute-relay init --root /absolute/private/runtime
./compute-relay workspace create --root /absolute/private/runtime --id app
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope read --scope write --scope operate --ttl 24h --output /absolute/private/runtime/app-token
```

Replace paths with operator-owned locations outside the checkout. Their parents must exist;
the installation and token file must be new. On Windows use `compute-relay.exe` and absolute
Windows paths. No local GPU or Docker is required.

Token issuance writes the secret once to the private output file and prints a non-secret receipt.
Keep the receipt's token ID for revocation. This is an application token, not a Kaggle credential.
Issue only the scopes the application needs; `operate` is required for job controls.

## Configure admission and serve

Apply an immutable profile and grant it to the workspace using [application CLI](application-cli.md)
before starting the server. Initialization does not create a default profile or fake provider.

```sh
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

Without provider flags the server stays in the foreground and reports `local-admission-only`,
`dispatch_enabled=false`. The default is literal loopback `127.0.0.1:7331`; port 0 requests an
assigned port. DNS names, wildcard/remote/private-network listeners and ambiguous aliases are
rejected. Provider workers require the complete explicit flag set in
[Kaggle runtime](kaggle-runtime.md); no flag enables remote HTTP exposure or a TLS bypass.

Application HTTP commands can run concurrently with serve. **Stop serve before local
administration**, including `state` and profile changes, because all acquire the installation's
exclusive locks. Never delete a lock file to bypass an active process.

```sh
./compute-relay state --root /absolute/private/runtime
./compute-relay workspace show --root /absolute/private/runtime --id app
./compute-relay workspace disable --root /absolute/private/runtime --id app
./compute-relay workspace enable --root /absolute/private/runtime --id app
./compute-relay token revoke --root /absolute/private/runtime --id tok_ACTUAL_RECEIPT_ID
```

Use real receipt IDs. Workspace enable/disable preserves its profile grants. Disabling/revoking
authority affects subsequent authorized work; it is not remote cancellation.

## Storage and token safety

The installation binds SQLite and separate input/result stores with private identity markers.
Only `init` creates a new installation. Missing/corrupt/swapped stores or markers are rejected,
not recreated. Do not edit markers, rename acceptance directories into this format, or manually
seed SQL. Opening supported state may apply existing migrations; local inspection is not a
byte-for-byte read-only filesystem operation.

Token scopes are explicit `read`, `write`, `operate`; TTL defaults to 24 hours and is bounded
from one minute to thirty days. Output requires a new file in a private, stable, same-volume
parent outside protected state/input/result subdirectories. Database insertion and secret-file
delivery are not one atomic transaction. A delivery failure attempts revocation and reports
`revoked-after-delivery-error` or `revocation-unconfirmed` with the token ID. Do not use a partial
file or assume a nonzero exit rolled back authority. A failure writing stdout can follow a
successful effect. Preserve receipts and inspect state before retrying.

Buffer clearing does not securely erase all OS/string copies. Protect tokens and backups from
other users; the local operator and same-user filesystem writers remain trusted.

## HTTP and shutdown

Only `/healthz` is public. Readiness/info and application routes use authentication, workspace,
Host/Origin, body/rate/concurrency and idempotency guards. The host's optional local-import and
HTTPS-ingestion services remain disabled. See [API contracts](../api/README.md).

`/readyz` establishes local HTTP/storage readiness, not proof of GPU allocation or job success.
A `202` means local admission/control committed. Admission-only serve starts no provider workers.
Provider-enabled serve starts the configured bounded dispatcher/collector after its read-only
account check; artifact routes still expose only committed publications.

Interrupt/SIGTERM stops new handlers, allows ten seconds for graceful completion, then closes
connections on timeout and reports failure. Store closure and lock release still wait for all
admitted handlers. A callback ignoring cancellation can delay shutdown; closing connections
alone is not proof that callbacks stopped.

## Validate and verify

```sh
./compute-relay validate --file ./job.json
```

This checks a regular bounded job file against schema/semantics only, with `admitted=false` and
`provider_checked=false`. It does not open state or resolve real objects/profiles/accounts.
`job validate` in the application guide performs contextual server validation, still without
admitting work.

Use the [operator checklist](development/validation-checklist.md) for a reproducible host test,
[storage](development/storage.md) for coordinated recovery and [artifact delivery](artifact-delivery.md)
for result reads. Local serving does not qualify live compute.
