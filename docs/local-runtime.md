# Local operator commands and admission-only HTTP lifecycle

> **Task:** M5-01a in PR #25, in review until owner merge. This is one slice of M5-01.
> **Requirements:** API-01/03, DX-01, preserving SEC-02 and durable ownership.
> **Mode:** local-admission-only; no provider workers, remote calls or live acceptance.

PR #24 merged the M4-06 acceptance harness, not its live GPU acceptance. M5-01a now exposes
local initialization, workspace/token administration, schema validation and guarded HTTP
serving through the main executable. Application client commands, profile/provider composition,
artifact/log routes and cleanup commands remain later M5-01 slices. M5-02 TOML is not supplied.

## Build and start a private local installation

Use the pinned Go toolchain and locked repository dependencies:

```text
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay help
./compute-relay init --root /absolute/private/runtime
./compute-relay workspace create --root /absolute/private/runtime --id app
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope read --scope write --ttl 24h --output /absolute/private/runtime/app-token
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

On Windows build `compute-relay.exe` and use the equivalent absolute Windows paths. Paths are
illustrative; their parents must already exist. The initial runtime directory must not exist.
The token destination's parent must be private and its file new. The initialized root itself
is an acceptable private parent, outside the protected state/input/result subdirectories.
Keep executable, state and token files outside source control and public/static-file roots.

The token is written once to the specified private file, with a final newline. Stdout contains
only a non-secret receipt (ID, workspace, scopes, expiry, delivery status), not the secret or its
digest. Give the application that file through the operator's existing secret-provisioning path;
do not paste it into chat, command arguments or documentation. This is an application token,
not a provider credential. No Kaggle credential lookup or provider construction occurs here.

`serve` stays in the foreground. The listening event includes the actual address, mode and
`dispatch_enabled=false`. The default address is `127.0.0.1:7331`; an explicit loopback port 0
requests an OS-assigned port. Literal IPv4/IPv6 loopback is required. DNS names, wildcard/private
network addresses, scoped IPv6, mapped aliases and noncanonical ports are rejected. There is no
remote-exposure, proxy, TLS-bypass or provider-enable flag in this slice.

**New workspaces have no allowed profiles.** They can use authorized local object operations,
but cannot admit a new job under a nonexistent/unallowed profile. No fictional default provider
or profile is installed. Existing job/control handlers are composed and tested with explicitly
seeded test-only profiles; no production profile configuration command or SQL-editing workaround
is advertised. Even an admitted job remains local: no scheduler/dispatch/collection worker runs.

## State identity and local administration

`init` creates SQLite metadata, separate private input/result stores and a canonical internal
`.compute-relay-runtime` marker binding all three identities. The final marker is written only
after initialization. Existing or partially initialized directories are rejected, not reset.
Other commands never create a missing installation or silently recreate absent database/store
markers. Corrupt markers and swapped input/result roots fail closed.

Opening takes exclusive OS locks for the root, SQLite and both blob stores. **Stop serve before
running administrative commands**, including local state inspection. A second command/process
fails rather than racing an active server. No application-admin HTTP endpoints are introduced.
The package's trusted composition methods require serialized operator access and closing only
after all consumers finish; the CLI owns that lifecycle.

```text
./compute-relay state --root /absolute/private/runtime
./compute-relay workspace show --root /absolute/private/runtime --id app
./compute-relay workspace disable --root /absolute/private/runtime --id app
./compute-relay workspace enable --root /absolute/private/runtime --id app
./compute-relay token revoke --root /absolute/private/runtime --id tok_REPLACE_WITH_RECEIPT_ID
```

The token-ID placeholder must be replaced by the receipt ID; it is not a secret value or a
literal valid ID. Workspace create is create-only. Enable/disable changes an existing workspace
and preserves its profile allowlist. Disabling authority is durable; existing authorization
services enforce scopes, workspace boundaries, expiry and revocation on subsequent HTTP work.
`state` reports installation/schema and local mode, not a job/GPU status or a backup certificate.

The internal marker is not runtime configuration, and its canonical bytes must not be edited.
Old component-smoke or Kaggle acceptance directories cannot be adopted simply by renaming them.
Opening a valid directory may run existing migrations: local inspection is not necessarily a
byte-for-byte read-only filesystem operation. No migration downgrade/reset or metadata pruning
is supplied. Preserve matching database, both blob roots and markers for coordinated recovery;
copying only SQLite does not copy blobs, stop remote work or establish complete recoverability.

## Token delivery and limits

Issue requires explicit nonduplicate scopes from read/write/operate. Default expiry is 24 hours;
allowed TTL is one minute through thirty days. Existing destination files are never overwritten.
Parent aliases are resolved before rejecting destinations inside state, inputs or results;
case aliases of protected names are also rejected. The destination currently must permit a
same-volume relative-path check against the installation. Parents must remain operator-controlled
and stable; this is not a sandbox against hostile same-user replacement or host administrators.

SQLite stores the digest before secret-file delivery. Those resources are not one atomic
transaction. If file writing/sync/publishing fails or panics, the command attempts revocation
with an independent five-second context and returns the retained non-secret token ID with
`revoked-after-delivery-error` or `revocation-unconfirmed`. Do not use a failed/partial delivery.
A crash or stdout failure can occur after durable effects; a nonzero exit is not rollback.
Bounded expiry limits orphan authority, and operators must preserve receipts and investigate
uncertain delivery rather than overwriting paths or assuming revocation succeeded.

Owned byte buffers are cleared after delivery. Immutable strings, OS caches, token files and
arbitrary retained copies are not securely erased by that operation. Protect the explicit token
file and any backup containing it. No automatic token rotation, file deletion or secret export
is provided. Non-serve local commands have a thirty-second context; OS operations/callbacks must
cooperate, so this is not a hard-real-time guarantee.

## HTTP and shutdown boundaries

The host composes existing `api`, `auth`, `objects`, `admission` and `operations` services with
actual SQLite/filesystem repositories. It retains their body, concurrency, rate, Host/Origin,
workspace and idempotency rules. Health is anonymous; readiness/info and application operations
retain authentication. Local import and HTTPS ingestion are not enabled, and no new artifact,
log, event-stream or administration route is introduced. Existing route/schema definitions
are unchanged. See [API status](../api/README.md), [objects](auth-and-objects.md) and
[controls](operations.md) for their individual contracts.

Every accepted HTTP response carries `X-Compute-Relay-Mode: local-admission-only`. `/readyz`
means the configured local dependencies are ready, **not** provider/GPU availability, a usable
profile or a running worker. A 202 admission/control receipt is durable local metadata, not proof
of remote execution or completed collection. Provider credentials, subprocesses, retention
sweep and remote cleanup are absent from this composition.

Interrupt/SIGTERM reaches the main command context. Shutdown stops admitting new handlers,
allows ten seconds for active requests, then forcibly closes connections if needed and reports
failure. In both cases, the lifecycle joins all admitted handlers before returning permission to
close stores/release locks. A forced close alone is not proof that handlers have exited. Existing
handlers do not hijack connections; a custom callback that ignores closure/context can delay
shutdown rather than allowing unsafe store reuse. Process termination still relies on durable
recovery and is not remote cancellation.

This follows the standard [net/http Shutdown/Close contract](https://pkg.go.dev/net/http#Server.Shutdown),
reviewed 2026-09-16. No HTTP request/response logging or raw server exception dump is enabled.

## Local validation is not admission

```text
./compute-relay validate --file ./job.json
```

The file must be regular, at most 1 MiB, stable through reading and satisfy the existing strict
job schema/semantics. Required arrays such as `inputs` must be explicit. Success emits
`schema-valid-local`, a canonical specification digest, `admitted=false` and `provider_checked=false`.
It opens no runtime state and checks no actual object/profile/account availability. Actual
admission still needs current authority, allowed profile, input ownership and its transaction.

Help/invalid flags do no state work. Cross-command and duplicate singleton flags are rejected;
scopes may be repeated only as distinct explicit values. The new local diagnostics and unknown
command branch do not echo raw arguments, token values or private paths. Existing bundle/version
commands remain separate, and bundle work now inherits the executable's cancellation context.

## Tests and remaining work

```text
go test -race ./internal/operatorcli ./internal/runtimehost ./internal/cli ./cmd/compute-relay
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Real SQLite/blob/loopback tests cover initialization/reopen/locking, missing/swapped evidence,
workspace/expiry/revocation boundaries, token-file delivery compensation, Host/Origin rejection,
uploaded bytes, original admission receipt replay and queued cancellation without remote intent.
Tests seed profiles explicitly for admission; production initialization does not. Separate child
processes invoke the real main entry for init/admin/serve/state and authenticated HTTP. Unix
children receive interrupt; Windows tests exercise abrupt process death/reopen instead of claiming
POSIX signal delivery. Symlink tests explicitly skip when the host cannot create test symlinks.

Local Go 1.23.2 ran the exact standard-library parser's vet and race tests ten times, and the exact
HTTP lifecycle's vet/race tests five times in an unshipped harness supplying only its ErrState
sentinel. These are parser/lifecycle evidence, not full local SQLite/pinned-repository integration.
Full pinned Go/native/race evidence belongs to final-head CI in PR #25. No replacement driver,
dependency downgrade or production test hook is shipped. No live provider/GPU call is performed.

See [ADR-0021](decisions/0021-local-runtime-lifecycle.md) and the
[implementation plan](implementation-plan.md). Stop at PR #25 for owner merge. Parent M5-01
remains incomplete until its later client/profile/provider/route slices; M4 live acceptance and
M5-02 configuration remain separate, and no release-ready multi-job compute service is claimed.
