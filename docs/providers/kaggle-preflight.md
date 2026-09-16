# Kaggle local and explicitly read-only preflight

> **Task:** M4-01 offline/read-only foundation; PR #19 in review until owner merge.
> **Evidence:** implemented and tested offline. No live credential/account probe was run.
> **Gate:** M1 live acceptance and M1-08 go remain blocked; this is not a batch adapter.

The owner requested M4 after merging M3-08 in PR #18. This bounded preparatory work uses
proposal section 23.6's permission to implement offline pieces without credentials. It does
not waive the private-input, finite-execution, identity/result or live-provider acceptance
gates. No staging, submission, remote cleanup or production server is wired by this task.

## What is implemented

`internal/provider/kaggle` provides immutable non-secret preflight configuration and an
explicit `Check(ctx, mode)` service. `internal/credentials.Environment` implements the existing
scoped credential port for allowlisted environment references. `cmd/kagglepreflight` is a
finite developer/operator utility, separate from the production `compute-relay` executable.

The package deliberately does **not** implement `provider.Provider` or register with dispatch.
Every report has `batch_ready=false`, including a successful authenticated read. Instance
labels/revisions identify this configuration, not new durable profile records or a migration.
Existing frozen job/profile/account snapshots remain unchanged. Unified TOML/configuration,
provider registration and production `serve` are separate composition tasks.

## Install the existing pinned client environment

Follow [the isolated client guide](../../tools/kaggle-client/README.md). No dependency or
lockfile is changed here. The accepted runtime series is Python **3.11**, with distribution
metadata exactly **kaggle 2.2.4** and **kagglesdk 0.1.35**. Existing CI uses Python 3.11.16 and
uv 0.12.13. Use the absolute interpreter path inside that locked environment; do not replace
it with a PATH lookup or a shell wrapper. On Windows use its `Scripts/python.exe`.

A local check reads installed distribution metadata without importing `kaggle` or the SDK.
It proves version matching, not package integrity, authentication, provider compatibility or
GPU eligibility. Install from the reviewed lock; the interpreter and its site packages are
trusted operator-controlled code. Version strings are not cryptographic attestation.

## Non-secret configuration

Create a regular JSON file, at most 8 KiB. All five fields are required; unknown/case-alias
fields, duplicate keys, nulls and trailing values are rejected. For Linux/macOS, adapt this
shape to the operator's actual paths and canonical account name:

```json
{
  "instance_id": "kaggle-personal",
  "revision": "preflight-1",
  "account_name": "your_canonical_username",
  "credential_ref": "env:CR_KAGGLE_TOKEN",
  "python_executable": "/absolute/path/to/compute-relay/tools/kaggle-client/.venv/bin/python"
}
```

The path is illustrative, not a preinstalled location. Windows paths must be absolute and
JSON-escaped, for example `C:\\workspace\\compute-relay\\tools\\kaggle-client\\.venv\\Scripts\\python.exe`.
This small internal configuration is not the normalized public runtime-config/TOML schema.
The account parser deliberately supports a conservative lowercase ASCII subset; it does
not claim to describe every possible upstream username.

Only **explicit environment references** and **opaque API bearer tokens** are supported in
this component. Provision the selected variable securely in the operator's runtime, outside
chat, source control and shell-history examples. Do not include a `Bearer ` prefix, whitespace
or newlines in the value. Token bounds are 1–8192 printable ASCII bytes.

There is no `.env` loader, home-directory search, legacy username/key fallback, credential
file mode, OAuth/browser login, refresh or shared secret cache. A reference such as
`env:KAGGLE_API_TOKEN` can be selected explicitly, but its name has no special fallback status.
Rotation is read fresh on every explicit authenticated check and must still match the same
configured account. Changing the service's caller-owned Config after construction has no effect.

## Local default and network opt-in

From the repository root with the pinned Go toolchain:

```text
go run ./cmd/kagglepreflight --config ./kaggle-preflight.json
```

This checks local versions only. It does not resolve a token or contact Kaggle. Ordinary Go
build/toolchain downloads, when dependencies are not already installed, are separate from
preflight behavior. Help and invalid configuration do not construct an authenticated check.

Only an operator who explicitly authorizes the read-only account probe should run:

```text
go run ./cmd/kagglepreflight --config ./kaggle-preflight.json --allow-read-only
```

The flag is credential/network authorization for this invocation, **not** authorization for
GPU, staging, submission or cleanup. The service checks local pins first, then sends the token
to a fixed isolated Python helper over stdin. It never puts the token in arguments, child
environment variables, configuration files or diagnostic output.

The helper uses two official SDK operations in order: token introspection, then accelerator
quota-statistics retrieval. These are read-only RPCs transported as HTTP POSTs. Introspection
must report an active token and a server-provided username exactly matching `account_name`.
A configured username is not authentication evidence. A mismatch stops before the quota call;
there is no fallback to another account/profile or a new compute attempt.

The command emits a JSON envelope with instance label, revision, UTC check time and a closed
enum report. It excludes actual account names, credential references, raw process output and
provider exception/response bodies. Relevant fields mean:

| Field | Interpretation |
|---|---|
| `local=ready` | The interpreter's installed metadata matched the required versions. |
| `authentication=verified` | This invocation received an active-token introspection response. |
| `account_binding=matched` | The server username matched this immutable configuration. |
| `quota=available` | The quota endpoint supplied a GPU-quota object; not a positive allowance or GPU reservation. |
| `quota=unknown` | The response did not supply that object; no limit/remaining value is invented. |
| `quota=unavailable` | Quota retrieval failed after account verification. |
| `batch_ready=false` | No batch/private-staging/GPU capability has been enabled. |

Numeric quota interpretation, precision, freshness, shared-account scheduling and adapter
integration remain M4-04. Authentication is a point-in-time observation, not a permanent grant.
`problem=none` produces exit code 0, including a truthful unknown quota. A negative diagnostic
or process/configuration failure produces 1; invalid flags produce 2. Process failure emits
only a constant sanitized diagnostic, not the underlying exception. A successful read must
not be persisted as an execution permit or used to close M1's full live gate.

## Transport, process and secret boundaries

The helper is embedded in Go and executed with fixed arguments, Python isolated mode and an
empty temporary home/cwd. Ambient proxy, Kaggle and Python search-path variables are removed;
only Windows bootstrap variables are retained when needed. There are no arbitrary endpoint,
command, shell or child-environment overrides in Config.

The official SDK constructs requests and parses typed responses. A deliberately version-bound
hook accesses its reviewed private `_session` field through public `http_client()`. It rejects
an unfamiliar session layout. The hook limits calls to the two exact production HTTPS RPC
URLs, in order, with no redirects, retries, proxy overrides or disabled certificate validation.
It uses requests' adapter directly rather than Session redirect processing, so a redirect body
cannot be prefetched ahead of the byte guard. This is a private transport-layout dependency,
not a claim of a stable public SDK session-injection API; see ADR-0015 and the reviewed sources.

Each response must be a bounded UTF-8 JSON object, with duplicate keys and invalid constants
rejected before SDK decoding. Bodies are limited to 64 KiB. A late read error remains failure.
Per-request connect/read timeouts are 5/10 seconds; each child has a 30-second Go deadline and
an independent 25-second Python watchdog. The service's 60-second budget includes its single
per-instance slot, local checks and credential resolution. Captured stdout/stderr are limited
to 16 KiB each; stderr is discarded and invalid stdout is never reflected. The command handles
interrupt/SIGTERM; the watchdog also bounds the helper without a live Go supervisor.

This fixed helper launches no descendants. It is **not a general workload/process-tree
supervisor**, and no admitted code runs here. The interpreter, installed packages, resolver
callback and local operator are trusted. A noncooperating custom resolver or an uninterruptible
OS operation is not made hard-real-time by a context deadline.

The environment resolver clears its owned byte slice on callback success/error/panic; process
output buffers are cleared after use. The original OS environment, immutable language strings,
pipe/kernel buffers and arbitrary retained copies cannot be securely erased by this API.
This is bounded lifetime and logging hygiene, not protection from the host owner or a memory
forensics guarantee. Keep aliases/revision labels non-secret as well.

## Tests and evidence

```text
go test -race ./internal/credentials ./internal/provider/kaggle ./cmd/kagglepreflight
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Inside `tools/kaggle-client`, run the existing locked offline suite:

```text
uv run --locked python -m unittest discover -s tests -v
```

Go tests cover reference allowlisting/rotation/buffer clearing, local-before-credential order,
configuration/report rejection, serialized/cancelled checks, fixed-process isolation, output
limits, timeout and CLI opt-in/redaction. Python tests cover bounded transport, ordered allowlist,
redirect refusal before body reads, malformed/oversized/duplicate responses, missing credentials,
exception redaction and independent watchdog termination. The real pinned SDK tests replace
only network transport with fixtures and exercise its actual request/response types, wrong
account, inactive token, missing name, authentication/quota errors and redirects.

The existing Kaggle-client workflow now watches the embedded-helper package as well as its
own tools, so future helper-only changes cannot bypass SDK tests. It retains read-only GitHub
permissions and uses synthetic canaries, not provider credentials. No dependency or runner
asset was changed. Exact final-head Go/native/race and locked SDK results are recorded in PR #19.

Local engineering uses Go 1.23.2 and Python 3.13. The new Go code passed vet/race/repeat tests
in an unshipped harness containing the exact credential-port interface declaration. This is
not the full repository/pinned-stack integration. Six Python transport/protocol/watchdog tests
ran locally; the real-SDK test was skipped locally because the SDK is absent. That test runs
against the actual locked stack in CI. No authenticated live call or complete M1/M4 acceptance
is claimed.

## Reviewed primary sources and next gate

Reviewed 2026-09-16 at the repository's existing pins:

- [Kaggle CLI v2.2.4 authentication and quota implementation](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/src/kaggle/api/kaggle_api_extended.py).
- [SDK v0.1.35 client constructor](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_client.py).
- [SDK transport/auth/session behavior](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_http_client.py).
- [Official introspection service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/security/services/oauth_service.py) and [quota service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py).

These are source/fixture findings, not observed account compatibility. Preserve the dated M0/M1
research rather than upgrading its evidence labels. An authorized operator can use the explicit
read-only utility for narrowly scoped evidence collection, but private staging, finite GPU,
identified output recovery and the M1-08 go decision still require their separate procedures.
See [ADR-0015](../decisions/0015-read-only-kaggle-preflight.md) and the
[implementation plan](../implementation-plan.md). Stop after PR #19 for owner review/merge;
M4-02 and live-provider work are not started by this PR.
