# Private attempt staging and separately verified readiness

> **Task:** M4-02, offline implementation in PR #20; owner-review gate pending.
> **Requirements:** DAT-04, OPS-05, SEC-03; preserves DUR-03 and frozen input identity.
> **Evidence:** real pinned-SDK fixtures and local durable-component tests, not live Kaggle.

M4-01's preflight foundation is merged in PR #19. This task supplies the next preparation-only
component. M1 live acceptance/M1-08 go and production batch registration remain separate gates.
No authenticated provider call, real upload, GPU allocation or remote deletion was performed
while implementing or testing this PR.

## One active implementation

The implementation is `internal/provider/kaggle/staging*.go` with an embedded `staging.py`.
PR #20 on `feat/m4-02-private-staging` is the active integration branch. The earlier exported
`checkpoint/m4-02-offline-staging` package is obsolete and must not be merged or cherry-picked.
It has a different marker/type model and content-dependent resource naming. Its archived
verification results are not evidence for this implementation. The owner requested removal
of that redundant branch; the PR conversation records whether the deletion has actually
succeeded. No duplicate implementation is added to compensate for unavailable deletion tools.

## Composition and durable authority

`NewStager(config, policy, credentialResolver, blobs, allowCreate)` composes a preparation-only
service. It implements `provider.PreparationObserver`, not the full `provider.Provider`.
`Prepare(ctx, plan, preparationID)` requires explicit creation enablement. That boolean is
operator configuration, **not a durable permit**: only the invocation that just received a
successful NEW M3 `BeginPreparation` commit may call it. Recovery must use
`ReconcilePreparation` with the original plan and operation, even after a not-found response.

The existing M3 transaction writes the journal, ownership intent, attempt state and event
before any staging side effect. It pins the first discovered resource reference and fences
subsequent state changes. Stager returns observations and never edits arbitrary SQL rows.
No migration, state-machine rewrite, implicit scheduler, new HTTP route or mutation CLI is
introduced here. The future batch adapter must retain the frozen account/configuration
binding and delegate preparation to this component; it must not expose a second create path.

Integration tests compose the actual Stager with real admission, filesystem blobs, SQLite and
the existing dispatch engine through a **test-only fake-provider wrapper**. The helper result
is synthetic in this tier. An independent read-only connection proves ownership is visible
before the helper is entered. Tests stop before submission and assert zero submission intents,
execution resources and compute events. The separate Python tier exercises real SDK wire types
with mocked HTTP. These tiers are not a single live end-to-end provider test.

## Frozen plan, dataset name and marker

The planner requires a validated `provider.Plan` with a complete workspace-scoped input
snapshot, matching instance/revision, original digests and a valid preparation operation.
No URL is fetched, archive extracted or workload command executed locally by staging.

A dataset slug is `crs-` followed by 40 hexadecimal characters derived from the prewritten
installation, workspace, instance, job, attempt and preparation-operation identities. The
name does **not** change when payload/configuration contents change under an existing intent.
A changed marker must conflict with the same resource, not silently choose another dataset.
Different attempts and operations have distinct names; there is no prefix-based adoption.

The payload consists of `relay-stage.bin`, `code.bin` and numbered `input-NNN.bin` files.
The marker records the attempt identity, preparation ID, frozen plan digest, file sizes/hashes,
logical input name/target mapping and license label. It excludes command/environment values,
credential references, source URLs, human job labels and host paths. The archive remains an
opaque code blob; this component neither expands it locally nor treats provider transforms
as proof that original bytes survived.

Policy defaults are a five-minute invocation budget and at most 4 GiB input bytes plus a
100 MiB code bundle and 64 KiB marker. Up to 64 input files plus code/marker are accepted.
Smaller byte limits are supported; timeout is configurable from one second to one hour.
These are local ceilings, not provider capacity or upload-time promises.

License metadata defaults to `copyright-authors`; `other` and `unknown` are the other supported
labels. The label does not grant upload rights or relicense user data. No automatic CC0 or
public-data fallback is supplied. Preserve the original staging policy with its configuration
revision across recovery; changing a license changes the marker, not the resource name.

## Creation is not readiness

Before resolving a credential or creating an upload session, the Go service checks local
client pins and independently reads every original object through verified size/hash/EOF and
Close acknowledgement. Missing/changed/extra bytes, a late read error or Close failure stops
before credential access. The helper independently hashes the actual bytes streamed later.

The fixed helper verifies an active token and server-returned account identity before dataset
operations. It reads the exact owner/slug first. Only an actual HTTP 404 from that exact read
can enter the newly authorized creation path. An authorization error, timeout, server error
or JSON body claiming code 404 is not absence. An existing resource is verified, not updated,
versioned or adopted solely because its name matches.

For each bounded file, the helper requests one official upload ticket and performs one signed
storage PUT. It verifies actual stream consumption, hash and bounded response EOF before the
single private dataset-create call. It never retries an upload or creation internally. A
false successful receipt, short/trailing stream, upload error or lost acknowledgement cannot
be converted into another create attempt.

**Pipe EOF is not source acknowledgement.** Go's stdin-copy goroutine closes the child pipe
on a source read failure as well as normal EOF. Even after the helper receives every declared
payload byte, the original source can still fail its final EOF probe or Close. The Go producer
therefore appends the fixed `stagingUploadComplete` trailer only after all payload sources
finish successfully. The helper consumes that exact trailer and final EOF before returning
from upload to `CreateDataset`. Missing/truncated/changed/trailing acknowledgement forbids
creation. This internal framing does not change the dataset marker or public API.

The trailer protects the cooperative Go/helper boundary; it is not cryptographic authorization
or proof against malicious same-process code. It neither replaces the M3 write-ahead intent
nor prevents a previously authorized external request from completing after a local timeout.
See the official [os/exec stdin-copy contract](https://pkg.go.dev/os/exec#Cmd).

Creation uses private visibility explicitly. The create receipt alone yields no readiness
claim: metadata, processing state and bytes must be observed separately. A discovered public
resource is returned as not-ready/private-false so M3 preserves its identity and moves to
attention rather than dispatching. The component never repairs privacy by publishing data.

## Read-only recovery and readiness verification

`ReconcilePreparation` sends the original marker/catalog but reads no local payload bytes.
It permits no upload ticket, PUT, dataset creation, update, new version or deletion. Missing
resources remain `not_found`; unknown/failed processing remains unresolved. Neither observation
rearms the M3 creation gate. Delayed processing remains pending rather than prematurely ready.

Readiness requires all of the following in one bounded verification invocation:

1. The exact owner/slug, positive dataset ID, version **1**, marker description and license
   match, and private visibility is explicitly true.
2. The provider processing state is READY. All catalogue pages are read with bounded cursors,
   page size and cycle detection; extra, duplicate, unsafe, missing or wrong-size files fail.
3. The marker is downloaded and hashed first, followed by every payload. Each file must match
   the original exact length and SHA-256, including successful EOF and stream closure. A late
   error after the last byte is failure. No archive is extracted and no downloaded bytes are
   republished as application artifacts.
4. Metadata and processing state are read again. Changed dataset identity, version, privacy,
   modification timestamp or readiness invalidates the observation.

The Go side validates the closed helper response again. Ready requires private=true and exact
verified file/byte totals. Unknown/not-found responses cannot contain a usable resource or
invent verification counts. The persisted reference includes numeric dataset ID, owner/slug,
version and marker digest. Once M3 has recorded that reference, replacement at the same slug
cannot retarget the original intent.

Verification is a point-in-time observation, not an immutable provider lock or GPU permit.
The execution path must continue to bind the correct dataset version and verify runner input
identity; those M4-03 duties are not waived by staging readiness.

## Transport and process boundary

The reviewed official SDK constructs RPC requests and parses typed responses. A version-bound
private session hook permits only introspection, upload-start, create, metadata/status/listing
and raw-file download RPCs on the exact Kaggle production API host. Each invocation permits
one expected call, preventing a hidden SDK retry from reusing its authority. There are no
kernel, dataset-update/version or delete operations in the allowlist.

Metadata JSON is bounded to 1 MiB and rejects duplicate keys, invalid UTF-8/nonfinite constants
and non-object documents. Data streams use bounded chunks and reject content encoding or size
mismatches. Raw download redirects preserve only validated metadata; their original response
bodies are never read. Signed storage URLs are restricted to HTTPS `storage.googleapis.com`
(optionally port 443), with no userinfo, fragments, control characters or backslashes. Other
CDNs/upload hosts fail closed pending review. Cloud transfers receive no account Authorization
or Cookie header; redirects, ambient proxies, retries and disabled TLS verification are absent.

Python runs in isolated mode with an empty temporary home/cwd and a restricted environment.
The token, request and payload are framed on stdin; credentials and private input bytes never
become command arguments or staging files. Stdout/stderr are bounded to 16 KiB each, diagnostics
are discarded, and reports expose only checked fields. The parent deadline and independent
helper watchdog bound a fixed leaf process. It starts no descendants and is not a general
workload supervisor. Context-aware readers/resolvers must cooperate; uninterruptible OS work
is not made hard-real-time by cancellation. Secret clearing is not a host-memory erasure claim.

## Recovery outcomes and retained evidence

| Failure boundary | Permitted recovery |
|---|---|
| Before the preparation transaction commits | No helper may run; normal local preparation can be attempted later. |
| Commit succeeds but its acknowledgement is lost | Reload the journal and observe only, even when no dataset was ever created. |
| Upload or create response is lost | Preserve the intent; observe the original identity, never upload/create automatically again. |
| Final source Read/Close fails after the last payload byte | No completion trailer; helper cannot create a dataset. Observe the original intent without restarting upload. |
| Pending/ready observation commits but acknowledgement is lost | Reload the same journal/reference; do not replace the resource or duplicate compute. |
| Dataset becomes public, changes version/identity or has wrong bytes | Preserve recovery evidence and fail closed; do not update, adopt or submit it. |

Upload tickets issued before dataset creation may leave incomplete provider-side storage.
Their opaque ticket values are transient and are not persisted as independent deletable
resources by this task. The staging intent remains pinned/unresolved; no automatic cleanup,
absence-based reset or provider reclamation guarantee is claimed. Complete remote cleanup
and provider-specific orphan handling retain separate evidence/authorization gates.

## Verification and current limits

With the pinned repository environment:

```text
go test -race ./internal/provider/kaggle ./internal/store/sqlite
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Inside `tools/kaggle-client`:

```text
uv run --locked python -m unittest discover -s tests -v
```

The Python suite uses the existing locked Python 3.11.16/Kaggle 2.2.4/SDK 0.1.35 environment.
Tests cover private create versus delayed readiness, more than one page, exact marker/payload
bytes, ownership/version/privacy changes, lost creation/transfer responses, forbidden fallback,
framing, redaction and independent watchdog termination. The SDK is real; network transport
and account/provider responses are fixtures. Go tests cover service authority/order, strict
reports, Close/EOF faults, process isolation, real SQLite ledger/reopen and lost acknowledgements.
Earlier M3 kill/fencing/disk-full suites remain supporting evidence; this PR does not claim a
new real provider-process kill or live network test.

Completion regressions add real Go/Python pipe execution, final payload EOF/Close faults and
SDK fixtures proving that all uploaded payload bytes without a complete source trailer still
make zero dataset-create calls. The normal framed path creates once; observe remains read-only.

Final-head workflow outcomes, exact commits and any unresolved checks are recorded in PR #20.
Local Go 1.23.2 ran the exact protocol constructor and real-pipe regression with race detection
and three repetitions, plus vet, in an isolated standard-library harness. Six Python protocol/
watchdog tests passed locally; seven SDK tests were explicitly skipped because the SDK was not
installed. Those local results do not qualify full Go 1.27.1/modernc integration, native builds
or actual SDK behavior; the pinned CI tier supplies that evidence. Historical results from the
obsolete checkpoint are not relabeled as current staging results. No dependency, public contract,
migration, runner asset or workflow is changed.

M4-02 is an offline staging component, not a production Kaggle batch adapter or a shipped
live-mutation command. No M1 live gate is closed, no staging/live support is asserted from a
fake test, and M4-03 has not started. Stop at PR #20 for owner review/merge.

## Primary-source boundary

Version-pinned source references, reviewed for this implementation on 2026-09-16:

- [SDK v0.1.35 dataset service](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/datasets/services/dataset_api_service.py): create, metadata, status, paginated listing and raw-file download methods.
- [SDK request/response types](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/datasets/types/dataset_api_service.py): version, private visibility and file metadata.
- [CLI v2.2.4 dataset metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets_metadata.md): supported metadata/license labels; not upload-rights advice or live readiness evidence.

See [ADR-0016](../decisions/0016-private-staging-and-readiness.md),
[preflight](kaggle-preflight.md), [dispatch](../dispatch.md), [retention](../retention.md)
and the [implementation plan](../implementation-plan.md).
