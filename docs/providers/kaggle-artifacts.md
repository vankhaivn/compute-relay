# Version-scoped artifact retrieval and verified local publication

> **Task:** M4-05, implemented offline in PR #23; in review until owner merge.
> **Requirements:** JOB-05, PRV-02, VER-02.
> **Evidence:** pinned SDK with mocked HTTP; real collection/SQLite/blob integration with
> synthetic helper results; process and pure selection tests. No live provider credentials.

M4-04 operational mappings are merged in PR #22. This task adds a read-only artifact port for
one original attempt, not another execution path, public download endpoint or complete runtime.
M1 live acceptance and M4-06 integrated acceptance retain their separate authorization gates.

## Composition and three different evidence stages

Compose `NewArtifactReader(executor, policy)` with the original per-attempt M4-03 Executor.
The constructor parses the frozen job's output declarations and copies its original identity.
It does not resolve credentials or call a provider. The reader implements `ListArtifacts` and
`FetchArtifact`; it has no preparation, submission, cancellation or cleanup operation.

The existing M3-06 collector still owns durable collection leases, immutable result pins,
independent blob verification and atomic publication. A test-only provider wrapper connects
these components in integration tests; no production Provider stub or registry is added.

1. **Candidate catalog:** a complete bounded listing, matching result manifest and observed
   control-file digests identify which bytes should be collected. Payload hashes in the manifest
   are claims until checked, not proof that the payload was downloaded.
2. **Verified local bytes:** the Python transfer guard, Go reader and M3 collector check actual
   stream length/digest and successful completion. M3 reopens each local blob and hashes it again.
3. **Published results:** M3 commits the whole artifact set, result/state/events and collection
   operation outcome atomically. A provider terminal status, catalog or completed temporary file
   alone never makes results application-visible.

No SQL transaction spans a provider request or byte transfer. Current workspace authorization
is enforced by the existing internal `collection.Reader`, not replaced by a provider URL.

## Identity before and after reads

The reader validates the original workspace/job/attempt remote reference, numeric kernel ID,
version 1 and expected source digest before local/provider work. Each helper invocation checks
local pinned tools before reading its stdin token, then verifies the server-returned account.
It validates current and explicit-version kernel metadata using M4-03's exact source/private/
resource checks and requires established terminal `COMPLETE` or `ERROR` status.

Missing, nonterminal, new or undecodable SDK statuses do not qualify for collection. SDK enum
errors are normalized to unestablished termination; no default `QUEUED` or success is inferred.
After all selected reads or a file transfer, terminal status and current kernel identity are
checked again. New source, privacy, ID/version or status uncertainty invalidates the result,
including when the destination already received every byte.

These are bounded observations, not an atomic provider lock. M4-03's same-version rerun and
source-reconstruction limitations remain. Preserve the original compatible binary/configuration
for in-flight recovery. The manifest, nonce and hashes protect against accidental result mixups,
not malicious workloads or cryptographic remote attestation.

## Complete listing and selected namespaces

Use the public pinned SDK's `ListKernelSessionOutput` with explicit `version_label="1"`, page
size 100 and bounded continuation tokens. Read every page before returning a candidate catalog.
Reject malformed/missing file arrays, empty continuing pages, duplicate/cyclic cursors, too many
pages/files and invalid or case/prefix-conflicting names. Missing required evidence fails closed
rather than truncating the catalog. All listed names, including unselected files, are checked.

The listing's file records do not supply SHA-256 checksums. **Do not treat listing URLs as
artifact authority.** The implementation ignores those URLs and calls `DownloadKernelOutput`
with exact owner, kernel, file path and `version_number=1`. The ZIP API is never used and no
provider archive is extracted on the control host.

The provider prefix is `relay-result/`. Applications and M3 see only these logical namespaces:

- `control/execution-result.json`, always required;
- optional bounded `control/stdout.log`, `control/stderr.log`, `control/environment.json`; and
- `outputs/` files declared by the frozen job and present in its result manifest.

Code, input, scratch and arbitrary undeclared files are not downloaded. Remote paths never
become host filesystem paths. Paths must satisfy the portable ASCII rules, with no traversal,
absolute paths, encoded separators, Windows reserved names or case/prefix collisions.
The SDK file endpoint is treated as a logical regular-file byte stream, not filesystem metadata
or symlink attestation. No local link/archive interpretation is performed.

Download and verify the bounded result manifest first. Compare job/attempt/nonce and original
bundle/input digests, then match its files against frozen declarations and the complete listing.
Directory declarations apply aggregate byte bounds. Required completed outputs cannot be absent;
empty required directories retain the M3 manifest-v1 limitation. A terminal failed payload can
still expose its available manifest/logs without inventing successful payload output.

Catalog discovery hashes available control bytes but does not fetch payload files. Full manifest
schema, phase, exit-code and GPU requirement consistency are checked by M3 before pinning. A
successful candidate-selection call is not full manifest validation or a success declaration.

## Pagination and immutable pins

The Go reader caches one complete sorted candidate catalog in memory. Public port pages are
copies; their cursors bind the exact remote-reference digest, catalog digest and offset. A new
empty cursor explicitly rediscovers a catalog and invalidates old cursors if contents differ.
A reconstructed reader rejects old in-memory cursors rather than silently issuing a new listing.

This cache is **not** the durable result pin. M3 pins a single manifest and complete artifact
identity set before output transfer. After restart or an explicit collection retry, M3 uses
that pin and already verified blobs without rerunning candidate discovery. Each missing file
still revalidates the original remote identity, full listing and manifest through the helper,
then checks the requested pinned path/length/digest. A newer result cannot replace the pin.

Discovery and per-file verification deliberately repeat metadata/manifest reads. The current
implementation favors bounded correctness over throughput for large file counts. It has no
long-lived signed-URL cache, partial-byte range resume or performance guarantee.

## Streaming and acknowledgement boundaries

`FetchArtifact` requires a temporary/unpublished destination. Token and request metadata are
framed on stdin; binary payload is streamed on stdout. The destination may contain partial or
complete bytes when the method returns an error. Callers must not publish it on pipe EOF alone.

A transfer succeeds only after bounded response EOF, stream closure, digest/length agreement,
final terminal/identity checks, successful helper exit and the independent Go writer checksum.
An error after the last byte, a failing Close, wrong digest, extra byte, short write, destination
panic, unknown final status or nonzero process exit returns no successful transfer receipt.
The writer cancels the producer on a destination failure even if an injected helper ignores it.

M3's existing transfer pipe withholds successful EOF until the entire provider method succeeds.
Thus a fully written payload followed by failed acknowledgement cannot become a complete blob
or published artifact. Once all files are independently verified, M3 commits publication. A
lost publication acknowledgement is recovered by reading committed state, not duplicate writes.

A committed transfer failure requires a new explicit collect operation/key, never compute retry.
An interrupted accepted collection can be reclaimed after its lease expires. Original operation
receipts remain replayable; current operation/result reads show subsequent progress or expiry.
Neither path refreshes original inputs or submits another execution.

## Transport and limits

The fixed helper uses only introspection, kernel metadata/status, output listing and explicit
file download RPCs on the exact Kaggle production host. The version-bound private SDK session
hook arms one wire call per method, preventing hidden retries. Mutation and ZIP operations are
absent from the allowlist. TLS verification remains enabled; ambient proxies/cookies/credential
fallback and automatic redirects are disabled.

A reviewed download redirect can supply one signed HTTPS URL on `storage.googleapis.com`
(optional port 443). Its original redirect body is not consumed. The separate storage GET
carries no Kaggle Authorization or Cookie header, accepts no further redirects and performs
no automatic retry. Other hosts, schemes, userinfo, fragments or malformed URLs fail closed.
Signed URLs and raw provider diagnostics are not returned in the artifact catalog.

Default policy permits at most 10,004 selected files, 4 GiB total selected bytes and a ten-minute
invocation; smaller byte/file bounds and one-second to thirty-minute contexts are configurable.
Manifest/environment files are bounded to 1 MiB each and logs to 20 MiB each. Listing bounds
are 20,000 names and 256 pages; one helper permits at most 300 read RPCs. A request is at most
128 KiB, metadata response 3 MiB, catalog stdout 8 MiB and discarded diagnostics 16 KiB.
Data chunks are at most 64 KiB; declared lengths must agree with actual EOF and content encoding
must be identity. Connect/read timeouts are 5/30 seconds, bounded by the parent invocation and
independent helper watchdog. None is a provider throughput or hard-real-time OS guarantee.

The fixed helper and its non-main execution/selection modules are compressed as inert source
for Python isolated mode, keeping the command below the 28,000-character internal budget.
Only repository code is packaged there, not job source, token or user payload. No downloaded
script executes locally. The interpreter/packages/operator and cooperative callbacks remain
trusted; this is a fixed leaf process, not a general process-tree or hostile-host sandbox.

## Verification and remaining gates

```text
go test -race ./internal/provider/kaggle ./internal/collection ./internal/store/sqlite
go test -count=3 -run='TestArtifact' ./internal/provider/kaggle
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Inside `tools/kaggle-client`, run `uv run --locked python -m unittest discover -s tests -v`.
The existing CI workflows use the unchanged pinned Go/modernc and Python 3.11.16/Kaggle 2.2.4/
SDK 0.1.35 stack. PR #23 records exact-head results separately from earlier checkpoints.

Pure tests cover selection, required/declaration bounds, path/cursor collisions and closed JSON.
Real SDK fixtures cover complete pagination, manifest-first selection, credential-free signed
storage, 16 MiB streaming, wrong identity/pins, terminal failure, final-status loss, late read/
Close errors and no fallback. Go process tests cover packed helper modules, binary framing,
isolation, output limits, nonzero exit after bytes and deadlines.

Real SQLite/admission/dispatch/collection/blob tests independently observe committed collection
leases and pins before helper entry. They verify complete publication and current-authority reads,
original receipts, profile remapping, 16 MiB halfway/wrong/late transfers, restart with the same
pin, lost pin/publication acknowledgements and false nonce/required-output/GPU/success claims.
The helper outcomes are synthetic in this tier; real-SDK HTTP fixtures are a separate tier.
Tests assert one original attempt/staging creation/submission and no fabricated release evidence.

Local continuation evidence is five pure selection/path/identity test roots on Python 3.13.5,
using source and test files verified byte-for-byte against Git blob hashes, plus gofmt of the new
journal tests on Go 1.23.2. No local full modernc, pinned SDK or end-to-end provider execution is
claimed. The earlier three checkpoints were resumed, not rewritten. Code and documentation
checkpoints are separate commits; no dependency, workflow, migration or public contract changes.

This task does not ship a full production Provider/registry, artifact HTTP/CLI, live account test,
remote cleanup or M4-06 acceptance. Result expiry and coordinated database/blob backup keep the
existing M3-07 semantics; a metadata-only backup cannot restore deleted bytes.

## Reviewed primary sources

Reviewed 2026-09-16 at the existing pin, without live account calls:

- [Official SDK output operations](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/services/kernels_api_service.py).
- [Explicit-file/version download and versioned paginated listing types](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kernels/types/kernels_api_service.py).
- [SDK transport/response handling](https://github.com/Kaggle/kaggle-sdk-python/blob/v0.1.35/kagglesdk/kaggle_http_client.py).

See [ADR-0019](../decisions/0019-version-scoped-kaggle-artifacts.md),
[collection](../collection.md), [execution](kaggle-execution.md),
[operations](kaggle-operations.md), [retention](../retention.md) and the
[implementation plan](../implementation-plan.md). Stop after PR #23 for owner review/merge;
M4-06 and live-provider acceptance have not started.
