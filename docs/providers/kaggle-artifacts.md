# Version-scoped Kaggle artifacts

ArtifactReader supplies ListArtifacts/FetchArtifact for one original Executor and frozen output
declarations. It performs read-only provider work; [collection](../collection.md) owns durable
pins and atomic publication, and [delivery](../artifact-delivery.md) serves committed local bytes.

## Identity and selection

Construction makes no credential/network calls. Validate workspace/job/attempt and original
numeric kernel ID/version/source before work. Each helper checks local pins, server account,
current/explicit-version metadata and established COMPLETE/ERROR state. Missing/new/undecodable
status is not terminal evidence. Recheck termination and current identity after reads/transfers;
final uncertainty invalidates even fully written temporary bytes.

Read all bounded ListKernelSessionOutput pages for version 1. Reject malformed/missing arrays,
empty continuing pages, duplicate/cyclic cursors and unsafe/case/prefix-conflicting names. Ignore
listing URLs: download with exact owner/kernel/file path and version using DownloadKernelOutput.
No bulk ZIP, local archive extraction, arbitrary URL or provider path as a host filename.

Under `relay-result/`, select only the mandatory `control/execution-result.json`, optional
stdout/stderr/environment controls and frozen declared `outputs/` files. Download manifest first
and match nonce, job/attempt and original input digests. Apply declaration/directory aggregate
bounds and required completed outputs. Failed terminal work can retain available manifest/logs
without fabricated payload success. Empty required directories retain manifest-v1 limitations.

A listing has no SHA-256 proof. Payload digests are manifest claims until bytes are verified;
control bytes are hashed during discovery. Full schema/phase/exit/GPU consistency is still M3's
responsibility before pinning. Candidate selection is not success publication.

## Pagination, transfer and recovery

One complete sorted in-memory catalog supports digest/reference/offset-bound port pages. Return
copies, not mutable cache slices. A changed catalog or reconstructed reader rejects old cursors.
This cache is not M3's durable pin. Once M3 pins a manifest/file set, recovery reuses that pin and
rehashed complete blobs, never substitutes a newer catalog. Each missing file still revalidates
original identity/list/manifest against its pinned path/size/hash.

Fetch requires an unpublished destination. Python, Go and M3 independently check length/digest;
successful response EOF/Close, final status/identity and successful helper exit are all required.
Writer short/invalid writes, errors or panic cancel the producer. A nonzero exit or late failure
after all bytes returns no successful transfer receipt. M3 withholds successful blob EOF until
this whole call succeeds, then independently reopens/hashes before atomic publication.

A committed collection failure needs explicit transfer-only retry, not compute retry. Lost
pin/publication acknowledgement is read from committed state. Repeated per-file list/manifest
checks favor safety over throughput; no range resume, persistent signed-URL cache or performance
guarantee is supplied.

## Transport and limits

The isolated fixed helper allowlists only account, metadata/status, output listing and exact-file
read RPCs, one armed send each. The private SDK session seam remains version-bound. Verified TLS,
identity encoding, strict bounded JSON and no ambient proxies/cookies/retries apply. One validated
signed download redirect may use HTTPS `storage.googleapis.com` (optional 443), with no account
Authorization/Cookie and no subsequent redirect. Raw signed URLs/diagnostics are not exposed.

Defaults: 10,004 selected files, 4 GiB selected bytes and ten-minute invocation, configurable down
and within one-second–thirty-minute bounds. Listing permits 20,000 names/256 pages; helper at most
300 read RPCs. Manifest/environment 1 MiB each, logs 20 MiB each, request 128 KiB, metadata 3 MiB,
catalog stdout 8 MiB, diagnostics 16 KiB and chunks 64 KiB. Connect/read budgets are 5/30 seconds
under parent/watchdog limits. Encoded/compressed or declared-length-mismatched payloads fail.

Only compressed fixed helper modules enter the command line under its 28,000-character budget;
token/request are stdin and payload binary stdout. No downloaded script executes locally.
Version/source/nonce/digests are not an atomic provider snapshot or hostile-workload attestation;
same-version rerun and original-binary recovery limits remain. See
[ADR-0019](../decisions/0019-version-scoped-kaggle-artifacts.md) and the
[operator checklist](../development/validation-checklist.md).
