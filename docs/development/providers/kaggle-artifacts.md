# Version-scoped Kaggle artifacts

ArtifactReader supplies ListArtifacts/FetchArtifact for one original Executor and frozen output
declarations. It performs read-only provider work; [collection](../collection.md) owns durable
pins and atomic publication, and [delivery](../../artifact-delivery.md) serves committed local bytes.

## Identity and selection

Construction makes no credential/network calls. Validate workspace/job/attempt and original
numeric kernel ID/version/source before work. Each helper checks local pins, server account,
current/explicit-version metadata and established COMPLETE/ERROR state. Missing/new/undecodable
status is not terminal evidence. Recheck termination and current identity after reads/transfers;
final uncertainty invalidates even fully written temporary bytes.

Download the result manifest directly with the exact owner/kernel/file path and version 1 using
DownloadKernelOutput. Do not call ListKernelSessionOutput: provider working directories can
contain large unrelated code, input and scratch trees. Reject unsafe/case/prefix-conflicting
selected paths. No bulk ZIP, local archive extraction, arbitrary URL or provider path as a host
filename.

Under `relay-result/`, select only the mandatory `control/execution-result.json`, optional
stdout/stderr/environment controls and frozen declared `outputs/` files. Download manifest first
and match nonce, job/attempt and original input digests. Apply declaration/directory aggregate
bounds and required completed outputs. Failed terminal work can retain available manifest/logs
without fabricated payload success. Empty required directories retain manifest-v1 limitations.

Read the fixed optional controls directly; omit one only when its exact provider download
returns 404. Authentication failures, 403, 429, storage errors and transport failures remain
failures. Payload existence, length and digests are manifest claims until exact-file transfer
verifies them; a missing selected file prevents publication. Control bytes are hashed during
discovery. Full schema/phase/exit/GPU consistency is still M3's
responsibility before pinning. Candidate selection is not success publication.

## Pagination, transfer and recovery

One complete sorted in-memory catalog supports digest/reference/offset-bound port pages. Return
copies, not mutable cache slices. A changed catalog or reconstructed reader rejects old cursors.
This cache is not M3's durable pin. Once M3 pins a manifest/file set, recovery reuses that pin and
rehashed complete blobs, never substitutes a newer catalog. Each missing file still revalidates
original identity/manifest against its pinned path/size/hash.

Fetch requires an unpublished destination. Python, Go and M3 independently check length/digest;
successful response EOF/Close, final status/identity and successful helper exit are all required.
Writer short/invalid writes, errors or panic cancel the producer. A nonzero exit or late failure
after all bytes returns no successful transfer receipt. M3 withholds successful blob EOF until
this whole call succeeds, then independently reopens/hashes before atomic publication.

The optional fetch-progress port reports absolute selected target bytes after writes of the
existing exact-file helper stream into Relay. Collection counts only output-role streams;
repeated manifest and control traffic is excluded. Progress callback failure stops transfer
without bypassing final verification. Cached local blobs are accounted separately by collection.
No additional Python progress channel, provider URL or application callback is exposed.

A committed collection failure needs explicit transfer-only retry, not compute retry. Lost
pin/publication acknowledgement is read from committed state. Repeated per-file identity/manifest
checks favor safety over throughput; no range resume, persistent signed-URL cache or performance
guarantee is supplied.

## Transport and limits

The isolated fixed helper allowlists only account, metadata/status and exact-file
read RPCs, one armed send each. The private SDK session seam remains version-bound. Verified TLS,
identity encoding, strict bounded JSON and no ambient proxies/cookies/retries apply. One validated
signed download redirect may use exact HTTPS `storage.googleapis.com` or
`www.kaggleusercontent.com` (optional 443), with no account Authorization/Cookie and no
subsequent redirect. Raw signed URLs/diagnostics are not exposed.

Defaults: 10,004 selected files, 4 GiB selected bytes and ten-minute invocation, configurable down
and within one-second–thirty-minute bounds. The helper permits at most 300 read RPCs and never
enumerates the provider working directory. Manifest/environment 1 MiB each, logs 20 MiB each, request 128 KiB, metadata 3 MiB,
catalog stdout 8 MiB, diagnostics 16 KiB and chunks 64 KiB. Connect/read budgets are 5/30 seconds
under parent/watchdog limits. Encoded/compressed or declared-length-mismatched payloads fail.

Only compressed fixed helper modules enter the command line under its 28,000-character budget;
token/request are stdin and payload binary stdout. No downloaded script executes locally.
Version/source/nonce/digests are not an atomic provider snapshot or hostile-workload attestation;
same-version rerun and original-binary recovery limits remain. See
[ADR-0019](../decisions/0019-version-scoped-kaggle-artifacts.md) and the
[operator checklist](../validation-checklist.md).
