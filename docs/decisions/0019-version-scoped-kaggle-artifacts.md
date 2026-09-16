# ADR-0019: Version-scoped artifact reads and immutable local publication

- Status: proposed; implemented offline in PR #23, pending owner review/merge
- Date: 2026-09-16
- Task: M4-05
- Requirements: JOB-05, PRV-02, VER-02
- Extends: ADR-0013, ADR-0015, ADR-0017 and ADR-0018

## Context

M4-03 supplies an exact per-attempt kernel identity and M4-04 supplies operational evidence.
M3-06 already owns collection leases, immutable result snapshots, verified blobs and atomic
publication. The pinned SDK exposes paginated output names/URLs and explicit-file/version
reads, but listing metadata is not proof of payload integrity. A successful download or pipe
EOF is also insufficient if identity checks or process acknowledgement fail afterward.

The owner requested offline M4-05 after merging PR #22. This does not authorize real account
reads, GPU use or remote mutations and does not waive the separate M1/live acceptance gate.

## Decision

Implement an explicitly composed ArtifactReader bound to the original per-attempt Executor
and frozen output declarations. It supplies the existing ListArtifacts/FetchArtifact port
methods, not another queue, database schema or complete production Provider. Reuse M3's
collection transaction and byte-publication boundaries without changing them.

Validate original numeric kernel ID/version/source/account/private metadata before and after
reads and require established terminal status. Missing/new status or SDK enum-decoding failure
is unestablished termination, not permission to fetch. Final status/identity loss invalidates
even a transfer whose destination already contains all expected bytes.

Read all bounded output-list pages for explicit version 1. Reject malformed/unsafe/colliding
names and incomplete/cyclic pagination. Ignore listing URLs; call the public SDK's explicit
DownloadKernelOutput with original owner/slug, version and file path. Do not use the ZIP API,
extract provider archives or map remote names into host paths.

Read the result manifest first and compare attempt nonce and original input digests. Select
only frozen declared outputs and a fixed control-file allowlist. Hash observed control bytes;
treat payload metadata as unverified manifest claims until actual bytes are checked. Keep
candidate discovery distinct from M3's full schema/phase/resource validation and publication.
Terminal failure can retain available manifest/log evidence without inventing payload success.

Use an in-memory, digest-bound complete catalog for stable port pagination. A cache miss or
changed snapshot invalidates an old cursor rather than mixing pages. The cache is not a durable
pin: M3 persists one original manifest/file set before payload transfer and reuses it after
restart or explicit collection retry. Missing files are fetched against original pinned hashes;
no path refreshes the durable snapshot or reruns compute.

Stream bytes into a temporary/unpublished destination with independent helper, Go and M3
size/digest checks. Require successful EOF/Close, final identity/status checks and helper exit.
Destination errors/panics/short writes cancel the producer. A nonzero exit after complete
stdout remains failure. M3 withholds successful blob EOF until the whole provider call succeeds
and reopens/hashes complete blobs before atomic artifact/state/event/operation publication.
Lost pin/publication acknowledgement is recovered through the existing journal, not replayed
SQL or a new execution.

Keep the helper read-only through exact RPC allowlisting and one armed send per SDK call.
Use the existing version-bound private SDK session seam, verified TLS, no retries/proxies or
ambient credential fallback, bounded strict metadata and one approved signed-storage redirect.
The separate storage request has no account token/cookie and cannot redirect further. Tokens,
metadata and payload never enter command arguments; only compressed fixed helper code is passed
there to respect the Windows command-line budget. Parent deadline and watchdog constrain a
cooperative leaf helper, not arbitrary process trees or uninterruptible host operations.

## Alternatives rejected

- Download directly from arbitrary listing URLs: weakens version/credential/destination control.
- Fetch/extract the complete ZIP: pulls unselected code/scratch and expands filesystem risk.
- Trust the provider listing or manifest digest without hashing bytes: permits false availability.
- Publish after the last byte before final checks/exit: loses the acknowledgement boundary.
- Relist and replace the M3 pin after failure: can silently associate newer results with history.
- Treat absent/new status as an SDK default: manufactures termination or execution evidence.
- Add another durable collection subsystem or optimistic full Provider: duplicates existing
  authority or conceals remaining composition work.

## Consequences and limits

Repeated per-file identity/list/manifest reads are conservative and can be expensive. Limits
bound work but do not promise large-job throughput. Partial-byte range resume and signed-URL
caching are not implemented. A reconstructed catalog cursor expires; an existing M3 pin does not.

Original version/source/nonce and digests reduce accidental confusion, not malicious workload
attestation or an atomic provider snapshot. Same-version rerun and cross-binary source recovery
limits from ADR-0017 remain. Preserve the original compatible binary/configuration for recovery.
The logical file API does not establish provider-side symlink metadata; no local link handling
or extraction is attempted. Only the reviewed storage hostname is accepted; other CDNs require
separate review, not automatic fallback.

Private payload/log bytes can contain sensitive user content. Temporary sinks must not be served
directly; only current-authority reads of committed results are application data. Metadata-only
backups do not restore result bytes, and byte expiry keeps M3-07's original history semantics.

This component adds no artifact HTTP/CLI, full production Provider/registry, remote cleanup or
live capability declaration. M4-06/M1 acceptance and installed runtime composition remain
separate. No dependency, public schema, migration, workflow or runner-source change is required.

## Verification

Pure selection tests check paths, declarations, identity and bounds. Go tests cover closed
catalogs, snapshot cursors, pinned transfer hashes, destination failures and real isolated
process framing/late exits/deadlines. The actual locked SDK is tested with mocked HTTP, including
all pages, manifest-first selection, signed storage without credentials, large bounded streams,
final-status loss and post-byte identity/digest/EOF/Close errors.

Real collection/SQLite/blob integration tests independently observe committed leases/pins
before synthetic helper entry, then verify authorized publication, original receipt replay,
profile remapping, 16 MiB partial/late/wrong transfers, same-pin restart, lost acknowledgements
and rejection of false manifest claims. These are separate tiers, not one live end-to-end test.
Exact-head CI and narrow local evidence are recorded in PR #23 and the
[artifact guide](../providers/kaggle-artifacts.md). Stop for owner review/merge after this PR.
