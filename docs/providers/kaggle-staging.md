# Private staging and readiness

The Stager prepares one attempt's original bytes and separately verifies private readiness.
It is not a general Provider or a new journal. The fixed [acceptance utility](kaggle-acceptance.md)
composes it; the normal server does not activate it.

## Authority and frozen identity

`NewStager(config, policy, resolver, blobs, allowCreate)` requires explicit creation enablement,
but that flag is not a durable permit. Only the invocation receiving a successful **new** M3
BeginPreparation commit may call Prepare. Recovery uses ReconcilePreparation with the original
plan/operation even when lookup returns not found.

The `crs-` name derives from prewritten installation/workspace/instance/job/attempt/preparation
identity, not mutable content. Changed content under that intent must conflict, not get a new
resource name. The marker binds exact plan, nonce, logical file mapping, length/digest and license.
Payload names are fixed `relay-stage.bin`, `code.bin`, `input-NNN.bin`; no credential/source URL,
command/environment or private host path enters the marker.

Local bytes must match original size/hash/EOF and Close before credential use. Streaming checks
repeat on the actual upload. The source-completion trailer is emitted only after all Go readers
finish successfully. Python requires that trailer and final EOF before CreateDataset: pipe EOF
after a Read/Close error is not successful source acknowledgement.

## One creation, observed readiness

The helper checks local pins and server account, then reads the exact dataset. Only genuine
HTTP 404 on a newly authorized path permits creation. Existing resources are verified, never
updated/versioned/adopted by prefix. One upload ticket/PUT per file and one private creation are
allowed; ambiguous errors do not trigger hidden retries. License defaults to `copyright-authors`,
with `other`/`unknown` supported; metadata never grants upload rights or public/CC0 fallback.

Creation acknowledgement alone is not ready. Require exact positive dataset ID, owner/slug,
version 1, marker/license and explicit privacy; READY state; all bounded listing pages; marker
hash first, then every original payload's size/hash/EOF/Close. Recheck metadata/state afterward.
Incomplete, changed or oversized evidence fails closed. A discovered public resource stays
private-false/not-ready attention evidence, not something to repair by publishing.

M3 pins the first discovered reference and rejects later replacement at the same name. Read-only
reconciliation creates no upload, dataset version or deletion. Uncertainty keeps intent/resources
for recovery instead of resetting them.

## Limits and provider boundaries

Default staging budget is five minutes, up to 4 GiB inputs, 100 MiB bundle and 64 KiB marker;
at most 64 inputs plus code/marker. Smaller limits apply, with invocation policy bounded from
one second to one hour. These are ceilings, not upload throughput promises.

The version-bound SDK transport allows only required account/staging reads/mutations, one armed
send each, verified TLS, bounded responses, no retries/proxies/automatic redirects. Signed data
transfers allow only HTTPS `storage.googleapis.com` (optional 443), with no account Authorization
or Cookie header and no further redirects. The isolated fixed leaf receives stdin data/secrets
and uses parent/watchdog bounds. No local workload or archive extraction occurs.

Readiness is a point-in-time observation, not an immutable provider lock. Execution must recheck
the original staging reference and verify marker/input bytes remotely. Partial upload tickets
can leave storage without independent cleanup authority; no automatic reclamation is claimed.
Use [execution](kaggle-execution.md), [retention](../retention.md) and
[ADR-0016](../decisions/0016-private-staging-and-readiness.md). Record live findings in
[validation results](../development/validation-results.md), not as a new implementation diary here.
