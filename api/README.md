# API contracts

> **Tasks:** M2-03 contracts; M2-05 through M2-08 auth/object input handlers;
> M3-02 admission/status, M3-05 controls and M3-07 expiry; M5-01a/b local serving/client;
> M5-01c published artifact delivery in PR #27.
>
> **Status:** contracts and eighteen composable handler operations are implemented offline.
> Local `serve` composes auth/object/admission/control and published-result reads, without
> provider workers. Complete production composition, log/cleanup HTTP and live integration
> remain gates. M5-01c is in review until owner merge.

## Contract versions

- HTTP API prefix: `/v1`
- OpenAPI: `3.1.1`
- JSON Schema dialect: Draft 2020-12
- Job/status API version: `compute-connector/v1alpha1`
- Runner result-manifest version: `1`
- Normalized runtime-config schema version: `1`
- Bundle manifest version: `compute-relay/bundle/v1`

The working job version intentionally keeps the proposal's `compute-connector/v1alpha1`
identifier while repository branding remains Compute Relay. Changing a published identifier
later requires an explicit compatibility decision rather than a silent rename.

## Files

| Path | Purpose |
|---|---|
| [`openapi.json`](openapi.json) | Provider-neutral `/v1` HTTP contract with per-operation implementation status. |
| [`schemas/common.v1alpha1.schema.json`](schemas/common.v1alpha1.schema.json) | Shared typed IDs, digests, state/error/capability/event enums, and safe relative paths. |
| [`schemas/job-spec.v1alpha1.schema.json`](schemas/job-spec.v1alpha1.schema.json) | Immutable Python/shell batch job request. |
| [`schemas/result-manifest.v1alpha1.schema.json`](schemas/result-manifest.v1alpha1.schema.json) | Generic remote runner result manifest. |
| [`schemas/runtime-config.v1alpha1.schema.json`](schemas/runtime-config.v1alpha1.schema.json) | Normalized non-secret runtime configuration. TOML decoding is a later task. |
| [`schemas/job-status.v1alpha1.schema.json`](schemas/job-status.v1alpha1.schema.json) | Truthful independent attempt-state dimensions. |
| [`schemas/operation.v1alpha1.schema.json`](schemas/operation.v1alpha1.schema.json) | Generic domain operation, including the reserved cleanup kind; not the HTTP control receipt. |
| [`schemas/control-request.v1alpha1.schema.json`](schemas/control-request.v1alpha1.schema.json) | Explicit attempt target and optional non-secret reason; retry requires a nonblank reason. |
| [`schemas/control-operation.v1alpha1.schema.json`](schemas/control-operation.v1alpha1.schema.json) | Actual HTTP control receipt/current view: effect, replay, termination evidence, optional new attempt, safe problem and links. |
| [`schemas/error.v1alpha1.schema.json`](schemas/error.v1alpha1.schema.json) | Stable error envelope without a dangerous generic `retryable` flag. |
| [`schemas/job-admission.v1alpha1.schema.json`](schemas/job-admission.v1alpha1.schema.json) | Durable asynchronous admission response. |
| [`schemas/job-validation.v1alpha1.schema.json`](schemas/job-validation.v1alpha1.schema.json) | No-compute validation response and verification requirements. |
| [`schemas/object.v1alpha1.schema.json`](schemas/object.v1alpha1.schema.json) | Committed workspace object ID, byte count and digest; never physical paths. |
| [`schemas/object-import.v1alpha1.schema.json`](schemas/object-import.v1alpha1.schema.json) | Named-root raw-file or explicitly selected bundle import request. |
| [`schemas/object-ingest.v1alpha1.schema.json`](schemas/object-ingest.v1alpha1.schema.json) | Public HTTPS input request with an optional expected SHA-256; destination policy remains a runtime check. |
| [`schemas/bundle-manifest.v1.schema.json`](schemas/bundle-manifest.v1.schema.json) | Regular-file bundle manifest with portable paths, sizes, hashes and executable flags. |
| [`schemas/artifact.v1alpha1.schema.json`](schemas/artifact.v1alpha1.schema.json) | Published artifact metadata/pages with explicit workspace/job/attempt, size/digest, historical phase/time and pagination. |
| [`examples/`](examples/) | Valid examples and deliberately invalid negative fixtures. |
| [`contract-manifest.json`](contract-manifest.json) | Explicit schema-to-fixture inventory. No untracked root schema is allowed. |
| [`contract.lock.json`](contract.lock.json) | SHA-256/byte-size identity of every committed JSON contract and fixture. |

## Strictness and limits

Object schemas reject unknown fields unless explicitly open, such as bounded labels or
diagnostic details. Public control problems exclude arbitrary details. Schemas validate
opaque IDs, lowercase digests, command/count/byte/time bounds, safe relative paths, HTTPS input
syntax, explicit enums and structural state/phase combinations. Normalized configuration
forbids automatic compute retries and provider fallback.

Runtime context is still necessary. Admission checks name/path collisions, setup/finalization
budgets, reserved environment variables, workspace ownership and current profile bounds.
Byte integrity, bundle layout and actual provider eligibility must pass later before dispatch.
The embedded admission parser rejects duplicate decoded keys, invalid Unicode, trailing values,
excessive nesting and oversized canonical expansion. Request identity uses a named integer-only
format, not raw JSON or an RFC 8785 claim. See [admission](../docs/admission.md).

Bundle/import runtime checks additionally cover case/prefix collisions, ordering, root authority,
exclusions, USTAR representation and actual bytes. HTTPS ingestion additionally enforces
DNS/connected-peer/TLS/redirect policy; schema validity is not SSRF authorization. Unknown,
duplicate/case-alias fields and nulls cannot be repaired by ordinary schema decoding after
ambiguity has already been discarded. Lower configured limits still apply under schema ceilings.

Control requests require `attempt_id`; no implicit active attempt is resolved. The actual
parser rejects duplicate keys, invalid UTF-8, nulls, unknown fields, bodies over 4,096 bytes
and reasons over 512 UTF-8 bytes. Retry requires a nonblank reason. Record validation checks
identity/time ordering and source/new-attempt separation in durable context. JSON Schema's
character count does not replace the byte limit.

M3 collection reuses the result-manifest schema and checks attempt/nonce, frozen input digests,
GPU requirements and output declarations. It rejects path collisions, wrong/missing bytes and
false success. A schema-valid manifest is not a verified artifact. See [collection](../docs/collection.md)
for the empty-required-directory limitation and atomic publication boundary.

M3-07's `result.expired` event, result state and tombstones commit atomically. New input references
and explicit retry reject expired inventory even while bytes await sweep; original receipts
remain replayable under current authority. The event enum does not create an event HTTP route.
M5-01c adds HTTP expiry reporting without changing that history. See [retention](../docs/retention.md).

Public artifact metadata/pages describe a historical publication. Their schema does not prove
current disk bytes, valid HTTP trailers or workload success. The runtime validates complete
publication identities, total bytes, uniqueness, ordering and snapshot offsets; content is
independently rehashed. Clients validate exact known lowercase keys and required values while
ignoring compatible future fields, rather than reflecting them into trusted output.

## OpenAPI operation inventory

The ten original operations retain their established offline component/HTTP evidence:

```text
GET  /healthz
GET  /readyz
GET  /v1/info
POST /v1/workspaces/{workspace_id}/objects
GET  /v1/workspaces/{workspace_id}/objects/{object_id}
POST /v1/workspaces/{workspace_id}/objects/import
POST /v1/workspaces/{workspace_id}/objects/ingest
POST /v1/workspaces/{workspace_id}/jobs/validate
POST /v1/workspaces/{workspace_id}/jobs
GET  /v1/workspaces/{workspace_id}/jobs/{job_id}
```

Only `/healthz` is public. Readiness checks local dependencies, not GPU availability. Upload,
import and ingestion require write scope and return 201 only after byte/ownership commit.
Named-root import and guarded HTTPS ingestion remain optional composition services, not enabled
by the current local server. See [auth/objects](../docs/auth-and-objects.md),
[packaging/import](../docs/packaging-and-import.md) and [HTTPS ingestion](../docs/https-ingestion.md).

Job creation requires write scope and exactly one printable-ASCII `Idempotency-Key`, 8–256 bytes
without whitespace. Job/attempt/profile/object/event/receipt state commits before 202. Equivalent
requests replay original IDs and `idempotency_replay=true`; changed requests conflict. Current
authority is always checked. Validation/status require read scope; validation reports remaining
checks, not provider eligibility. No job handler executes commands, refreshes inputs or calls a
provider. Nil admission composition has no nondurable fallback.

M3-05 supplies five control operations:

```text
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/cancel
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/retry
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/reconcile
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/collect
GET  /v1/workspaces/{workspace_id}/operations/{operation_id}
```

Control POSTs require operate scope and the same explicit key bounds; GET requires read scope.
Successful POSTs return 202 and Location. `replay=true` preserves the original control receipt;
GET returns current durable status with `replay=false`. This field differs from job admission's
`idempotency_replay`. The `/retry` route has operation kind `retry_compute`, source `attempt_id`
and distinct `new_attempt_id`. No control handler calls a provider. Explicit compute retry
verifies original local bytes, while reconcile never repeats submission and collect creates a
transfer-only ticket. Neither acceptance nor cancellation acknowledgement proves execution
termination, verified results or hardware release. See [operations](../docs/operations.md).

M5-01c adds **three published artifact operations**, bringing the inventory to **eighteen**:

```text
GET /v1/workspaces/{workspace_id}/jobs/{job_id}/artifacts
GET /v1/workspaces/{workspace_id}/jobs/{job_id}/artifacts/{artifact_id}
GET /v1/workspaces/{workspace_id}/jobs/{job_id}/artifacts/{artifact_id}/content
```

All require one explicit `attempt_id` query parameter and current read authority. List additionally
accepts canonical `limit=1..100` and a snapshot-bound cursor. Unknown/duplicate query parameters,
request bodies, encoded paths, Range/If-Range and non-GET methods are rejected. A page has explicit
`next_cursor`, empty only at the end. Its complete-publication digest survives server reopen;
wrong/stale cursor returns 409. Cursors are not permission tokens.

A nil result reader, missing/unpublished result or invisible artifact returns 404. Expired
publications return 410 with `ARTIFACT_MISSING`, after current authorization. Revoked credentials
return 401 even when the requested publication expired. Metadata contains no host paths or
provider URLs. Historical `result_phase` need not mean payload success.

Content starts an octet-stream attachment with identity/size/digest headers and HTTP/1.1 chunked
framing, **not Content-Length**. The server declares `Trailer: X-Compute-Relay-Verified` before
the body and sets `X-Compute-Relay-Verified: true` only after actual byte verification, clean
source EOF/Close and final authorization/expiry checks. A late failure aborts the response
without JSON or the success trailer. Already delivered bytes cannot be recalled.

**A 200 response or complete body is not sufficient.** Require exact metadata/header identity,
independent byte count/SHA-256, clean body EOF/Close and exactly one declared final true trailer.
An initial header is not final acknowledgement. Use an unpublished temporary sink until all
checks pass. Missing/stripped/duplicate trailers, wrong bytes, compression or a fixed-length
replacement cannot qualify. The OpenAPI `x-completion-trailers` extension documents this
additional semantic requirement; JSON Schema alone cannot enforce it.

See [artifact delivery](../docs/artifact-delivery.md) and
[ADR-0023](../docs/decisions/0023-verified-artifact-delivery.md) for commands, private create-only
publication, limits and evidence. These GETs read local publications only, never auto-collect,
submit or change durable receipts/events. Retained stdout/stderr are historical artifacts, not
live provider logs.

The root OpenAPI status remains `planned` for the complete product. Logs, events, attempts,
profile/quota reads and cleanup retain separate endpoint gates. Operation status is granular;
a link in a receipt does not implement its target. M3 collection/retention semantics and all
existing mutation contracts remain unchanged by the additive artifact routes.

## Local serving and application commands

`compute-relay serve` composes auth/object/admission/control services and the published result
reader with actual SQLite/input/result stores on literal loopback. It starts no scheduler,
provider, collector or retention workers and does not enable local import or HTTPS ingestion.
`X-Compute-Relay-Mode: local-admission-only` and local readiness do not promise execution.
Signal shutdown joins active handlers before closing stores and releasing ownership.

M5-01b's local profile apply/show and workspace grant/revoke commands configure admission using
the existing immutable profile revisions. They require a stopped server and do not create
provider bindings or activate workers. Application commands use a separately selected private
token file and HTTP while the server retains its installation lock. Local `validate --file`
reports `admitted=false`/`provider_checked=false`; `job validate` uses contextual server checks.
No SQL seed or direct database modification is an operator procedure.

M5-01c application commands list/show published artifacts and download verified bytes to a
user-chosen new file in a private directory. They perform no automatic retry or overwrite.
A late output-report failure can follow successful file publication and is reported explicitly.
See [local runtime](../docs/local-runtime.md), [application CLI](../docs/application-cli.md)
and [artifact delivery](../docs/artifact-delivery.md). Parent M5-01 and live M1/M4-06 stay separate.

## Validation commands

```bash
./scripts/dev.sh contract-check
./scripts/dev.sh contract-lock
./scripts/dev.sh check
```

PowerShell and CMD wrappers accept the same task names. Contract checking is offline: validate
the manifest/schema inventory, reject remote/escaping refs, compile Draft 2020-12 with formats,
require positive/negative fixture results, compare domain enums, validate OpenAPI and its exact
operation inventory, then verify the content lock. `contract-lock` is an explicit reviewed
update, not an automatic CI repair.

Tests validate actual object/admission/validation/status/control and artifact HTTP responses.
Control serializer tests retain the 880-combination truth table. Artifact fixtures reject
missing attempt/cursor, null bytes and traversal; stream tests separately verify trailers and
failure acknowledgement. No fixture URL is fetched, provider credential used or compute allocated.

## Change policy

A contract change updates schemas, positive/negative examples, lock and documentation together.
Breaking changes must be visible in commit/PR history under the repository convention. M5-01c
adds artifact schemas/fixtures, three operations, advertised features and their locks; it does
not rewrite existing request/response semantics. Do not claim endpoint support from schemas alone.
