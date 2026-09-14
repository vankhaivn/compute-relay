# API contracts

> **Tasks:** M2-03 contract baseline; M2-05/M2-06 authentication and object handlers;
> M2-07 bundle manifests/local import; M2-08 bounded public HTTPS ingestion.
>
> **Status:** contracts and seven handler operations are implemented offline. The production
> runtime composition and all job/operation routes remain planned; no live provider claim.

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
| [`schemas/common.v1alpha1.schema.json`](schemas/common.v1alpha1.schema.json) | Shared typed IDs, digests, state/error/capability enums, and safe relative paths. |
| [`schemas/job-spec.v1alpha1.schema.json`](schemas/job-spec.v1alpha1.schema.json) | Immutable Python/shell batch job request. |
| [`schemas/result-manifest.v1alpha1.schema.json`](schemas/result-manifest.v1alpha1.schema.json) | Generic remote runner result manifest. |
| [`schemas/runtime-config.v1alpha1.schema.json`](schemas/runtime-config.v1alpha1.schema.json) | Normalized non-secret runtime configuration. TOML decoding is a later task. |
| [`schemas/job-status.v1alpha1.schema.json`](schemas/job-status.v1alpha1.schema.json) | Truthful independent attempt-state dimensions. |
| [`schemas/operation.v1alpha1.schema.json`](schemas/operation.v1alpha1.schema.json) | Durable cancel/retry/reconcile/collect/cleanup operation representation. |
| [`schemas/error.v1alpha1.schema.json`](schemas/error.v1alpha1.schema.json) | Stable error envelope without a dangerous generic `retryable` flag. |
| [`schemas/job-admission.v1alpha1.schema.json`](schemas/job-admission.v1alpha1.schema.json) | Durable asynchronous admission response. |
| [`schemas/job-validation.v1alpha1.schema.json`](schemas/job-validation.v1alpha1.schema.json) | No-compute validation response and verification requirements. |
| [`schemas/object.v1alpha1.schema.json`](schemas/object.v1alpha1.schema.json) | Committed workspace object ID, byte count and digest; never physical paths. |
| [`schemas/object-import.v1alpha1.schema.json`](schemas/object-import.v1alpha1.schema.json) | Named-root raw-file or explicitly selected bundle import request. |
| [`schemas/object-ingest.v1alpha1.schema.json`](schemas/object-ingest.v1alpha1.schema.json) | Public HTTPS input request with an optional expected SHA-256; destination policy remains a runtime check. |
| [`schemas/bundle-manifest.v1.schema.json`](schemas/bundle-manifest.v1.schema.json) | Regular-file bundle manifest with portable paths, sizes, hashes and executable flags. |
| [`examples/`](examples/) | Valid examples and deliberately invalid negative fixtures. |
| [`contract-manifest.json`](contract-manifest.json) | Explicit schema-to-fixture inventory. No untracked root schema is allowed. |
| [`contract.lock.json`](contract.lock.json) | SHA-256/byte-size identity of every committed JSON contract and fixture. |

## Strictness and limits

Object schemas reject unknown fields unless a field is explicitly an open map, such as
bounded labels or diagnostic details. Schemas validate:

- provider-neutral opaque IDs and lowercase SHA-256 values;
- command vector, item count, string length, byte, duration, and retention bounds;
- relative paths with no absolute path, backslash, control byte, duplicate separator, or
  parent traversal;
- HTTPS-only URL syntax for public ingestion requests;
- explicit Python/shell, GPU/network, operation, state, capability, evidence, and error
  enums;
- no automatic compute retry or provider fallback in normalized configuration;
- success/result/cancellation combinations that can be established structurally; and
- result-manifest requirements such as zero exit code, no error, and verified GPU when a
  completed run says GPU was required.

Some invariants are intentionally not forced into JSON Schema because they require runtime
context or arithmetic. Application/config validation must additionally check unique
normalized input/output targets, setup/finalization budgets within the remote wall budget,
workspace ownership, profile/provider references, policy upper bounds, immutable object
existence, and actual provider capability.

Bundle/import schemas impose a stricter portable ASCII path subset. Runtime validation
additionally checks case/prefix collisions, manifest ordering, allowed roots, exclusions,
USTAR encoding, total sizes and actual digests. Schema ceilings are hard representational
bounds; lower configured/default policy limits still apply. Strict import decoding also
rejects duplicate JSON keys and nulls, which ordinary schema validation cannot disambiguate
after a parser has already discarded duplicate keys.

HTTPS ingestion likewise rejects duplicate/case-alias fields and nulls in the actual
decoder. Its structural schema does not replace runtime port/query/host policy, DNS answer
validation, connected-peer verification or TLS/redirect checks. An input that passes a
schema can still be rejected as an unsafe destination before a network connection.

## OpenAPI status

The following seven operations have offline handler and loopback integration evidence:

```text
GET  /healthz
GET  /readyz
GET  /v1/info
POST /v1/workspaces/{workspace_id}/objects
GET  /v1/workspaces/{workspace_id}/objects/{object_id}
POST /v1/workspaces/{workspace_id}/objects/import
POST /v1/workspaces/{workspace_id}/objects/ingest
```

Only `/healthz` is public. The readiness callback checks local services, not provider or
GPU availability. Upload/import/ingestion require workspace write scope; metadata requires
read scope. They return 201 only after verified bytes and ownership metadata commit.
Local import is disabled unless the operator composes a named, workspace-allowed root
manager. HTTPS ingestion is disabled unless a guarded input service is supplied; it only
fetches permitted public HTTPS sources, never arbitrary headers or provider credentials.

See [`../docs/auth-and-objects.md`](../docs/auth-and-objects.md) for HTTP/ownership limits,
[`../docs/packaging-and-import.md`](../docs/packaging-and-import.md) for import/bundle safety,
and [`../docs/https-ingestion.md`](../docs/https-ingestion.md) for SSRF policy, streaming
budgets and URL privacy. Runtime token/object repositories are still test fixtures until
persistent SQLite composition is implemented.

These eight operations remain `planned`:

```text
POST /v1/workspaces/{workspace_id}/jobs/validate
POST /v1/workspaces/{workspace_id}/jobs
GET  /v1/workspaces/{workspace_id}/jobs/{job_id}
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/cancel
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/retry
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/reconcile
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/collect
GET  /v1/workspaces/{workspace_id}/operations/{operation_id}
```

The root status remains `planned` because a production CLI/SQLite runtime is not composed
yet. Artifact transfer, listings, logs, events, attempts, profiles and quota remain approved
API work and must be added with their actual application/storage behavior.

`GET` operations are observational. Compute creation requires an explicit `POST`, and job
creation/compute retry expose an `Idempotency-Key` requirement. Provider names, notebook
slugs, credential material, and provider filesystem paths do not appear in the normal
contract. `INVALID_REQUEST` and `REQUEST_LIMIT_EXCEEDED` distinguish local request
validation/backpressure from provider failures. Import source changes use `INPUT_CHANGED`;
unsafe local paths use `INVALID_INPUT_PATH` without exposing absolute host paths.

## Validation commands

Use the repository wrappers:

```bash
./scripts/dev.sh contract-check
./scripts/dev.sh contract-lock
./scripts/dev.sh check
```

PowerShell and CMD wrappers accept the same task names.

`contract-check` performs entirely local validation:

1. parses the strict manifest and requires every schema to be accounted for;
2. rejects remote, absolute, platform-dependent, or escaping `$ref` values;
3. compiles every root schema as Draft 2020-12 with format assertions;
4. requires every positive fixture to pass and every negative fixture to fail;
5. compares public enum arrays against the Go domain constants;
6. loads and validates OpenAPI 3.1.1 and its operation/status inventory; and
7. verifies `contract.lock.json` is current.

Tests additionally check actual serialized object/error/import/bundle/HTTPS-input types and
fixture decoding against their contracts. No fixture URL is fetched by schema validation.
`contract-lock` is the explicit update step after reviewing an intentional JSON contract or
fixture change. CI runs `contract-check`; CI never contacts Kaggle or allocates compute.

## Change policy

A contract change must update the schema, valid example, relevant negative fixture, lock,
and documentation together. Breaking changes must be visible in commit/PR history and use
the repository breaking-change convention. Do not generate clients or claim endpoint
support merely because a schema exists.
