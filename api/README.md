# API contracts

> **Tasks:** M2-03 contracts; M2-05 through M2-08 auth/object input handlers;
> M3-02 durable job admission, validation and cached status; M3-05 durable controls.
>
> **Status:** contracts and fifteen composable handler operations are implemented offline.
> Scheduling and one-shot dispatch are separate offline components. Production composition,
> artifact collection and live-provider integration remain separate gates.

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
| [`schemas/control-request.v1alpha1.schema.json`](schemas/control-request.v1alpha1.schema.json) | Explicit attempt target and optional non-secret reason; the retry definition requires a nonblank reason. |
| [`schemas/control-operation.v1alpha1.schema.json`](schemas/control-operation.v1alpha1.schema.json) | Actual HTTP control receipt/current view: effect, replay, termination evidence, optional new attempt, safe problem and links. |
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
bounded labels or diagnostic details. Public control problems exclude arbitrary details.
Schemas validate:

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

Some invariants require runtime context or arithmetic. Admission additionally checks
input-name/path collisions, setup/finalization budgets, reserved environment variables,
workspace ownership and current profile-policy bounds. Byte integrity, bundle layout,
provider capability and remaining preparation checks must still pass before dispatch.

The admission parser uses these embedded schemas with external loading disabled. It rejects
duplicate decoded keys, invalid Unicode, trailing values, excessive nesting and oversized
canonical expansion. Request identity uses a named integer-only format, not raw JSON bytes
or an RFC 8785 claim. See [`../docs/admission.md`](../docs/admission.md).

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

Control requests require `attempt_id`; they never resolve an implicit active attempt. The
runtime rejects duplicate keys, invalid UTF-8, nulls, unknown fields, bodies over 4,096 bytes
and reasons over 512 UTF-8 bytes. JSON Schema's character ceiling is not a substitute for
the byte limit. Retry requires a nonblank reason. Record validation additionally checks
identities, time ordering and source/new-attempt separation against durable context.

## OpenAPI status

The following ten base operations have offline component and HTTP integration evidence:

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

Only `/healthz` is public. The readiness callback checks local services, not provider or
GPU availability. Upload/import/ingestion require workspace write scope; metadata requires
read scope. They return 201 only after verified bytes and ownership metadata commit.
Local import is disabled unless the operator composes a named, workspace-allowed root
manager. HTTPS ingestion is disabled unless a guarded input service is supplied; it only
fetches permitted public HTTPS sources, never arbitrary headers or provider credentials.

Job creation requires write scope and exactly one 8–256 byte printable-ASCII
`Idempotency-Key`, with no whitespace. Job validation/status require read scope. Job,
first attempt/nonce, profile revision, object references, event and receipt commit together
before 202. Repeated equivalent requests return original IDs and `idempotency_replay=true`;
changed requests return 409. Replay preserves original resolution but still checks current
authority. No job handler executes uploaded commands, downloads URLs or calls a provider.

The admission service must be supplied explicitly; nil configuration has no nondurable
fallback. `/v1/info` reports whether that service is configured. Validation reports remaining
`before_dispatch` and `verify_after_start` checks rather than claiming provider eligibility.
Direct HTTPS input records can be durably accepted while still pending preparation.

See [`../docs/auth-and-objects.md`](../docs/auth-and-objects.md) for HTTP/ownership limits,
[`../docs/packaging-and-import.md`](../docs/packaging-and-import.md) for import/bundle safety,
[`../docs/https-ingestion.md`](../docs/https-ingestion.md) for SSRF policy, and
[`../docs/admission.md`](../docs/admission.md) for canonicalization, replay and frozen inputs.
SQLite repositories exist; production CLI/configuration composition remains separate.

M3-05 adds five `implemented-offline` control operations, bringing the total to fifteen:

```text
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/cancel
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/retry
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/reconcile
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/collect
GET  /v1/workspaces/{workspace_id}/operations/{operation_id}
```

Control POSTs require `operate` scope and exactly one `Idempotency-Key` with the same ASCII
bounds as job admission. GET requires `read`. The operation service must be composed
explicitly; `/v1/info` advertises the five control features only when it is supplied.
All successful POSTs return 202, `Location` and the strict control-operation view. A replay
returns the original immutable receipt with `replay=true`; GET returns the current durable
revision with `replay=false`. The control replay field is distinct from job admission's
`idempotency_replay` field. No HTTP control calls a provider; retry verifies local frozen
bytes, and the separately composed worker performs any permitted provider action.

Cancellation acknowledgement is not terminal evidence. Retry names both the source attempt
and a distinct new attempt. Reconcile never repeats submission. Collect accepts a durable
transfer-only ticket after terminal execution evidence; M3-06 still owns its consumer and
artifact verification/publication. See [`../docs/operations.md`](../docs/operations.md).

The root status remains `planned` because a production runtime is not composed yet.
Artifact transfer, listings, logs, events, attempts, profiles and quota retain their own
implementation gates. Receipt links reserve these contract locations; they do not claim
that the corresponding collection routes exist.

`GET` operations are observational. Compute creation requires an explicit `POST`, and job
creation plus every control POST expose an `Idempotency-Key` requirement. Provider names,
notebook slugs, credential material, and provider filesystem paths do not appear in the
normal contract. `INVALID_REQUEST` and `REQUEST_LIMIT_EXCEEDED` distinguish local request
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

Tests additionally validate serialized object/error/import/bundle/HTTPS-input types and
actual admission/validation/status/control HTTP responses against their contracts. Control
serializer tests check 880 state/effect/identity-presence/termination combinations against
the record validator, including rejection of false termination and invalid new-attempt
claims. No fixture URL is fetched by schema validation. `contract-lock` is the explicit
update step after reviewing an intentional JSON change. CI never contacts Kaggle or
allocates compute.

## Change policy

A contract change must update the schema, valid example, relevant negative fixture, lock,
and documentation together. Breaking changes must be visible in commit/PR history and use
the repository breaking-change convention. Do not generate clients or claim endpoint
support merely because a schema exists.
