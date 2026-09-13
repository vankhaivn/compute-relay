# API contracts

> **Task:** M2-03 — strict JSON Schema and OpenAPI baseline.
>
> **Status:** implemented offline for review. The OpenAPI operations are marked `planned`;
> this directory does not claim that an HTTP server or provider integration exists yet.

## Contract versions

- HTTP API prefix: `/v1`
- OpenAPI: `3.1.1`
- JSON Schema dialect: Draft 2020-12
- Job/status API version: `compute-connector/v1alpha1`
- Runner result-manifest version: `1`
- Normalized runtime-config schema version: `1`

The working job version intentionally keeps the proposal's `compute-connector/v1alpha1`
identifier while repository branding remains Compute Relay. Changing a published identifier
later requires an explicit compatibility decision rather than a silent rename.

## Files

| Path | Purpose |
|---|---|
| [`openapi.json`](openapi.json) | Provider-neutral `/v1` HTTP skeleton. Every operation is explicitly `planned`. |
| [`schemas/common.v1alpha1.schema.json`](schemas/common.v1alpha1.schema.json) | Shared typed IDs, digests, state/error/capability enums, and safe relative paths. |
| [`schemas/job-spec.v1alpha1.schema.json`](schemas/job-spec.v1alpha1.schema.json) | Immutable Python/shell batch job request. |
| [`schemas/result-manifest.v1alpha1.schema.json`](schemas/result-manifest.v1alpha1.schema.json) | Generic remote runner result manifest. |
| [`schemas/runtime-config.v1alpha1.schema.json`](schemas/runtime-config.v1alpha1.schema.json) | Normalized non-secret runtime configuration. TOML decoding is a later task. |
| [`schemas/job-status.v1alpha1.schema.json`](schemas/job-status.v1alpha1.schema.json) | Truthful independent attempt-state dimensions. |
| [`schemas/operation.v1alpha1.schema.json`](schemas/operation.v1alpha1.schema.json) | Durable cancel/retry/reconcile/collect/cleanup operation representation. |
| [`schemas/error.v1alpha1.schema.json`](schemas/error.v1alpha1.schema.json) | Stable error envelope without a dangerous generic `retryable` flag. |
| [`schemas/job-admission.v1alpha1.schema.json`](schemas/job-admission.v1alpha1.schema.json) | Durable asynchronous admission response. |
| [`schemas/job-validation.v1alpha1.schema.json`](schemas/job-validation.v1alpha1.schema.json) | No-compute validation response and verification requirements. |
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

## OpenAPI status

The OpenAPI document defines only the first provider-neutral metadata/control slice:

```text
GET  /healthz
GET  /readyz
GET  /v1/info
POST /v1/workspaces/{workspace_id}/jobs/validate
POST /v1/workspaces/{workspace_id}/jobs
GET  /v1/workspaces/{workspace_id}/jobs/{job_id}
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/cancel
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/retry
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/reconcile
POST /v1/workspaces/{workspace_id}/jobs/{job_id}/collect
GET  /v1/workspaces/{workspace_id}/operations/{operation_id}
```

This is a skeleton, not endpoint implementation. Binary object/artifact transfer, listings,
logs, events, attempts, profiles, and quota remain approved API work but should be added with
their application/storage behavior rather than optimistic empty handlers.

`GET` operations are observational. Compute creation requires an explicit `POST`, and job
creation/compute retry expose an `Idempotency-Key` requirement. Provider names, notebook
slugs, credential material, and provider filesystem paths do not appear in the normal
contract.

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
6. loads and validates OpenAPI 3.1.1 and its exact planned operation inventory; and
7. verifies `contract.lock.json` is current.

`contract-lock` is the explicit update step after reviewing an intentional JSON contract or
fixture change. CI runs `contract-check`; CI never contacts Kaggle or allocates compute.

## Change policy

A contract change must update the schema, valid example, relevant negative fixture, lock,
and documentation together. Breaking changes must be visible in commit/PR history and use
the repository breaking-change convention. Do not generate clients or claim endpoint
support merely because a schema exists.
