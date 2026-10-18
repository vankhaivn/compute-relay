# API contracts

[schemas/](schemas/) holds the JSON Schemas for every HTTP request, receipt and projection;
[examples/](examples/) holds valid documents and deliberately invalid ones under `examples/invalid/`.
Example names start with their schema name (`job.*` uses `job-spec`). The runtime embeds these same
schemas.

| Identifier | Version |
|---|---|
| HTTP path prefix | `/v1` |
| Job spec and status | `compute-connector/v1alpha1` |
| Result manifest | `1` |
| Bundle manifest | `compute-relay/bundle/v1` |
| Runner contract | `compute-relay/runner/v1` |

## Routes

All routes except `/healthz` need `Authorization: Bearer <token>`. Workspace routes are under
`/v1/workspaces/{w}`. Every POST that changes state takes an `Idempotency-Key`; replaying the same
request returns the original receipt, and a changed request with the same key conflicts.

| Route | Token | Doc |
|---|---|---|
| `GET /healthz`, `/readyz`, `/v1/info` | public / any | [Getting started](../docs/getting-started.md) |
| `POST /objects`, `GET /objects/{id}` | write / read | [Jobs](../docs/jobs.md#upload-and-admit) |
| `POST /jobs`, `POST /jobs/validate`, `GET /jobs/{id}` | write / read | [Jobs](../docs/jobs.md) |
| `POST /jobs/{id}/authorize`, `GET …/authorizations/{a}` | execute | [Jobs](../docs/jobs.md#authorize-an-attempt) |
| `POST /jobs/{id}/prepare`, `/start`; `GET …/readiness`, `…/gate-authorizations/{a}` | execute; readiness read | [Prepare and start](../docs/prepare-start.md) |
| `POST /jobs/{id}/workload-secrets`, `…/{a}/revoke`; `GET …/{a}` | execute | [Prepare and start](../docs/prepare-start.md#3-supply-secrets-optional) |
| `GET /jobs/{id}/logs`, `/artifacts`, `/artifacts/{a}`, `/artifacts/{a}/content` | read | [Results](../docs/results-and-cleanup.md#download) |
| `POST /jobs/{id}/cancel`, `/retry`, `/reconcile`, `/collect`, `/cleanup`; `GET /cleanup` | operate | [Jobs](../docs/jobs.md#observe-and-control), [Results](../docs/results-and-cleanup.md) |
| `GET /operations/{id}` | read | [Results](../docs/results-and-cleanup.md#cleanup) |
| `GET /providers`, `GET /connections[/{id}]`, `GET /connection-operations/{id}` | read | [Connections](../docs/connections.md) |
| `POST /connections`, `POST /connections/{id}/actions` | manage | [Connections](../docs/connections.md) |
| `GET /connections/{id}/health` | read | [Host health](../docs/ssh-host-health.md) |
| `…/host-watchdog/actions/{action}`; `GET …/host-watchdog`, `/host-watchdog-operations/{id}` | manage; read | [Host health](../docs/ssh-host-health.md#host-watchdog) |
| `GET /connections/{id}/storage`, `/storage-cleanup-operations/{id}` | read | [SSH storage](../docs/ssh-storage.md) |
| `GET`/`POST /connections/{id}/storage/orphans/{o}/cleanup` | manage | [SSH storage](../docs/ssh-storage.md#storage-view-and-orphans) |

Parsers reject duplicate keys, invalid Unicode, trailing data and unknown fields. Schema validity
alone does not prove authority, verified bytes or a completed download.

## Changing a contract

Update the schema with positive and negative examples in the same change, keep stable versions and
the Studio wire contract (`internal/api/studio_contract_test.go`), then run:

```sh
go test ./internal/contracts ./internal/api
```
