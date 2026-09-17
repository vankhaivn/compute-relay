# Published artifact API and verified application downloads

> **Task:** M5-01c, implemented offline in PR #27; in review until owner merge.
> **Requirements:** API-01/02/03, JOB-05, DX-01; preserves DUR-01/03 and retention history.
> **Boundary:** read already published M3 results. No provider, collector or compute activation.

M5-01b profile/application commands are merged in PR #26. This slice connects the existing
`collection.Reader` to loopback HTTP and adds application commands for metadata, pagination
and private local file delivery. The parent M5-01 task remains incomplete: provider/worker
lifecycle, remaining log/control surfaces and cleanup retain their own implementation gates.

## Before using the commands

Start the existing local `serve` against an initialized installation and use an application
token with current workspace `read` scope. Commands contact that running server; they do not
open SQLite or resolve provider credentials. Local administration still requires exclusive
installation ownership and a stopped server, as described in [local runtime](local-runtime.md).

The selected attempt must already have a committed M3 collection publication in this installation.
An accepted job or collect ticket, provider terminal observation, or complete uncommitted blob
is insufficient. A new admission-only installation does not create results merely because
these routes are installed. `serve` remains `local-admission-only`, `dispatch_enabled=false`:
no scheduler, provider worker, collector, input fetcher or retention sweeper starts here.

Use IDs from the actual job/publication. The following shapes use illustrative IDs and paths;
they do not seed a job, run a workload or provision a token:

```text
compute-relay job artifacts --workspace app --token-file /private/app.token --id job_example --attempt att_example --limit 100
compute-relay artifact show --workspace app --token-file /private/app.token --job job_example --attempt att_example --id art_example
compute-relay artifact download --workspace app --token-file /private/app.token --job job_example --attempt att_example --id art_example --output /private/results/chosen-name.bin
```

All commands accept `--url`, defaulting to `http://127.0.0.1:7331`. The existing literal-loopback
HTTP endpoint policy applies. Specify **both job and attempt**; there is no implicit latest
attempt. A download requires an existing private destination directory and a filename that
must not exist. Adapt token/output paths to the host platform; Windows paths are ordinary
command arguments, not JSON strings. `artifact --help` and malformed arguments perform no
network, token or state work.

`job artifacts` returns one bounded page. Repeat it explicitly with the returned `--cursor`
until `next_cursor` is the empty string. It does not automatically poll, traverse all pages,
call collect, or retry compute. `artifact show` returns one artifact's metadata. Download makes
one metadata GET and one content GET; no request is automatically replayed or redirected.

## HTTP contract and authorization

All three operations require current workspace read authority and an explicit `attempt_id`:

| Method and path | Result |
|---|---|
| `GET /v1/workspaces/{w}/jobs/{j}/artifacts?attempt_id={a}` | One sorted page; optional canonical `limit` (1–100) and `cursor`. |
| `GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}?attempt_id={a}` | One published artifact identity. |
| `GET /v1/workspaces/{w}/jobs/{j}/artifacts/{id}/content?attempt_id={a}` | Binary transfer with mandatory final acknowledgement. |

Unknown/duplicate query fields, request bodies, encoded paths, Range/If-Range and malformed
identifiers are rejected. HEAD is not an alternate read path; unsupported methods return 405.
No host path, underlying blob ID or provider URL is a public content selector. The optional
`api.Config.Results` reader disables these routes when nil; there is no provider fallback.
`/v1/info` advertises artifact features only when the reader is composed.

Metadata includes the workspace/job/attempt, result phase, publication verification time and
artifact ID/path/role/length/SHA-256. It describes **historical verified publication**, not a
new check of current disk bytes or a guarantee that the payload succeeded. Failed-job manifests
and retained logs can be valid artifacts. Download independently rechecks the bytes.

The publication is sorted by logical path. A cursor binds its entire file set, target and
verification timestamp, so it survives server/database reopen when that publication is unchanged.
A mismatched cursor returns 409 rather than mixing publications. The digest is a snapshot
identifier, not authorization or a signature. Every request checks current authority again.
The CLI decoder requires exact known keys and explicit end-of-pagination evidence while ignoring
compatible future response fields rather than echoing them.

Missing/invisible/unpublished results and uncomposed routes return 404. Expired publications
return **410**, using the existing `ARTIFACT_MISSING` code with explicit expiry semantics.
Current authority is checked before that disclosure: a revoked token still returns 401 rather
than revealing whether a publication expired. The existing result/receipt/event history is
not rewritten by these read-only requests.

## A 200 response or complete body is not a successful download

Content is always `application/octet-stream`, with attachment disposition using the opaque
artifact ID, `nosniff`, sandbox CSP and `Cache-Control: no-store`. The logical path is a label,
never a client destination or browser-rendering instruction. Retained stdout/stderr are
historical artifacts, not a live provider log or SSE feature.

The current protocol uses HTTP/1.1 chunked delivery without Content-Length. Identity headers
bind workspace, job, attempt and artifact; `X-Content-Bytes` and `X-Content-SHA256` describe the
exact pinned body. Before writing body bytes, the server declares:

```text
Trailer: X-Compute-Relay-Verified
```

Only after exact byte count/hash, explicit source EOF, successful source Close, live request
context and a final current-authority/unexpired-publication recheck does it send this trailer:

```text
X-Compute-Relay-Verified: true
```

A late read/Close error, panic, timeout, expiry or revocation aborts the response without the
success trailer. The server preserves `http.ErrAbortHandler`; it does not append JSON errors
to an already started binary stream. Previously delivered bytes cannot be recalled after a
token is revoked or a publication expires. The final check is not a lock against later changes.

A client must require matching identity headers, exact independent size/SHA-256, clean body
EOF/Close and **one declared final true trailer**. A value in the initial headers is not final
acknowledgement. Missing, duplicated or false trailers, fixed-length responses and compressed
content fail qualification, including for empty files. Generic clients that ignore trailers
must not label their downloads verified. Intermediaries that strip trailers cause failure;
transparent proxy/HTTP/2/browser compatibility is not claimed by this implementation.

The Go client uses a fresh non-reusing transport for each request, no ambient proxy or redirect
following and no automatic replay. Read failures retain sanitized HTTP status but do not claim
a mutation may have committed. Downloads do not reread a provider or cause collection.

## Private create-only local delivery

The CLI rejects an existing destination before token lookup or network access. It writes to a
private same-directory temporary file, checks the complete HTTP acknowledgement, flushes it,
then independently rereads/hashes the local file and verifies its identity/Close. Only then
does a same-directory **hard link create the final name without replacement**. A concurrent
creator cannot have its file overwritten. Unsupported hard links fail closed; ordinary rename
with overwrite is not a fallback.

The destination is chosen solely by `--output`, never by server path or Content-Disposition.
The final directory is synced and permissions checked. Stdout carries JSON metadata with
`delivery=verified-new-file`, not raw artifact content. If final linking succeeded but directory
sync, final validation or stdout reporting failed, diagnostics retain
`download_may_be_published=true`. Inspect the chosen file against its original size/digest;
do not assume rollback or silently replace it on retry.

Temporary names are removed on ordinary completion/failure. A process crash can leave a private
`.compute-relay-download-*` file; no automatic crash cleanup, resumable range download or
secure-erasure guarantee is added. Hard-link support, directory synchronization and permissions
remain subject to the tested local filesystem/platform. The same-user operator and private
parent directory are trusted, not a hostile-writer sandbox or general power-loss guarantee.
Do not remove an existing output automatically to make a repeated download succeed.

## Limits and operational interpretation

The public view allows at most 10,004 files and 4 GiB in one publication, pages of at most 100,
and JSON bodies at most 1 MiB. Manifest/provenance are at most 1 MiB each; retained stdout/stderr
at most 20 MiB each. Streams use 64 KiB buffers, not whole-file memory. Defaults retain the
server's 10-second metadata/control context and two-minute transfer context. Downloads share
the four-transfer admission pool with uploads, inside the overall 32-request bound; rate limits
and actual connection read/write deadlines still apply. The CLI bounds metadata work to 30
seconds and full delivery to two minutes. These limits are not a throughput guarantee for 4 GiB.

There is no byte-range resume, automatic retry, automatic collect, new execution, provider URL
fallback or state repair. Use existing explicit collect semantics only for genuinely failed
provider-to-local collection, not merely because a client failed to receive a local file.
Byte expiry, immutable receipts and database/blob backup limits remain the M3-07 contract.

## Verification and evidence

```text
go test -race ./internal/artifactwire ./internal/appclient ./internal/appcli ./internal/api ./internal/store/sqlite
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Unit and real-loopback tests cover strict targets/queries/pages, a 19 MiB streamed fixture,
empty files, false/missing/duplicate/early trailers, wrong headers/hashes, late EOF/Close/panic,
final expiry/revocation, no redirect/replay, private destination races, local rehash and stdout
failure after publication. Actual HTTP metadata/page responses validate against the additive
JSON Schema/OpenAPI contracts; the operation inventory grows from fifteen to eighteen.

The real SQLite/blob/M3 collection test first proves a pending ticket exposes no artifact,
then publishes five files through the existing collector. HTTP/CLI downloads and stable cursor
pagination survive database reopen, return exact protected bytes and preserve original collect
receipts, attempt state and events. Provider list/fetch counters do not advance during local
delivery. Expiry returns 410; revocation takes precedence; one publication and its history remain.
This is synthetic-provider component evidence, not a live Kaggle workflow or new process-kill test.

On local Go 1.23.2, the four artifactwire roots passed race detection with ten repetitions,
plus vet, using five exact Git-blob-verified source/test files in an unshipped standard-library
module. This is metadata/cursor/stream evidence only. Full pinned Go 1.27.1/modernc, native and
race integration comes from the existing offline CI recorded on the PR's final head. No
substitute driver, dependency downgrade or local full-runtime claim is made.

See [ADR-0023](decisions/0023-verified-artifact-delivery.md), [API contracts](../api/README.md),
[application CLI](application-cli.md), [collection](collection.md) and [retention](retention.md).
Go's [ResponseWriter/trailer and ErrAbortHandler contracts](https://pkg.go.dev/net/http) and
[server implementation](https://go.dev/src/net/http/server.go) were reviewed for this boundary.
The [implementation plan](implementation-plan.md) owns the current owner-review gate. Stop
after PR #27; no later M5-01 slice, M5-02 or live acceptance starts automatically.
