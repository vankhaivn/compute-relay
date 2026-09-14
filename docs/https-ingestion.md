# Bounded public HTTPS input ingestion

> **Task:** M2-08, implemented offline; PR #9 in review.
>
> **Requirements:** DAT-01, DAT-03, SEC-02; proposal sections 10.4 and 19.5.
>
> **Boundary:** operator-hosted input preparation, not provider execution. Production
> SQLite/configuration/server composition remains M3/M5 work.

## API and composition

`POST /v1/workspaces/{workspace_id}/objects/ingest` accepts an unencoded
`application/json` request with exactly `url` and optional `sha256`. The decoder rejects
unknown fields, duplicate fields, case aliases, nulls and trailing JSON. A request example
is a contract illustration, not a download performed by this repository:

```json
{
  "url": "https://example.org/data.jsonl"
}
```

`sha256`, when provided, must be 64 lowercase hexadecimal characters. The schema and
positive/negative examples are in [`../api/`](../api/README.md). JSON Schema describes
structure only; passing it does not authorize a hostname or replace DNS/peer checks.

The route is disabled when `api.Config.HTTPSInputs` is nil. The operator's composition
root explicitly supplies an `objects.Ingestor` backed by `httpsinput.Client` and the
existing object service/repositories. No public URL is fetched by initialization, CI or
a default doctor command. The low-level resolver, dial and TLS seams are private to the
transport package; there is no exported insecure/private-destination switch.

Authentication and workspace write authorization happen before fetching. Ingestion shares
the existing HTTP upload concurrency gate, ordinary JSON body limit and upload deadline.
The transport also enforces its own bounds; the earlier applicable deadline wins.

Success returns `201`, a metadata `Location`, and the existing immutable object receipt:
`object_id`, `workspace_id`, `bytes`, `sha256`. The receipt contains neither the original
nor redirected URL. The object service revalidates authority before metadata commit.

## Allowed destinations

V1 accepts public HTTPS on port 443 only. An optional operator-configured `AllowedHosts`
list restricts exact normalized hostnames and applies to every redirect. No wildcard or
suffix matching is performed; an empty list still enforces the public-address policy.
An allowlist cannot override the private-address, TLS or port rules.

URL checks reject userinfo, fragments, whitespace/control bytes, ambiguous numeric IP
spellings, IPv6 zones/mapped literals, backslashes, non-443 ports and local-only names.
Common token/signature query keys are rejected, including AWS/Google signing parameters.
This is not a general secret detector: an arbitrary path or unknown query key may itself
be sensitive. Only public URLs are supported; do not submit signed/private URLs, and do
not log request JSON. Use upload or allowlisted local import for private sources.

At each hop the client:

1. validates the URL and exact optional host allowlist;
2. resolves an absolute DNS name without a search suffix;
3. checks the complete bounded A/AAAA answer set, rejecting mixed public/private answers;
4. dials one selected numeric public address, avoiding a second hostname resolution;
5. checks the connected peer address and port before sending TLS/HTTP data; and
6. verifies normal TLS certificate trust and hostname identity.

Nonpublic/special-use, loopback, private, link-local, metadata and platform destinations
are blocked. IPv6 is conservatively restricted to public unicast space with additional
special/transition ranges denied. Some special-use addresses that are globally reachable
are intentionally rejected as a compatibility trade-off. See
[ADR-0006](decisions/0006-public-https-ingestion.md) for the primary-source baseline and
policy update rules.

Every redirect repeats the checks, including DNS for same-host redirects. Redirects cannot
downgrade to HTTP or enable credentials. A fresh HTTP/1 transport per hop disables ambient
proxies, connection reuse, automatic redirect following and transparent decompression.
Requests are new GETs with fixed headers; no runtime Authorization, cookies, proxy
credentials, Referer or arbitrary client headers are forwarded.

## Bounds and failure behavior

| Bound | Default |
|---|---|
| URL / response headers / streaming buffer | 4 KiB / 32 KiB / 64 KiB |
| Input bytes | 2 GiB, further restricted by blob storage policy |
| Redirects / simultaneous fetches | 3 / 4 |
| DNS and connection budget | 10 seconds combined |
| TLS handshake / response headers | 10 / 15 seconds |
| Idle socket read / total operation | 15 / 120 seconds |
| DNS answers / optional allowlisted hosts | 16 / 256 maximum |

The total budget covers redirects, TLS, body transfer and the consuming callback; idle
read deadlines do not allow an endless slow trickle. No compute is allocated. There is no
automatic retry, alternate-IP retry, resume, or source substitution in this implementation.
A new explicit call creates a new snapshot, not an update to an old object.

Only a complete 200 response is accepted. Partial/range responses, encoded content,
excessive headers, premature EOF, unexpected source errors, too many redirects, oversized
known/unknown-length bodies and digest mismatches fail. The client does not download or
expose upstream error bodies and does not drain redirect bodies to reuse connections.

Malformed requests and blocked/failed inputs produce sanitized 400 errors; authentication
and authorization use 401/403; timeout uses 408; size uses 413; unsupported request media
uses 415; local concurrency/rate pressure uses 429; local repository/storage failure uses
503. Upstream errors do not become provider quota or compute failures. No error includes
raw URLs, query values, upstream headers/body, certificate details or resolver diagnostics.

## Immutable publication

`httpsinput.Client.Fetch` passes a bounded verified reader to the trusted ingestion service.
It yields successful EOF only after the complete body, declared length where available,
and optional expected digest have been checked. Failures are sticky: another read cannot
convert a failed transfer into successful EOF.

`objects.Ingestor` reuses `Service.Upload`, so the existing blob publication and ownership
commit order remains authoritative. Failed transfers do not commit metadata or expose
partial objects. Complete blobs survive a failed/ambiguous ownership acknowledgement for
later reconciliation; they are not speculatively deleted. A late error can therefore mean
an acknowledgement was lost, not that all local writes were rolled back.

The source URL is not retained as authority to refresh the object. Later GETs read the
committed snapshot, never the network source. Future job admission must freeze this object
before dispatch and preserve it across attempts; M2-08 does not implement the M3 scheduler
or claim durable job admission from the nondurable test repository.

## Executable offline verification

With the repository's pinned Go toolchain, from its root:

```text
go test -race ./internal/httpsinput ./internal/objects ./internal/api ./internal/contracts
go test -count=25 ./internal/httpsinput ./internal/objects ./internal/api
go test -run TestLoopbackTLSSmoke -v ./internal/httpsinput
go test -run=^$ -fuzz=FuzzRequestPolicy -fuzztime=5s -parallel=2 ./internal/httpsinput
```

The TLS smoke streams 4,480,000 synthetic bytes through a controlled redirect into a
verified local file. It checks size/digest, chunked transfer and absence of ambient
headers/proxy use. Test-only seams map a logical public peer to a loopback TLS fixture;
certificate verification remains enabled with only the fixture's CA trusted. This proves
local transport behavior, not Internet connectivity, a public endpoint or Kaggle support.

Tests cover mixed/private DNS, rebinding, mismatched peers, invalid certificate trust and
hostname, redirect policy, all timeout layers, cancellation, limits, sticky errors,
authorization before fetching, revocation during ingestion, commit ambiguity, no refresh
on reads, and sanitized API errors. The contract suite checks actual request serialization
and fixture decoding without fetching any URL.

Continuation validation ran the exact transport sources on Linux Go 1.23.2 using a
separate temporary module file because toolchain downloads were unavailable. Vet, race
coverage (92.5%), 25 repetitions, 63,940 fuzz executions and the compiled TLS-smoke test
binary passed. Committed go.mod/go.sum are unchanged; full-repository and native-platform
validation runs in existing offline CI using Go 1.27.1. Neither the local smoke nor CI
uses provider credentials, public endpoint downloads or GPU compute.
