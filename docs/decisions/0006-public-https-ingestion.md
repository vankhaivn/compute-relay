# ADR-0006: Pin public HTTPS connections before immutable input publication

- **Status:** accepted
- **Date:** 2026-09-14
- **Task:** M2-08
- **Requirements:** DAT-01, DAT-03, SEC-02
- **Related decisions:** ADR-0004 (object publication) and ADR-0005 (local import)

## Context

The approved proposal requires public HTTPS inputs to be downloaded on the operator's
runtime before remote compute, with DNS/IP/redirect checks and bounded time/bytes. A URL
is untrusted input, not a grant to read internal services or pass application credentials.
Authenticated/private sources can already use upload or allowlisted local import.

An ordinary shared HTTP client can follow redirects, reuse connections and inherit proxies
in ways that hide the actual destination or forward unwanted metadata. Input publication
must also wait for verified EOF rather than accepting a partially downloaded body.

## Decision

Use a small standard-library-only `httpsinput` transport, a consumer-owned
`objects.HTTPSFetcher` seam and an opt-in HTTP handler. The production implementation:

- accepts public HTTPS on 443 only, with an optional exact-host allowlist that applies to
  redirects and never overrides the address policy;
- rejects ambiguous URL forms, embedded credentials, fragments and common signing/token
  query keys; signed/private URLs and custom headers are not supported;
- validates all bounded DNS answers, rejects mixed public/private sets, dials one numeric
  allowed address, and verifies the connected peer before TLS/HTTP;
- repeats validation and DNS resolution on every redirect, including same-host redirects;
- verifies normal TLS identity with a minimum of TLS 1.2;
- creates a fresh non-reusing HTTP/1 transport for each hop, with no ambient proxy, cookie
  jar, incoming Authorization, Referer, transparent decompression or automatic retry;
- bounds URL/header/body size, redirects, DNS answer count, concurrency, DNS/connect, TLS,
  header, idle-read and total-operation time;
- returns fixed sanitized transport errors rather than URLs or upstream diagnostics; and
- exposes successful EOF only after size/length/digest checks, allowing the existing
  object service to publish verified bytes and commit ownership without weakening ADR-0004.

Only the test package may replace private resolver/dial/TLS seams for a loopback fixture.
There is no exported insecure certificate or private-network bypass. The runtime's
composition root remains trusted code and must use the guarded implementation.

## Address policy and reviewed sources

The address policy deliberately rejects broader special-use ranges, including some IANA
entries marked globally reachable. Ordinary global-unicast classification alone is not
sufficient. IPv6 is limited to 2000::/3 with additional special/documentation/transition
ranges denied; mapped/translation alternatives cannot reach blocked IPv4 destinations.
Azure's 168.63.129.16 platform virtual IP is explicitly blocked as well.

Primary sources reviewed on 2026-09-14:

- [OWASP SSRF Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html): DNS A/AAAA validation, redirect and DNS-pinning considerations.
- [IANA IPv4 Special-Purpose Address Registry](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml) and [IPv6 registry](https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml): special-purpose address classifications.
- [Microsoft's Azure platform IP description](https://learn.microsoft.com/en-us/azure/virtual-network/what-is-ip-address-168-63-129-16): platform-service address despite its public-looking form.
- [Go net/http documentation](https://pkg.go.dev/net/http): transport connection reuse, automatic retry conditions, proxies and timeout controls.

These sources inform the defensive implementation, not a claim of perfect SSRF prevention.
Operator-controlled DNS, routing, trust stores and host administrators remain outside the
application's trust boundary. Future changes to the public-address policy require source
review, explicit regression tests and this decision's documented compatibility limits.

## Alternatives and trade-offs

**Shared default client:** rejected because proxy inheritance, redirect behavior and reused
connections complicate endpoint and metadata guarantees.

**URL-only or pre-resolution validation:** rejected because a later resolution/redirect can
change the destination. The connected endpoint must match the validated numeric address.

**Private-network exception or signed URLs now:** rejected for MVP. Use existing uploads or
local imports rather than weaken public ingestion before a credential-reference design.

**Mandatory host allowlist:** not selected as the only mode because the approved scope is
public HTTPS inputs. Operators may narrow the public-only policy with an exact allowlist;
leaving it empty does not permit special/private addresses.

The conservative address policy can reject legitimate special-purpose destinations. A
single selected IP and no retry favors a reviewable failure boundary over availability.
HTTP/1 with new connections trades performance for straightforward per-hop validation.
Common secret-key checks cannot identify every sensitive URL path/query; callers must
submit public sources and avoid logging raw request JSON.

## Verification and consequences

Unit/fault tests cover DNS answer sets, rebinding, connected-peer mismatch, TLS trust/name,
redirect downgrade/credentials/allowlist, encoded/range/oversized/truncated responses,
timeout layers, cancellation and sanitized errors. A finite loopback TLS smoke verifies
4,480,000 streamed bytes and their digest. API/service tests cover workspace authority,
revocation, absent partial objects, commit ambiguity and no source refresh on GET.

The route returns a new immutable object; it does not modify old snapshots or create a job.
Production repositories and configuration composition remain M3/M5. No new dependencies,
workflows, credential provisioning or real provider/GPU tests are introduced.

See [`../https-ingestion.md`](../https-ingestion.md) for request semantics, bounds and commands.
