# ADR-0006: Validate each public HTTPS connection before publication

Status: accepted. Requirements: DAT-01/03, SEC-02.

## Decision

Accept public HTTPS on port 443 only, optionally narrowed by exact-host allowlist. Reject
ambiguous URLs, userinfo/fragments and common signing/token query forms; custom headers and
private/signed URLs are not this ingestion mode. Use upload for private sources.

Validate all bounded DNS answers, reject mixed/prohibited addresses, dial an allowed numeric
address and check its connected peer and normal TLS identity (minimum TLS 1.2). Repeat on every
redirect, even same-host. Use fresh non-reusing HTTP/1 per hop, without ambient proxies, cookies,
Authorization/Referer forwarding, transparent decompression or automatic retry. Bound URL/headers/
bytes/redirects/concurrency and each network/operation stage; sanitize errors.

The conservative address policy is stricter than global unicast: reject special-use ranges,
IPv4 mapping/translation bypasses and Azure's platform address 168.63.129.16. IPv6 is restricted
to 2000::/3 minus excluded special/documentation/transition ranges. No public insecure bypass.
Publish immutable input only after verified length/hash/EOF through the existing object service.

## Reason and consequences

Shared default clients and pre-resolution-only validation hide redirect/rebinding endpoints.
Fresh connections and conservative ranges sacrifice some availability/performance. Operator DNS,
routing/trust stores remain trusted, and URL heuristics cannot detect every secret. Changes to
address policy need primary-source review and regressions, not a compatibility exception.

Sources reviewed 2026-09-14: [OWASP SSRF guidance](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html),
[IANA IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml)/[IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml),
[Azure platform IP](https://learn.microsoft.com/en-us/azure/virtual-network/what-is-ip-address-168-63-129-16)
and [Go HTTP](https://pkg.go.dev/net/http). See [ingestion](../https-ingestion.md).
