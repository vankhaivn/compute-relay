# Public HTTPS input snapshots

The optional ingestion component turns an approved public HTTPS source into immutable local
input bytes. The normal local server does not enable this route, and admitting a pending URL
does not fetch it. See [admission](admission.md) for the preparation boundary.

## Contract

`POST /v1/workspaces/{w}/objects/ingest` requires current write authority and the strict
[request schema](../api/schemas/object-ingest.v1alpha1.schema.json). The source must be public
HTTPS; an optional expected SHA-256 pins its content. Operator composition supplies the service;
a nil service disables it rather than falling back to unrestricted fetching.

Schema-valid URL syntax is not network authorization. Every destination and redirect is checked
against policy, DNS results, the connected peer and TLS identity. Private/loopback/link-local,
metadata, mixed or otherwise prohibited address results are rejected. Redirects cannot bypass
these checks. Requests do not inherit ambient proxies, cookies or provider credentials.

## Bounded transfer and immutability

Time, redirects, bytes and concurrency are bounded by the service policy. Stream length/digest,
EOF and late errors must be verified before [object publication](auth-and-objects.md). A successful
HTTP prefix is not successful input ingestion. Error responses do not reveal internal paths,
credentials or arbitrary upstream response bodies.

The first committed object snapshot becomes the input identity. Once a job role is frozen,
restart/retry uses those stored bytes; it must not refetch a changed URL to satisfy the original
job. An uncommitted preparation can still fail/retry under the original guarded policy, but no
completed input is silently refreshed. Network checks do not establish rights to uploaded data.

## Verification boundary

Offline TLS/DNS/redirect fixtures test enforcement, not arbitrary real Internet behavior.
For operator evidence use the [validation checklist](development/validation-checklist.md).
Do not change address guards or use a paid/private-storage workaround merely to complete a test.
See [ADR-0006](decisions/0006-public-https-ingestion.md) and [recovery](recovery.md).
