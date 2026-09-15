# Architecture decision records

Use ADRs for material choices that affect public contracts, dependency boundaries,
persistence, provider transport, security properties, compatibility, or major operational
defaults.

## Index

| ADR | Status | Decision |
|---|---|---|
| [`0001-official-kaggle-client-boundary.md`](0001-official-kaggle-client-boundary.md) | accepted | Isolate a pinned official-client environment; use CLI plus a narrow public-client bridge where structured results require it. |
| [`0002-attempt-scoped-kaggle-resources.md`](0002-attempt-scoped-kaggle-resources.md) | accepted | Use one persisted Kaggle execution resource per attempt plus manifest identity; make no exactly-once claim. |
| [`0003-go-and-sqlite-baseline.md`](0003-go-and-sqlite-baseline.md) | accepted | Start with Go 1.27.1 and a CGo-free `modernc.org/sqlite` driver family, exact dependency pin deferred to implementation. |
| [`0004-workspace-auth-and-atomic-objects.md`](0004-workspace-auth-and-atomic-objects.md) | accepted | Separate token authority, atomic blob publication and ownership metadata commit; no nondurable production fallback. |
| [`0005-bundle-format-and-rooted-import.md`](0005-bundle-format-and-rooted-import.md) | accepted | Use manifest-bound regular-file USTAR/gzip bundles, explicit selections and rooted, workspace-allowed local snapshots; no local extraction. |
| [`0006-public-https-ingestion.md`](0006-public-https-ingestion.md) | accepted | Revalidate DNS/peer/TLS on every HTTPS hop, bound transfers and publish immutable inputs only after verified EOF. |
| [`0007-finite-remote-runner.md`](0007-finite-remote-runner.md) | accepted | Keep one-attempt Python/Linux execution outside admission, with frozen identities, bounded supervision and honest provider-evidence limits. |
| [`0008-sqlite-durability-and-backup.md`](0008-sqlite-durability-and-backup.md) | accepted | Pin SQLite/libc, protect installation identity with OS locking and ordered migrations, and separate database-only backup/restore from complete runtime recovery. |
| [`0009-durable-idempotent-admission.md`](0009-durable-idempotent-admission.md) | accepted | Atomically persist canonical request identity, original receipt, job/attempt, frozen resolution and references; replay without remapping or executing compute. |
| [`0010-fair-scheduling-and-fenced-local-claims.md`](0010-fair-scheduling-and-fenced-local-claims.md) | accepted | Persist FIFO/round-robin fairness and fenced local ownership; keep remote capacity evidence independent from lease expiry and submission intent. |
| [`0011-one-shot-mutations-and-recovery.md`](0011-one-shot-mutations-and-recovery.md) | accepted | Freeze inputs, commit staging/submission identities before one-shot mutations, and recover uncertainty through fenced observation rather than replay. |

## Naming

```text
NNNN-short-kebab-case-title.md
```

Numbers are sequential and never reused. Copy [`template.md`](template.md) and replace every
placeholder.

## Status

Use one of:

- `proposed`
- `accepted`
- `superseded by ADR-NNNN`
- `deprecated`
- `rejected`

An accepted ADR records the decision at that point in time. Do not edit its outcome silently
after implementation. Add a superseding ADR when the decision changes; minor typo and link
corrections are acceptable.

## When an ADR is required

Examples include:

- selecting the SQLite driver or migration strategy;
- choosing CLI-only versus a pinned official-client Python bridge for Kaggle;
- changing the public job or HTTP versioning scheme;
- changing archive format or safety semantics;
- introducing a new provider capability or session resource model;
- changing authentication, credential, or workspace boundaries; or
- changing no-retry, fallback, or cleanup behavior.

Do not create ADRs for routine variable names, formatting, or an easily reversible library
helper.
