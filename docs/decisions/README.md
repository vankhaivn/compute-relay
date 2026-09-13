# Architecture decision records

Use ADRs for material choices that affect public contracts, dependency boundaries, persistence, provider transport, security properties, compatibility, or major operational defaults.

## Naming

```text
NNNN-short-kebab-case-title.md
```

Numbers are sequential and never reused. Copy [`template.md`](template.md) and replace every placeholder.

## Status

Use one of:

- `proposed`
- `accepted`
- `superseded by ADR-NNNN`
- `deprecated`
- `rejected`

An accepted ADR records the decision at that point in time. Do not edit its outcome silently after implementation. Add a superseding ADR when the decision changes; minor typo and link corrections are acceptable.

## When an ADR is required

Examples include:

- selecting the SQLite driver or migration strategy;
- choosing CLI-only versus a pinned official-client Python bridge for Kaggle;
- changing the public job or HTTP versioning scheme;
- changing archive format or safety semantics;
- introducing a new provider capability or session resource model;
- changing authentication, credential, or workspace boundaries; or
- changing no-retry, fallback, or cleanup behavior.

Do not create ADRs for routine variable names, formatting, or an easily reversible library helper.
