# Research and evidence

Research documents record mutable provider facts and feasibility evidence. They do not
replace approved product requirements or turn upstream documentation into proof that a
configured account can execute a workflow.

## Current evidence set

| Document | Scope | Current evidence |
|---|---|---|
| [`kaggle-interface-review.md`](kaggle-interface-review.md) | Official client release/source review, transport boundary, identity risks, and M-1 probes. | `documented-upstream`; no account access. |
| [`kaggle-feasibility.md`](kaggle-feasibility.md) | K-01 through K-16 capability/evidence ledger and go/no-go rule. | Upstream review complete; live checks `blocked-environment`. |

## Required evidence fields

Each material probe or source review should record:

```text
Question:
Evidence level: documented-upstream | passed-offline | passed-live | not-tested | unsupported | blocked-environment
Checked date:
Primary source or exact procedure:
Provider/client version and commit/tag:
Account/environment scope, sanitized:
Observed result:
Capability conclusion:
Fallback or operational consequence:
Artifacts/fixtures produced:
```

## Source quality

Prefer official provider documentation, released client source, versioned changelogs, and
reproducible authorized tests. Community issues may identify a risk or research lead but do
not establish an API contract by themselves.

Record exact versions and dates because provider documentation, quotas, CLI output,
authentication, and notebook behavior can change.

## Live-test safety

- Require explicit authorization and a finite budget.
- Never request or record credentials in chat or repository files.
- Use private test resources and synthetic non-sensitive data.
- Sanitize account identifiers, tokens, URLs, and provider response payloads.
- Clean up only connector-owned resources whose identity and terminal state are known.
- Mark unavailable credentials as `blocked-environment` and continue offline work.
- Separate read-only probes, provider-storage mutations, and compute-consuming probes.

## Interpretation

A parser fixture establishes parsing behavior. A fake provider establishes local contract
behavior. Upstream documentation establishes a feature to investigate. Only an authorized,
recorded live result establishes that the tested account/client/environment completed the
tested path.

A live result is still scoped: one tested account and version does not establish universal
quota, availability, startup time, accelerator model, or provider policy.
