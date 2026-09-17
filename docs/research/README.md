# Provider evidence references

These documents explain why the adapter uses particular official surfaces and what still
requires real verification. They are reference material, not onboarding or completed-work logs.

| Document | Role |
|---|---|
| [Pinned interface evidence](kaggle-interface-review.md) | Versioned primary sources and transport/identity limitations. |
| [Feasibility gates](kaggle-feasibility.md) | Stable K-01–K-16 questions and unverified live conclusions. |

For usage, read [provider guides](../providers/README.md). To execute checks, use the
[validation checklist](../development/validation-checklist.md) and update
[validation results](../development/validation-results.md). Keep one results record rather
than separate competing checklists in every research document.

## Evidence rules

Record the question, source/version or exact procedure, date, scope, observation, limitation
and supporting evidence when a conclusion changes. Prefer versioned official documentation,
released source and reproducible authorized tests. Community issues are research leads, not
API contracts. Formatting edits must not silently re-date an upstream review.

A fixture establishes local behavior; source establishes a surface to investigate; a live
result establishes only the tested account/client/workload. None proves universal capacity,
quota, timing or future compatibility. Missing optional capabilities can remain explicitly
unsupported, but required private execution/result criteria need evidence.

Live work requires explicit authorization and finite budgets, separately for account reads,
provider storage, compute and deletion. Use synthetic private data, sanitize reports, and keep
credentials, account exports, signed URLs and state outside Git. An ambiguous mutation must
not be repeated to obtain a cleaner result. Unsupported cleanup or fault procedures remain
blocked until a safe exact-target procedure exists; preserve recovery evidence.
