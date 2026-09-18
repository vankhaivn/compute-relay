# Outcome roadmap

This is the path to a usable release. [Current status](../status.md) describes today's software;
[remaining tasks](implementation-plan.md) and the [validation checklist](validation-checklist.md)
identify the work needed to advance. No release dates are promised.

| Outcome | Current boundary | Exit condition |
|---|---|---|
| M0 — Scope and evidence | Product brief, stable requirements and design constraints available. | Preserve the approved scope and explicit provider unknowns. |
| M1 — Real-provider proof | Offline harness available; live evidence outstanding. | Authorized private input → bounded actual GPU work → identified, verified outputs, plus timeout/fault/capability conclusions and provider go decision. |
| M2 — Portable core | Implemented offline. | Provider-neutral contracts, access controls, input handling and finite runner remain regression-tested. |
| M3 — Durable orchestration | Implemented offline. | Preserve receipts, one-shot intents, uncertainty, collection pins and recovery under fault tests. |
| M4 — Integrated Kaggle batch | Components and fixed acceptance harness implemented; live acceptance outstanding. | Exact-account, private, finite execution and verified results survive a separate resume process; retain other M1 requirements. |
| M5 — Usable developer product | Local admission/CLI/artifact delivery available; general workers and product integration unfinished. | Complete runtime configuration, worker lifecycle, remaining user commands, doctor, clients, examples and installation workflow. |
| M6 — Release hardening | Outstanding. | Security, native host evidence, coordinated recovery, safe cleanup, reproducible artifacts and final traceability review. |

Offline progress does not waive the first-provider proof. Operator validation can proceed
with the fixed acceptance utility before general runtime integration is finished; it must
not be described as arbitrary-job execution through `serve`.

If private staging, identity-safe results or bounded execution fails, retain the exact blocker.
Do not substitute public resources, browser automation, an always-on worker, another account
or a paid fallback. Missing optional capabilities must remain unsupported/unknown rather than
being simulated as successful cancellation, live logs or hardware release.
