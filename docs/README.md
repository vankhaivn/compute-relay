# Documentation map

Start with the [repository README](../README.md). These documents describe how the software
works now; the approved product brief describes the intended product, not implemented commands.

## Use and verify

| Document | Purpose |
|---|---|
| [Current status](status.md) | Available behavior, missing integration and unverified live capabilities. |
| [Local runtime](local-runtime.md) | Build, initialize, manage access and run the loopback server. |
| [Application CLI](application-cli.md) | Configure admission profiles and use upload/job/control commands. |
| [Artifact delivery](artifact-delivery.md) | Read committed results and verify create-only downloads. |
| [Recovery](recovery.md) | Interpret uncertainty and choose safe recovery without resubmitting compute. |
| [Operator validation checklist](development/validation-checklist.md) | Values, commands, authorization and pass/fail criteria for a local agent. |
| [Validation results](development/validation-results.md) | Current run/gate outcomes and reproducible failure handoff. |
| [Bug report template](development/bug-report-template.md) | Minimum sanitized evidence for a fix and a retest. |

## Contributor references

[Contributing](../CONTRIBUTING.md) and [AGENTS.md](../AGENTS.md) define workflow.
[Toolchain and checks](development/go-toolchain.md),
[commit convention](development/commit-convention.md) and
[repository workflow](development/repository-workflow.md) cover development mechanics.
[Implementation plan](implementation-plan.md) contains remaining work;
[roadmap](roadmap.md) describes release outcomes, not a completed-task diary.

The [approved proposal](proposal.md) and [requirement IDs](scope-and-requirements.md) remain
the product authority. Their proposed examples are not a substitute for current usage guides.
[Architecture](architecture.md), [domain model](domain-model.md) and
[architecture decisions](decisions/README.md) explain current design constraints.

## Component references

- Inputs and access: [auth/objects](auth-and-objects.md),
  [packaging/import](packaging-and-import.md), [HTTPS ingestion](https-ingestion.md).
- Durable work: [storage](storage.md), [admission](admission.md),
  [scheduler](scheduler.md), [dispatch](dispatch.md), [controls](operations.md).
- Results: [collection](collection.md), [retention](retention.md),
  [offline fault matrix](fault-matrix.md).
- Interfaces: [API contracts](../api/README.md), [provider contract](providers/contract.md),
  [remote runner](../runner/README.md).
- Kaggle: [preflight](providers/kaggle-preflight.md), [staging](providers/kaggle-staging.md),
  [execution](providers/kaggle-execution.md), [operations](providers/kaggle-operations.md),
  [artifacts](providers/kaggle-artifacts.md), [fixed acceptance experiment](providers/kaggle-acceptance.md).

[Compatibility](compatibility.md), [current risks](risk-register.md) and
[provider research](research/README.md) retain evidence boundaries and version-pinned sources.

## Keep this structure usable

Usage belongs in usage guides; current constraints belong in component references; new run
results belong in the validation ledger. Do not append commit lists, local sandbox stories,
CI run transcripts, owner prompts or completed implementation audits to these pages.
Preserve requirements, safety constraints and relevant dated source evidence when editing.
