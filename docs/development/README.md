# Development documentation

This directory is for contributors, maintainers and operator qualification work. If you cloned
Compute Relay simply to understand or operate it, start with [Using Compute Relay](../README.md),
[Getting started](../getting-started.md), or the [Operator runbook](../runbook.md).

## Contributor workflow

- [Repository instructions](../../AGENTS.md)
- [Contributing](../../CONTRIBUTING.md)
- [Repository workflow](repository-workflow.md)
- [Go toolchain and checks](go-toolchain.md)
- [Commit convention](commit-convention.md)
- [Validation checklist](validation-checklist.md)
- [Bug report template](bug-report-template.md)

## Product and architecture authority

- [Approved proposal](proposal.md)
- [Scope and requirement IDs](scope-and-requirements.md)
- [Architecture](architecture.md)
- [Domain model](domain-model.md)
- [Architecture decisions](decisions/README.md)

The proposal/requirements define intended product scope. [Current status](../status.md) defines
what is actually available now.

## Core component references

- [Auth and objects](auth-and-objects.md)
- [Packaging/import usage](../packaging-and-import.md)
- [HTTPS ingestion](https-ingestion.md)
- [Storage](storage.md)
- [Admission](admission.md)
- [Scheduler](scheduler.md)
- [Dispatch](dispatch.md)
- [Controls and operations](operations.md)
- [Collection](collection.md)
- [Retention](retention.md)
- [Offline fault matrix](fault-matrix.md)
- [Compatibility and support boundaries](../compatibility.md)
- [HTTP/API contracts](../../api/README.md)
- [Remote runner](../../runner/README.md)

## Provider work

Provider contracts, Kaggle internals and the bounded live acceptance path are grouped under
[providers](providers/README.md). These documents are not the normal end-user runbook.

Research and source-evidence records are under [research](research/README.md).

## Planning and release work

- [Remaining implementation plan](implementation-plan.md)
- [Roadmap](roadmap.md)
- [Risk register](risk-register.md)

Keep completed development history in Git/PRs rather than adding run diaries to living
documentation. Keep full provider/operator evidence private; only focused sanitized evidence that
changes a support conclusion belongs in an issue/PR.
