# Development documentation

For contributors and maintainers. To use or operate Compute Relay, start with
[Using Compute Relay](../README.md) or [Getting started](../getting-started.md).

## Contributor workflow

- [Repository instructions](../../AGENTS.md) and [contributing](../../CONTRIBUTING.md)
- [Go toolchain and checks](go-toolchain.md)
- [Commit convention](commit-convention.md)
- [Validation checklist](validation-checklist.md) and [bug report template](bug-report-template.md)

## Product and architecture authority

- [Approved proposal](proposal.md) and [scope and requirement IDs](scope-and-requirements.md)
- [Architecture](architecture.md) and [domain model](domain-model.md)
- [Architecture decisions](decisions/README.md)

The proposal and requirements define intended scope. [Current status](../status.md) defines what
is available now.

## Component references

- [Auth and objects](auth-and-objects.md)
- [HTTPS ingestion](https-ingestion.md)
- [Storage](storage.md)
- [Admission](admission.md)
- [Scheduling and dispatch](dispatch.md)
- [Controls and operations](operations.md)
- [Collection](collection.md)
- [Retention](retention.md)
- [Managed connections](managed-connections.md)
- [Offline fault matrix](fault-matrix.md)
- [HTTP/API contracts](../../api/README.md)
- [Remote runner](../../runner/README.md)

## Provider work and planning

- [Provider references](providers/README.md)
- Research: [pinned Kaggle interface evidence](research/kaggle-interface-review.md) and
  [feasibility gates](research/kaggle-feasibility.md)
- [Remaining implementation plan](implementation-plan.md)
- [Risk register](risk-register.md)

Keep completed development history in Git/PRs rather than in living documentation. Keep full
provider/operator evidence private; only focused sanitized evidence that changes a support
conclusion belongs in an issue/PR.
