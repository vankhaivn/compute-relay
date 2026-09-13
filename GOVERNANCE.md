# Governance

## Current model

Compute Relay is in an owner-led bootstrap stage. The repository owner is the final decision maker for product scope, release policy, maintainer appointments, and changes to the approved product direction.

The approved proposal is the current product brief. Contributors and maintainers may make routine technical choices within that brief without reopening settled scope questions.

## Decision process

- Small, reversible implementation choices are made in focused pull requests.
- Material architectural choices are recorded as ADRs under [`docs/decisions/`](docs/decisions/README.md).
- Provider capability claims require current primary-source research and, where necessary, live evidence.
- Changes to owner-approved requirements require explicit owner approval and corresponding documentation updates.
- Security-sensitive decisions should receive focused review and threat-model consideration.

Discussion should seek evidence and a workable decision rather than indefinite consensus. When contributors disagree, document the trade-offs and escalate the decision to the owner or an appointed maintainer.

## Maintainers

Maintainers are expected to:

- enforce repository instructions and scope boundaries;
- review for correctness, security, operational honesty, and maintainability;
- distinguish offline tests from provider evidence;
- protect contributor and operator secrets; and
- keep releases and compatibility claims traceable to tests and documentation.

A maintainer list and delegated responsibilities will be added when additional maintainers are appointed.

## Releases

There is no released runtime yet. Before the first release, the project will document versioning, supported platforms, compatibility evidence, security support, release signing/checksums, and a release checklist.
