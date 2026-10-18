# Contributing

Read [design](docs/design.md) and the doc for the area you change. Keep public behavior and the
design invariants unless the change deliberately alters them.

## Workflow

1. Branch from `main` as `feat/`, `fix/`, `docs/`, `test/`, `refactor/` or `chore/`.
2. Commit per the [commit convention](docs/development.md#commit-convention); keep each fix with
   its regression tests.
3. Run `./scripts/check.sh` (see [Development](docs/development.md)).
4. Open a PR with a Conventional title. List the checks you ran and their results, checks you did
   not run and why, any authorized live effects, and remaining risks.

The owner reviews and merges. Merge, rebase, force-push, pushing to `main`, tags, deploys and
CI dispatch need explicit permission.

## Issues

Plain-sentence titles, no type prefix: a bug states the symptom, a feature starts with a verb.
Describe the problem and the proposed behavior, then `Acceptance:` with observable outcomes and
required checks. Report vulnerabilities privately per [SECURITY.md](SECURITY.md).

## Docs

Update the existing topic when behavior changes — one document per topic, current behavior only.
Put design reasons in [design](docs/design.md), evidence in the PR, and leave out development
history. Never commit credentials, account exports, databases or raw run logs.

Contributions are licensed under [Apache-2.0](LICENSE).
