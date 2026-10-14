# Contributing

Read [CLAUDE.md](CLAUDE.md), [design](docs/design.md) and the relevant [topic](docs/README.md).
Preserve public behavior and safety semantics unless the task deliberately changes them.

Branch from `main` under `feat/`, `fix/`, `docs/`, `test/`, `refactor/` or `chore/`.
Use focused [Conventional Commits](docs/development.md#commit-convention) and a Conventional PR title.
Keep a fix with its regression tests. No AI authorship or attribution in Git text.
An approved change permits normal branch push and opening a PR. Stop for owner review;
merge, rebase, force push, main push, tags, deploys and CI/CD triggers need explicit permission.

```sh
./scripts/setup-git-hooks.sh
./scripts/check.sh
```

The hook checks commit subjects; the check command covers formatting, Go and Python tests,
schemas and the fault matrix. See [development](docs/development.md) for pinned tools and CI.
State exact checks/results, unrun checks with reasons, authorized external effects and risks
in the PR. Offline fixtures are not live-provider evidence. Preserve uncertain state and original
binaries rather than retrying remote mutations to manufacture success.

Issue titles are plain sentences without a type prefix. A bug states the observed symptom;
a feature starts with an imperative verb. Explain the concrete problem, proposed behavior,
then `Acceptance:` with observable outcomes and required checks. Keep raw private run data,
credentials, account exports and runtime databases off Git. Report vulnerabilities privately
through [SECURITY.md](SECURITY.md).

Update the existing topic when behavior changes. Put essential design reasons in
[design](docs/design.md), provider limits in provider guides and PR-specific evidence in the PR.
Avoid development diaries, duplicated plans and speculative support claims.
Contributions use [Apache-2.0](LICENSE); preserve third-party notices and terms.
