# Contributing

Read [AGENTS.md](AGENTS.md), [current status](docs/status.md) and the relevant
[component guide](docs/README.md). Use the approved proposal and requirement IDs for design
constraints, not as proof that a proposed command or provider capability exists.

## Change workflow

Create one focused branch/PR. Use atomic [Conventional Commits](docs/development/commit-convention.md),
push checkpoints for long tasks and keep code/docs changes separate. Do not add an AI co-author,
rewrite shared history or push directly to `main` without explicit task authorization. Stop at
the owner-review boundary; do not merge or start the next task automatically.

Enable the local hook from the clone root:

```sh
./scripts/setup-git-hooks.sh
```

PowerShell/CMD equivalents are in `scripts/`. The hook supplements CI; it does not replace checks.

## Checks and evidence

```sh
go run ./cmd/devtool check
go run ./cmd/devtool test-race
```

Use [toolchain guidance](docs/development/go-toolchain.md) and narrow component tests during
implementation. PR descriptions should state behavior changed, exact checks/limits and external
effects. Fake-provider/SDK fixtures are not live acceptance.

For a real installation/account run, follow the [operator checklist](docs/development/validation-checklist.md),
update [results](docs/development/validation-results.md) and use the
[bug template](docs/development/bug-report-template.md). Only sanitized reports enter Git.
Credentials, binaries, private raw logs, inputs and runtime databases stay on the operator host.

## Keep docs useful

Usage guides describe current commands. Component references explain contracts and limitations.
ADRs capture material decisions and consequences. Plans contain remaining work. Changelog entries
summarize user-visible features, not every audit/commit/test run. Remove stale completion stories;
Git/PR history already preserves them. Do not copy owner or agent messages into documentation.

Live provider facts need dated, versioned primary-source evidence. Do not add account farming,
quota evasion, browser/anti-idle bypasses, hidden retries or automatic paid fallback. See
[SECURITY.md](SECURITY.md) for private vulnerability reporting.

Contributions use [Apache-2.0](LICENSE); preserve third-party notices and terms for data/models.
