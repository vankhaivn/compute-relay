# Contributing

Read [CLAUDE.md](CLAUDE.md), [current status](docs/status.md) and the guide for the task in the
[development documentation](docs/development/README.md). Use the approved proposal and requirement
IDs for design constraints, not as proof that a proposed command or provider capability exists.

## Change workflow

- Branch from current `main` with a short lowercase name under `feat/`, `fix/`, `docs/`, `test/`,
  `refactor/` or `chore/`.
- Use atomic [Conventional Commits](docs/development/commit-convention.md). Keep a fix and its
  regression tests together and code and documentation in separate scoped commits. Do not bundle
  unrelated cleanup, reformat whole files for a small change, or change generated or lock files
  without the source change that requires it.
- Preserve public behavior unless a deliberate contract change is documented and tested. Never add
  optimistic provider stubs or weaken verification to obtain a passing check.
- Open a PR and stop for owner review. Do not push directly to `main`, auto-merge, force-push shared
  branches or rewrite history without explicit task authorization. Do not add an AI co-author.

Enable the local hook from the clone root:

```sh
./scripts/setup-git-hooks.sh
```

PowerShell/CMD equivalents are in `scripts/`. The hook supplements CI; it does not replace checks.

## Checks and evidence

```sh
go run ./cmd/devtool check
```

Use [toolchain guidance](docs/development/go-toolchain.md) and narrow component tests during
implementation. Full race and native macOS CI are manual checks for concurrency investigations,
platform changes or release validation, not routine PR requirements. Run
`go run ./cmd/devtool test-race` locally when that deeper validation is needed.
A PR states the behavior changed, exact checks and results, unrun checks with
reasons, credential or provider effects and remaining risks. Fake-provider/SDK fixtures are not
live evidence.

For a real installation/account run, follow the [operator checklist](docs/development/validation-checklist.md)
and use the [bug template](docs/development/bug-report-template.md) for a focused issue/PR when a
failure or support conclusion must be shared. Full run reports, credentials, binaries, private raw
logs, inputs and runtime databases stay on the operator host. A merged fix without an observed
retest remains `fix-pending-verification`.

## Review priorities

Product boundaries, duplicate-execution and recovery safety, credentials and workspace isolation,
archive/path/SSRF defenses, truthful state and capability semantics, then maintainability. Inspect
the final diff for secrets and unrelated changes as well as checking tests.

## Keep docs useful

Usage guides describe current commands. Component references explain contracts and limitations.
ADRs capture material decisions and consequences. Plans contain remaining work. Changelog entries
summarize user-visible features, not every audit/commit/test run. Remove stale completion stories;
Git/PR history already preserves them. Do not copy owner or agent messages into documentation.

Live provider facts need dated, versioned primary-source evidence; community issues are research
leads, not API contracts. Do not add account farming, quota evasion, browser/anti-idle bypasses,
hidden retries or automatic paid fallback. See [SECURITY.md](SECURITY.md) for private
vulnerability reporting.

Contributions use [Apache-2.0](LICENSE); preserve third-party notices and terms for data/models.
