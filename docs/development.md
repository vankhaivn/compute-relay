# Development

## Toolchain

| Component | Pin |
|---|---|
| Go | 1.27.1 ([go.mod](../go.mod)); SQLite via CGo-free modernc, Keychain via purego |
| Python (runner and tests only) | 3.11.16, installed through uv |
| SSH execution image | [Dockerfile](../internal/provider/sshhost/image/Dockerfile) with a digest-pinned Python 3.11 base |

Never lower a pin or weaken an assertion to make a check pass.

```sh
uv python install 3.11.16
./scripts/setup-git-hooks.sh
./scripts/check.sh
```

`check.sh` runs gofmt, `go mod tidy -diff`, `go vet`, the runner and example Python tests, and
`go test ./... -count=1` (schemas, embedded runner, Studio HTTP flow, upgrades, fault catalog). Use
focused tests while working; run the full check before finishing.

| Area | Where |
|---|---|
| HTTP routes | `internal/api/routing.go` |
| Schemas and examples | [api/](../api/README.md) — `go test ./internal/contracts` |
| Remote runner | [runner/](../runner/README.md) |
| Fault catalog | `internal/devtool/fault-matrix.json` ([design](design.md#fault-catalog)) |

## CI

[ci.yml](../.github/workflows/ci.yml) runs the same offline check on Linux;
[commit-convention.yml](../.github/workflows/commit-convention.yml) checks commit subjects and PR
titles. Race tests and native macOS jobs run only on manual dispatch, which needs the owner's
permission. CI never holds credentials or touches providers.

## Live validation

A live run needs authorization for the exact account or host, a finite compute/wall budget and the
cleanup it will do. Start with local and read-only checks, run CPU before GPU, inspect each result
before the next step, and stop if a host, token or permission blocks you. Keep raw records private;
share only sanitized outcomes with the binary revision. Fake-provider passes are not live evidence.

## Commit convention

[Conventional Commits](https://www.conventionalcommits.org/), enforced by
`scripts/check-commit-subject.sh` in the local hook and CI.

```text
<type>(<scope>)!: <summary>
```

- Types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `style`, `revert`.
- Scope is optional; use a real boundary (`api`, `store`, `kaggle`, `ssh`, `runner`, `docs`, …),
  never an issue number or person.
- Summary: lowercase start, imperative, no trailing period; whole subject ≤ 80 characters. PR titles
  follow the same format.
- `!` marks a breaking change and needs a `BREAKING CHANGE:` footer.
- Body (wrapped near 100 columns) explains what and why when not obvious. Trailers: `Refs: #123`,
  `Fixes: #123`.
- One commit is one coherent change with its direct tests. No AI author or co-author lines.

```text
fix(store): retain active attempt identity after restart
feat(api)!: require idempotency keys for job creation
```

`./scripts/test-commit-subject.sh` runs the validator's own tests.
