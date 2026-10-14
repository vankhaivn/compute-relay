# Development

Build/check with Go 1.27.1 from [go.mod](../go.mod). Use Python 3.11.16 only for development and
remote-runner code/tests; Relay runtime has no local interpreter dependency. Install the pinned
interpreter into uv's managed cache before running checks; checks never download it implicitly.
Do not lower pins or replace drivers to make a check pass.

```sh
uv python install 3.11.16
./scripts/check.sh
```

The single check command runs gofmt verification, `go mod tidy -diff`, `go vet ./...`, both
Python test trees and `go test ./... -count=1`. Go tests validate generated schemas, embedded
runner assets, the Studio-shaped HTTP flow, sequential upgrades and the
[self-contained fault catalog](../internal/devtool/fault-matrix.json). Catalog tests reject missing
or invalid test references; the full Go suite executes the referenced cases. [Design](design.md#fault-cases) explains the scenarios.
Use narrow package/tests while implementing; run the complete command before completion.

CI uses [.github/workflows/ci.yml](../.github/workflows/ci.yml) for the same offline gate and
[commit-convention.yml](../.github/workflows/commit-convention.yml) for subjects/PR titles.
Race (`go test -race ./...`) and native macOS jobs are optional manual checks for concurrency or
platform changes. Never put account credentials or live provider effects in CI. Workflow dispatch
requires explicit owner authorization.

## Operator validation

A live run needs authorization for the exact account/host, finite compute/wall budget and resource
mutations, including cleanup. Begin with local checks and read-only identity/capability checks;
inspect each result before the next effect. Use private attempt-owned resources and retain original
receipts, exact binary and uncertain state. Do not widen scope when a host/token/permission blocks.
A Keychain prompt requires owner interaction; do not bypass ACLs or recreate existing entries.

Validate CPU end to end before separately authorized GPU work. Verify device evidence, logs,
artifact bytes, restart observation and cleanup independently. SSH qualification additionally
covers prepared-but-not-started containers, workload-secret delivery/deadline, cancellation and
shared capacity across connection paths. A skipped check or fake-provider pass is not live evidence.
Keep raw records privately; share only minimal sanitized outcomes with exact revision and limits.

## Commit convention

This repository uses Conventional Commits for every human and automated commit.

### Subject format

```text
<type>(<scope>)!: <summary>
```

- `type` is required.
- `scope` is optional but preferred when it clarifies the affected boundary.
- `!` marks an intentional breaking change and requires a `BREAKING CHANGE:` footer.
- `summary` starts with a lowercase letter or digit, uses the imperative mood and has no
  trailing period.
- The full subject, including type, scope and punctuation, is at most 80 characters. This applies
  to commit subjects and pull-request titles.

### Allowed types

| Type | Use |
|---|---|
| `feat` | New externally observable behavior. |
| `fix` | Correction of incorrect behavior. |
| `docs` | Documentation-only change. |
| `refactor` | Internal restructuring without intended behavior change. |
| `perf` | Measured performance improvement. |
| `test` | Test-only or test-infrastructure change. |
| `build` | Build, packaging, or dependency mechanics. |
| `ci` | Continuous-integration configuration. |
| `chore` | Focused maintenance not covered above. |
| `style` | Formatting or whitespace only, without behavior changes. |
| `revert` | Reversal of a previous change. |

### Suggested scopes

Use the smallest stable boundary, for example:

```text
repo docs api domain scheduler reconcile provider kaggle fake
store sqlite objects packaging transfer artifacts runner cli config
security tests release deps
```

New scopes are allowed when they describe a real boundary. Avoid issue numbers, personal names or
temporary branch names as scopes.

### Body and trailers

Use a body when the reason, trade-off, failure semantics or migration impact is not obvious.
Explain **what** and **why**, not a line-by-line diff. Wrap prose near 100 characters.

Useful trailers include:

```text
Refs: #123
Fixes: #123
BREAKING CHANGE: describe the incompatible behavior and migration path
```

Do not add an AI model, coding agent, or assistant as an author or co-author.

### Examples

```text
docs(repo): bootstrap open-source project
feat(domain): model ambiguous submission outcomes
fix(store): retain active attempt identity after restart
test(provider): cover adapter without remote cancellation
style(api): normalize whitespace
ci(repo): validate conventional commit subjects
```

Breaking example:

```text
feat(api)!: require idempotency keys for job creation

Reject compute-creating requests that omit a stable idempotency key so a lost
HTTP response cannot create duplicate local admission.

BREAKING CHANGE: clients must send Idempotency-Key on job creation and retry.
```

### Atomic history

One commit is one coherent, reviewable change that leaves the repository valid. Separate unrelated
formatting, dependency upgrades, generated output and behavior changes, but keep a fix and its
direct regression tests together rather than splitting them into micro-commits.

Platform merge commits and Git-generated `Revert "..."` subjects are accepted by the validator.
Pull-request titles follow the normal format because they may become squash-merge subjects.

### Enforcement

- `.githooks/commit-msg` validates local subjects after running a setup script.
- `.github/workflows/commit-convention.yml` validates PR titles and committed subjects.
- `scripts/check-commit-subject.sh` is the single validator used by the hook and CI.
- `scripts/test-commit-subject.sh` holds the validator's regression checks; CI runs it before
  validating the PR title and commit range.

Run the same regression checks locally with:

```sh
./scripts/test-commit-subject.sh
```

Local hooks improve feedback but are not a security boundary; repository CI remains authoritative.
