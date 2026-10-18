# Repository instructions

For humans and coding agents; `AGENTS.md` points here. Follow the owner's current task, then these
rules. Read the topic in [docs](README.md#documentation) and the [design](docs/design.md)
invariants before changing behavior.

## Product and safety

- Self-hosted finite batch runtime: operator-owned accounts, no hosted quota pool or paid service.
  One Go control plane, local HTTP/JSON API, embedded remote Python runner.
- Public contracts are provider-neutral; provider behavior stays in its adapter.
- Keep the [design invariants](docs/design.md#invariants): intent before mutation, observe-only
  recovery, frozen bindings, no automatic resubmission or fallback, unknown stays unknown,
  all-or-nothing results, credentials only in the selected vault.
- Live compute, quota use, public resources, billing, deployment and destructive cleanup need exact
  authorization and a finite budget. CI is offline.

## Validation and documentation

- Run `./scripts/check.sh` before finishing; use focused tests while working.
- Report exact commands, results and limits; separate offline tests, host checks and live evidence.
- Never lower toolchain pins, weaken assertions or delete evidence to get a pass.
- Docs: one document per topic, current usage only, short and plain. Prefer tables and diagrams
  over prose. No development narrative or raw run evidence in Git.
- Code, comments and docs are in English.

## Git

Follow [CONTRIBUTING.md](CONTRIBUTING.md). No AI attribution in any Git text. An approved change
permits its branch, commits, normal push and PR; anything more needs explicit permission. Inspect
the final diff for secrets and unrelated changes before reporting completion.
