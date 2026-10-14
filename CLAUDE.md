# Repository instructions

These instructions apply to humans and coding agents; `AGENTS.md` points here.
Follow the owner's current task, then these rules. Read the relevant topic in
[docs](docs/README.md) and [design](docs/design.md) before changing its behavior.

## Product and safety

- Self-hosted finite batch runtime; operator-owned accounts, no hosted quota pool or mandatory
  paid service. One Go control plane, local HTTP/JSON API, embedded remote Python runner.
- Public/domain contracts are provider-neutral. Keep provider behavior in its adapter.
  Workspaces belong to one trusted operator; they are not hostile SaaS tenants.
- Preserve jobs, attempts, original receipts, mutation intents and exact frozen provider bindings.
  Commit one-shot intent before mutation; recover ambiguity by observation only.
- Never automatically resubmit, switch provider/account, fall back to CPU or enable paid capacity.
  Read-only recovery does not grant new mutation authority.
- Unknown stays unknown. Cancellation, timeout, payload completion, result availability, cleanup
  and hardware release are separate facts. Preserve fencing, shared capacity and tombstones.
- Publish only verified complete artifact sets; retain uncertain state and original recovery material.
- Keep credentials out of jobs, logs, fixtures, SQLite and Git. Use the selected protected vault;
  never fall back to a different backend or recreate inaccessible credentials.
- Live compute, quota use, public resources, billing, deployment and destructive cleanup need
  exact authorization and a finite budget. CI is offline.

## Validation and documentation

Run `./scripts/check.sh` before completion; use focused tests during implementation.
Report exact commands, results and limits. Distinguish offline tests, host checks and live evidence.
Never lower toolchain pins, weaken assertions or delete evidence to obtain a pass.
Write current usage and constraints in one document per topic. Keep only non-obvious design
reasons; remove completed development narrative. Keep private raw run evidence outside Git.
Code, comments and documentation follow repository English conventions.

## Git

Follow [CONTRIBUTING.md](CONTRIBUTING.md) and [commit convention](docs/development.md#commit-convention).
Use repository branch prefixes (`feat/`, `fix/`, `docs/`, `test/`, `refactor/`, `chore/`).
One commit contains one coherent change and its direct tests. No AI attribution in any Git text.
No direct main push, merge, rebase, force push, tag, deploy or CI/CD trigger without explicit
permission for that action. An approved change permits its branch, commits, normal push and PR.
Inspect the final diff for secrets and unrelated changes before reporting completion.
