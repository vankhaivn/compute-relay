## Change

What problem this solves and the resulting behavior. Link the issue.

## Validation

Exact commands and results; checks not run and why; remaining risks. Mark offline tests, host
checks and authorized live runs separately.

```text
./scripts/check.sh — not run
```

## Effects

API, schema or installation impact and any operator action needed. Authorized compute, quota or
deletion used (no credentials or private logs).

- [ ] Docs and examples match the new behavior.
- [ ] Design invariants in `docs/design.md` still hold.
- [ ] No credentials, private data or unauthorized provider effects.
- [ ] Title and commits follow `docs/development.md#commit-convention`.
