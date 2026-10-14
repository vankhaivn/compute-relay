## Change

Describe the concrete problem and resulting behavior. Link the issue when applicable.

## Validation

List exact commands/results, unrun checks with reasons and remaining risks.
Distinguish offline tests, host checks and authorized live-provider evidence.

```text
./scripts/check.sh — not run
```

## Compatibility and effects

Describe public/API/schema/installation impact and any required operator actions.
Summarize authorized finite compute/quota/deletion effects without credentials or private logs.

- [ ] Relevant docs and examples match the resulting behavior.
- [ ] Safety invariants in `CLAUDE.md` and `docs/design.md` are preserved.
- [ ] No credentials, private inputs, account exports or sensitive logs are included.
- [ ] No unauthorized provider mutation, paid capacity or deployment is introduced.
- [ ] PR title and focused commits follow `docs/development.md#commit-convention`.
- [ ] Breaking behavior has `!` and a `BREAKING CHANGE:` footer where required.
