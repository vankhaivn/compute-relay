## Summary

Describe the focused behavior or documentation change.

## Why

Explain the problem, evidence, or requirement that motivates this change.

## Validation

- [ ] Relevant formatting, linting, tests, schemas, or documentation checks were run.
- [ ] The exact commands and results are listed below.
- [ ] Offline/fake-provider validation is not represented as live-provider evidence.

Commands and results:

```text
not run
```

## External effects

- [ ] No provider credentials, private inputs, account exports, or sensitive logs are included.
- [ ] No provider compute or quota was consumed, or the authorized finite live test is documented.
- [ ] No paid service, public resource, deployment, or remote deletion was introduced without authorization.

## Documentation and compatibility

- [ ] Relevant public documentation and examples were updated.
- [ ] A material architectural decision has an ADR, or no ADR is required.
- [ ] Breaking behavior is marked with `!` and a `BREAKING CHANGE:` footer.

## Checklist

- [ ] The PR title follows Conventional Commits.
- [ ] Commits are focused and follow the repository convention.
- [ ] The change preserves the invariants in `AGENTS.md` and `docs/proposal.md`.
