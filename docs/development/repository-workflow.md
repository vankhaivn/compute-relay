# Repository workflow

## Default path

1. Read the proposal, repository instructions, relevant ADRs, and current evidence.
2. Create a focused branch from the latest `main`.
3. Make the smallest coherent change and add tests or evidence appropriate to it.
4. Use atomic Conventional Commits.
5. Open a pull request with validation results and external-side-effect disclosure.
6. Resolve review findings without rewriting unrelated history.
7. Merge using a repository-approved strategy after required checks pass.

Direct pushes to `main` are allowed only when the project owner explicitly authorizes that workflow for the current task. Such authorization does not become a permanent exception.

## Branch naming

Prefer:

```text
feat/<topic>
fix/<topic>
docs/<topic>
research/<topic>
chore/<topic>
```

Names should be short, lowercase, and hyphenated.

## Change isolation

- Do not bundle unrelated cleanup with a behavior change.
- Do not create optimistic provider stubs that report support without evidence.
- Do not modify generated, vendored, or lock files without the source change that requires it.
- Do not reformat entire files merely to touch a small section.
- Preserve existing public behavior unless the change explicitly and visibly breaks it.

## Validation disclosure

A pull request or direct-main completion report must list:

- exact checks run and their result;
- checks not run and why;
- whether provider credentials or compute were used;
- whether evidence is offline, fixture-based, documented upstream, or live; and
- any remaining unknown state or follow-up required.

## Review focus

Reviews prioritize:

1. product-boundary compliance;
2. duplicate-execution and recovery safety;
3. credential, workspace, archive, path, and SSRF security;
4. honest status/error/capability semantics;
5. provider-neutral boundaries;
6. testability and operational diagnosis; and
7. maintainability and documentation consistency.

## History safety

Do not force-push shared branches, rewrite `main`, delete tags, or remove another contributor's work without explicit owner instruction. When an upstream or provider fact changes, update the evidence record and ADR rather than rewriting historical claims without context.
