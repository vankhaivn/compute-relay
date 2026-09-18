# Changelog

The project has not published a release. This section summarizes the current pre-release
feature set, not individual commits, audits or CI runs.

## Unreleased

- Local HTTP runtime and CLI with private installations, workspace access, expiring tokens
  and immutable admission profiles.
- Immutable input uploads, safe code bundles and provider-neutral job contracts.
- SQLite-backed jobs, attempts, idempotent receipts, recovery and explicit job controls.
- Verified artifact collection, authorized downloads, retention rules and local byte cleanup
  components.
- Kaggle preflight, private staging, one-shot execution, quota/log snapshots and versioned
  result retrieval components, plus an explicitly authorized GPU acceptance experiment.

**Not yet a complete production compute service:** the normal server is admission-only;
general provider/worker integration and live acceptance remain unfinished. See
[current status](docs/status.md) and the [remaining plan](docs/development/implementation-plan.md).
