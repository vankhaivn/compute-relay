# Changelog

The project has not published a release. This section summarizes the current pre-release
feature set, not individual commits, audits or CI runs.

## Unreleased

- Local HTTP runtime and CLI with private installations, workspace access, expiring tokens
  and immutable admission profiles.
- Immutable input uploads, safe code bundles and provider-neutral job contracts.
- SQLite-backed jobs, attempts, idempotent receipts, recovery and explicit job controls.
- Verified artifact collection, authorized downloads, retention rules and local byte cleanup.
- Kaggle preflight, private staging, one-shot execution, quota/log snapshots and selected
  artifact transfer, plus a fixed live-qualified GPU acceptance experiment.
- Explicit Kaggle-enabled normal `serve` composition with exact frozen binding verification,
  a finite provider-attempt authorization budget, durable dispatch/reconciliation and verified
  artifact collection; admission-only mode remains the default.
- Optional managed connection API with write-only protected credentials, asynchronous account
  discovery, shared account capacity and durable finite per-attempt execution authorization.
  Connection changes preserve existing immutable job bindings and recovery.

**Pre-release:** public provider log/cleanup surfaces, strict runtime configuration, doctor/client
examples, installation packaging and full release hardening remain. The integrated normal-server
path requires scoped operator re-qualification before new live support claims. See
[current status](docs/status.md).
