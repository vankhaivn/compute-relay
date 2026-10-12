# Changelog

No release has been published. This summarizes the current pre-release feature set.

## Unreleased

- Local HTTP runtime and CLI with private installations, workspace tokens and immutable
  admission profiles.
- Immutable uploads, safe code bundles and provider-neutral job contracts.
- Durable jobs, attempts, idempotent receipts, recovery and explicit job controls.
- Verified artifact collection and downloads, with optional manual collection and byte progress.
- Automatic local deletion of expired input and result bytes while provider workers run.
- Kaggle provider adapter behind an explicit, finitely authorized `serve` mode.
- Managed connections with write-only credentials in a private file or optional macOS Keychain,
  configurable batch concurrency, shared account/machine capacity, effective limits and queue
  reasons, and durable per-attempt execution authorization.
- SSH host connections that run CPU jobs in isolated Docker containers on an operator-owned
  machine, with visible pinned host keys, verified architecture, explicit image installation,
  configurable host limits, Python-free SSH-only serving on macOS and requested remote cleanup.
- Optional immutable absolute execution deadlines with remote runner enforcement and SSH engine
  clock-skew checks on advertised-capability hosts; see [execution deadline limits](docs/execution-deadlines.md).
- Optional SSH preparation and start gates with separately supplied ephemeral workload secrets;
  see [preparation gates](docs/preparation-gates.md) and [workload secrets](docs/workload-secrets.md).
- Reproducible macOS arm64 companion bundles.

See [current status](docs/status.md) for limits.
