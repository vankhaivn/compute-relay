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
  shared account capacity and durable per-attempt execution authorization.
- SSH host connections that run CPU jobs in isolated Docker containers on an operator-owned
  machine, with pinned host keys and requested remote cleanup.
- Reproducible macOS arm64 companion bundles.

See [current status](docs/status.md) for limits.
