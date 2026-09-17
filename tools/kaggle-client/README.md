# Locked Kaggle client environment

This directory installs the official client/SDK in a dedicated virtual environment and
provides credential-free package/help inventory. Provider components use the same locked
interpreter; authenticated operations have separate, explicit authorization boundaries.

## Install and check

From the repository root, install uv **0.12.13**, then use the wrapper for your shell:

```bash
./scripts/kaggle-client.sh sync
./scripts/kaggle-client.sh check
```

```powershell
./scripts/kaggle-client.ps1 sync
./scripts/kaggle-client.ps1 check
```

```cmd
scripts\kaggle-client.cmd sync
scripts\kaggle-client.cmd check
```

The wrappers do not install uv globally or alter another Python environment. Sync downloads
locked packages/Python as needed; the checks do not authenticate Kaggle or allocate compute.

| Component | Pin |
|---|---|
| Python | 3.11.16 |
| uv | 0.12.13 |
| Kaggle CLI | 2.2.4 |
| Kaggle SDK | 0.1.35 |

`pyproject.toml`, `.python-version` and `uv.lock` own these values. The lock resolves this
project's Python environment, not an assumed copy of upstream's development environment.
`dependency-inventory.json` records reference distribution/license metadata; platform-specific
installed sets are checked without assuming they are identical. Preserve upstream notices.

## Wrapper tasks

| Task | Behavior |
|---|---|
| `sync` | Install the locked environment with the pinned Python. |
| `lock-check` | Reject a `pyproject.toml`/lock mismatch. |
| `test` | Compile sources and run the offline unit suite, including provider-helper SDK fixtures. |
| `inventory ...` | Run local version/help inventory; supports an explicit output file. |
| `dependencies ...` | Read installed distribution/license metadata. |
| `check` | Validate lock, tests, inventory and dependency record. |

The `compute-relay-kaggle-probe` executable exposes only `inventory` and `dependencies`.
Inventory permits `kaggle --version`, top-level help, kernels/datasets/quota help. Those paths
exit before authentication; the probe strips credential variables and uses empty temporary
configuration. It is not an arbitrary Kaggle-command proxy. Dependencies reads installed
metadata without importing/authenticating the provider client.

## Authenticated use is separate

Use the absolute `.venv/bin/python` path on POSIX or `.venv/Scripts/python.exe` on Windows
in [preflight configuration](../../docs/providers/kaggle-preflight.md). Local preflight checks
pins without reading tokens. Authenticated account reads require explicit opt-in; private
staging and GPU execution require their own authorization. Never pass a token as a flag,
commit credential values, or use a CI credential for operator acceptance.

The SDK tests mock HTTP, so passing them is not account eligibility, provider compatibility
or a live CUDA result. Follow the [operator validation checklist](../../docs/development/validation-checklist.md)
and record real outcomes in its results ledger. See [ADR-0001](../../docs/decisions/0001-official-kaggle-client-boundary.md)
for the client isolation boundary and [THIRD_PARTY.md](THIRD_PARTY.md) for dependency notices.
