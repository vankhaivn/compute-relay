# Kaggle client probe environment

> **Task:** M1-01 — reproducible official-client environment and credential-free probe.
>
> **Evidence boundary:** this tool performs local package/version/help inspection only. It
> does not authenticate, call Kaggle APIs, create provider resources, or consume compute.

This directory isolates the official Kaggle client from the Go control plane and consuming
applications. It implements the boundary selected by ADR-0001: exact client/runtime pins,
no-shell subprocess execution, bounded output and deadlines, minimal environment inheritance,
and machine-readable local evidence.

## Exact baseline

| Component | Pin | Reason |
|---|---|---|
| Python | `3.11.16` | Concrete Python 3.11 runtime for the first adapter environment. |
| uv | `0.12.13` | Reproducible environment/lock tooling; enforced by `tool.uv.required-version`. |
| Kaggle CLI | `2.2.4` | Reviewed official release selected by M-0. |
| Kaggle SDK | `0.1.35` | Exact SDK version used by the selected upstream release lock. |

`uv.lock` is the authoritative cross-platform dependency resolution. The committed
`dependency-inventory.json` records the Linux reference environment's installed distribution
versions and declared license metadata; CI also generates and validates inventories on macOS
and Windows without assuming platform-specific sets are byte-identical. The upstream Kaggle
v2.2.4 lock was generated with Python 3.13, so this project resolves and validates its own
Python 3.11 environment rather than copying that file blindly.

## Supported commands

The entry point deliberately exposes only two local operations:

```text
compute-relay-kaggle-probe inventory
compute-relay-kaggle-probe dependencies
```

`inventory` runs this allowlist:

```text
kaggle --version
kaggle --help
kaggle kernels --help
kaggle datasets --help
kaggle quota --help
```

These argparse version/help paths terminate before the official CLI authentication call.
The probe also supplies an empty temporary `KAGGLE_CONFIG_DIR` and strips
`KAGGLE_API_TOKEN`, `KAGGLE_USERNAME`, and `KAGGLE_KEY` from the subprocess environment.
No arbitrary Kaggle subcommand can be passed through this interface.

`dependencies` reads local installed-wheel metadata only. It does not import or call the
provider client.

## Setup and validation

Install uv `0.12.13`, then use a repository wrapper:

```bash
./scripts/kaggle-client.sh sync
./scripts/kaggle-client.sh test
./scripts/kaggle-client.sh inventory --output /tmp/kaggle-client-inventory.json
```

```powershell
./scripts/kaggle-client.ps1 sync
./scripts/kaggle-client.ps1 test
./scripts/kaggle-client.ps1 inventory --output kaggle-client-inventory.json
```

```cmd
scripts\kaggle-client.cmd sync
scripts\kaggle-client.cmd test
scripts\kaggle-client.cmd inventory --output kaggle-client-inventory.json
```

Wrapper tasks:

| Task | Behavior |
|---|---|
| `sync` | Install the exact `uv.lock` environment using the pinned Python. |
| `lock-check` | Fail when `pyproject.toml` and `uv.lock` disagree. |
| `test` | Compile sources and run the standard-library unit test suite. |
| `inventory ...` | Run the credential-free client inventory with remaining arguments. |
| `dependencies ...` | Generate dependency/license metadata with remaining arguments. |
| `check` | Run lock validation, tests, inventory, and dependency-record comparison. |

The wrappers do not install uv globally and do not mutate another Python environment.

## Safety properties covered offline

Tests establish that the local process boundary:

- never invokes a shell;
- preserves arguments containing shell metacharacters as literal data;
- does not inherit or permit injection of Kaggle credential variables;
- uses an existing controlled working directory;
- applies a finite deadline;
- bounds stdout and stderr independently;
- terminates the process tree on timeout or output overflow;
- rejects unexpected package versions before invoking the CLI; and
- rejects a nonzero version/help command as failed evidence.

These tests establish local behavior, not Kaggle account eligibility or provider support.

## Next authorization boundary

M1-02 is a separate read-only authenticated probe. It will require an explicitly configured
credential reference and authorization, but credentials must not be pasted into chat,
committed, included in command arguments, or written to evidence fixtures. This M1-01 tool
does not accept credentials by design.
