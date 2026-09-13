# Third-party dependency record

> **Scope:** M1-01 Kaggle client/probe environment.
>
> **Status:** exact versions are in `uv.lock`; installed metadata is captured in
> `dependency-inventory.json`.

## Direct runtime dependencies

| Package/tool | Pin | Declared license/source | Purpose |
|---|---|---|---|
| `kaggle` | `2.2.4` | Apache-2.0; official `Kaggle/kaggle-cli` release | Supported provider CLI/client surface. |
| `kagglesdk` | `0.1.35` | Apache Software License in published package metadata | Structured official client dependency used by Kaggle CLI. |
| Python | `3.11.16` | Python Software Foundation License | Isolated interpreter for provider tooling. |
| uv / `uv_build` | `0.12.13` | Apache-2.0 OR MIT | Lock, sync, managed Python, and project build backend. |
| `astral-sh/setup-uv` | commit `bec219d24cd3e171d82865faccec33120bb574f4` (`v10.1.0`) | MIT | CI installation of exact uv/Python tooling. |
| `actions/checkout` | commit `3d3c42e5aac5ba805825da76410c181273ba90b1` (`v7.0.1`) | MIT | CI source checkout. |

## Transitive record

`dependency-inventory.json` is generated only after `uv sync --locked`. It records, in
stable name order:

- exact installed version;
- `License-Expression` when supplied;
- a bounded normalized `License` field plus byte count and SHA-256 of its full metadata
  value;
- license classifiers; and
- package-publisher project URLs.

Publisher metadata can be incomplete or inconsistent. This inventory is an engineering
review aid, not legal advice and not a substitute for preserving and reviewing the license
files shipped in distributions. Release hardening will produce the final SBOM/notices from
the release artifact set.

## Update rule

Changing Python, uv, Kaggle CLI, Kaggle SDK, or any resolved dependency requires:

1. source/changelog and license review;
2. regenerated `uv.lock` and `dependency-inventory.json`;
3. passing process-safety and client-inventory tests on Linux, macOS, and Windows;
4. updated M-0 compatibility/evidence documents when a capability assumption changes; and
5. separate authorized live re-verification for affected provider claims.
