# Native companion distribution

A companion bundle contains the native Relay binary and an independent, locked provider
interpreter. Installing or starting it needs no source checkout, Go toolchain, uv, system
Python, package download or sibling application repository. Private Relay state and tokens
live in a separate operator-chosen directory.

The initial target is **macOS arm64**. These are unsigned local candidate artifacts, not a
published release. Native installation/readiness qualification does not establish provider
account access, GPU execution, macOS signing/notarization or support on another platform.

## Build

From a stable checkout, with Go **1.27.1**, uv **0.12.13** and a build-host Python 3.9+:

```sh
./scripts/package-companion.sh --output /absolute/new-build-directory
```

The output directory must be new and outside the checkout. The builder obtains Python
**3.11.16**, standalone build **20260901**, and installs only hashed wheels exported from
`tools/kaggle-client/uv.lock`, including Kaggle **2.2.4**, kagglesdk **0.1.35** and their
exact transitive dependencies. Dependency retrieval happens only while building. It does
not alter the system Python or install tools globally. Existing uv/Go caches may be used.

The builder copies a standalone interpreter, materializes only internal links, removes
preinstalled package managers and build-path console launchers, then installs the locked
packages into its own `site-packages`. It never ships a virtualenv or an editable package.
All archive members are regular files; the installer rejects every symbolic/hard link.
Checked-hash Python bytecode uses relative source filenames and is built ahead of time, so
normal isolated helper imports do not add unverified caches to the installed payload.

For a bounded rebuild, `--python-install` can name the matching uv standalone directory,
and `--python-license-archive` can name the pinned upstream full archive. The latter is
always checked against its fixed SHA-256. Python redistribution notices come from that
matching [upstream distribution](https://github.com/astral-sh/python-build-standalone/releases/tag/20260901).

Output includes the `.tar.gz`, its `.sha256`, a native `companionpack-darwin-arm64` installer
and its `.sha256`. Build staging and failed attempts are retained inside the output directory.
Archive metadata has stable ordering, ownership, permissions and timestamps. The manifest
records HEAD, a bounded digest of compiler inputs and the packaging recipe, exact tool/runtime
versions, and every payload file's mode, size and SHA-256. The builder refuses changing
compiler inputs during a build. Dirty builds are explicitly named `candidate` and record
`worktree_modified: true`; their HEAD alone is not the candidate's source identity. Build from
the final committed revision before qualifying a distributable artifact.

## Install and discover

Obtain the installer and expected checksums from the approved build. Checksums detect altered
bytes; these unsigned artifacts do not establish publisher authenticity by themselves.

```sh
./companionpack-darwin-arm64 install \
  --archive /absolute/companion.tar.gz \
  --sha256 EXPECTED_ARCHIVE_SHA256 \
  --dest /absolute/new-companion-directory

/absolute/new-companion-directory/bin/companionpack discover \
  --root /absolute/new-companion-directory
```

Installation verifies the archive checksum before creating the destination, then validates
the complete manifest and every member's relative path, type, size, mode and checksum.
Traversal, links, duplicates, case collisions, unexpected members and unbounded expansion
are rejected. It never merges with an existing directory, overwrites a previous installation
or touches Relay state. Failure retains partial output without a completed manifest; inspect
it and choose a new directory for a retry.

`discover` verifies installed payload bytes and returns JSON with `executable`,
`managed_python` and `manifest`. Both executable paths are absolute for the current location.
Keep the installed payload unchanged and call discovery again after relocation. It detects
corruption against the installed manifest; the installation directory remains trusted
operator-owned code, not an adversarial tamper-proof store.

The bundle includes:

| Path | Purpose |
|---|---|
| `bin/compute-relay` | Native HTTP runtime and local administration |
| `bin/companionpack` | Offline install/discovery tool |
| `client/python/bin/python3.11` | Standalone interpreter and locked site-packages |
| `manifest.json` | Build identity and complete payload checksums |
| `share/python-dependencies.json`, `share/go-dependencies.json` | Exact installed/linked dependency and declared-license inventories |
| `share/licenses`, Python `LICENSE.txt`, wheel license files | Preserved upstream notices, including Python's linked dependencies |
| `share/uv.lock`, `share/requirements.txt` | Original lock and hashed installation inputs |
| `share/source-files.json` | Relative compiler-input paths and content digests |
| `share/qualify.py` | Neutral native lifecycle check |

## Lifecycle integration

The consuming application supervises the existing foreground Relay process. First-time local
setup uses the discovered binary's ordinary `init`, workspace and token commands from
[Local runtime](local-runtime.md). Only initialize a new state directory. Upgrades select a
new verified bundle and reuse the existing state root after stopping the old process; they
never initialize or replace it as part of extraction.

Pass `executable` as the process and `managed_python` as the value of `--managed-python`:

```sh
/absolute/companion/bin/compute-relay serve \
  --root /absolute/private-relay-state --listen 127.0.0.1:0 \
  --managed-python /absolute/companion/client/python/bin/python3.11 \
  --managed-machine-shape NvidiaTeslaT4 \
  --managed-max-wall-seconds 1800 --managed-max-workers 2
```

Read the listening JSON line on stdout, then use the private read-token file for authenticated
`GET /readyz`. Read `/v1/info` to check the advertised `managed_connections` and
`attempt_authorization` features.
Policy flags grant no compute consent. The empty-state startup does not verify an account;
connections and individual execution authorizations are separate authenticated operations.
Ordinary `serve` without managed/provider flags remains admission-only.

Send SIGINT and wait for exit before reopening or performing local administration. Process
shutdown does not cancel remote jobs or prove GPU release. There is no daemon installer,
automatic updater, remote publication or render-time package installation in this surface.

## Native qualification

Run outside the source checkout, using a new scratch directory and the installed Python:

```sh
env -i PATH=/usr/bin:/bin /absolute/companion/client/python/bin/python3.11 -I -B \
  /absolute/companion/share/qualify.py \
  --bundle /absolute/companion --scratch /absolute/new-private-scratch
```

The check verifies relocation and the full installed package inventory, exercises native
Python extensions and local Kaggle version output, then runs initialization, scoped token
issuance, authenticated admission-only and managed readiness, graceful stop and state reopen.
It creates no saved provider connection, performs no account verification and authorizes no
compute. Scratch state is retained for inspection. Move the bundle to a second absolute
directory and repeat with a new scratch directory to qualify relocation independently of
the original installation path.

Source checks: `go test ./internal/companion ./cmd/companionpack`. Archive tests cover
determinism, checksum failure, create-new installation, relocation, payload modification,
traversal, links, duplicate members, special files and malformed manifests.
