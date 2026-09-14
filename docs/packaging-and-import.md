# Safe bundles and allowlisted local import

> **Task:** M2-07, implemented offline; PR #8 in review.
>
> **Boundary:** local bundle commands and an opt-in HTTP import component. No provider
> credential, archive extraction or workload execution. Production `serve`, SQLite ownership
> and TOML root configuration remain separate M3/M5 work.

## Bundle commands

Build the pre-release CLI with the repository's pinned toolchain:

```text
go build -o compute-relay ./cmd/compute-relay
```

Use `compute-relay.exe` on Windows. Commands accept the same flags in POSIX, PowerShell and
CMD. The paths below assume a caller-created `job` directory containing `main.py` and `src`:

```text
compute-relay bundle preview --root ./job --include main.py --include src
compute-relay bundle create --root ./job --include main.py --include src --output ./job.tar.gz
compute-relay bundle inspect --file ./job.tar.gz
```

`preview` prints JSON with selected relative paths, file lengths/digests, executable flags,
expanded bytes, exclusions and `manifest_sha256`. To require the reviewed snapshot, pass
that exact digest to `create` with `--expect-manifest-sha256 DIGEST`. A changed preview or
source fails; the command never executes `main.py` or installs its requirements.

Explicit included directories are traversed within configured limits. No includes, `.` or
an overlapping/duplicate selection is an error. Supplying a project/home directory does
not automatically authorize packaging all of it. Output must be outside the project root,
with an existing parent and a new destination name. Existing files are never overwritten.
Creation publishes a flushed temporary file with an atomic no-overwrite link, then removes
the temporary name; filesystems without that operation fail instead of weakening semantics.

`inspect` verifies an existing regular archive file, not stdin or a special device. All three
commands have the CLI's finite two-minute operation context. Context cancellation is checked
between reads; it cannot force an unresponsive host filesystem to complete a system call.

## Bundle v1

```text
.compute-relay/bundle.json
code/main.py
code/src/helper.py
```

The JSON manifest contains `bundle_version: compute-relay/bundle/v1` and a sorted nonempty
`files` array. Each entry requires `path`, `bytes`, `sha256` and `executable`. File paths are
relative to `code/`, not host paths. Empty files are supported; empty directories are omitted.
The manifest hash identifies its exact JSON bytes; the bundle hash identifies the compressed
archive bytes. Neither is a cryptographic attestation against the host owner.

The format is deliberately strict: one gzip stream, regular-file USTAR members, normalized
0644/0755 modes, zero owners, and fixed creation metadata. Repeated creation from the same
snapshot is deterministic with the same implementation. Cross-version compressor byte
identity is not promised. A tar.gz made by another utility may be rejected even if a generic
tar reader accepts it.

Inspection checks every raw header before parsing it, matches every file against the
manifest and verifies payload digests and the gzip checksum. It rejects PAX/GNU/sparse
metadata, links, devices, FIFOs, directory entries, duplicates, file/directory prefix
conflicts, case collisions, extra members, concatenated gzip streams and trailing data.
No file is extracted by the local control plane. The future runner must still enforce
containment during extraction; this validator is not a substitute for that boundary.

## Paths, exclusions and credential checks

Portable v1 paths use printable ASCII, `/` separators, at most 240 bytes per bundle path and
100 bytes per component, subject also to USTAR encoding. Absolute/drive/UNC paths, `..`,
`.` components, repeated separators, backslashes, control bytes, reserved Windows names,
trailing dots/spaces and names conflicting only by case are rejected on every host.
Raw file imports permit paths up to 1,024 bytes using the same component rules.

Default exclusions include VCS folders, `.env*`, common provider/key/config files, private
key extensions, dependency/editor caches, build outputs and known runtime state directories.
Operator-defined `Protected` relative prefixes exclude custom state/cache directories too.
Defaults and protected paths cannot be re-included.

A root-level `.computeignore` adds exclusions; see [`../.computeignore.example`](../.computeignore.example).
The implemented grammar is intentionally smaller than Git's ignore syntax:

| Pattern | Meaning |
|---|---|
| `*.log` | Match a component at any depth. |
| `generated/` | Exclude that named component and descendants. |
| `assets/**/draft?.txt` | Root-relative pattern; `**` matches whole components at any depth. |
| `# comment` | Comment line; blank lines are ignored. |

Patterns with `!`, leading `/`, backslashes, `.`/`..` components or embedded partial `**`
are rejected. There are no reinclusion or escape rules. Bounds are 64 KiB per ignore file,
256 rules, 1,024 bytes per rule and 64 components per rule. Exclusion matching never expands
filesystem globs before entering the rooted reader.

Streaming heuristic checks look for obvious private-key blocks and credential patterns,
including across read-buffer boundaries. A detection fails with no matched value in the
error. This is not comprehensive secret detection: review the preview and included source
before upload. Exclusions cannot prove arbitrary application data contains no secrets.

## Local import component

The endpoint is:

```text
POST /v1/workspaces/{workspace_id}/objects/import
Content-Type: application/json
Authorization: Bearer <workspace runtime token>
```

It is disabled by default. Operator code configures `localinput.RootSpec` with a name,
absolute host directory, allowed workspace IDs and protected relative paths, then composes
`objects.NewImporter` with the existing object service and supplies `api.Config.LocalImports`.
Empty roots or a nil importer grant no access. These constructors exist now; no new TOML
key or production `serve` configuration is claimed before its implementation task.

The application sends only a configured root name and relative selection:

```json
{"root":"source","kind":"file","path":"inputs/prompts.jsonl"}
```

For a directory/code bundle, includes must be explicit:

```json
{"root":"source","kind":"bundle","includes":["main.py","src"]}
```

Both forms optionally accept `sha256`, the expected digest of the final raw file or bundle.
Requests cannot mix a nonempty file path with bundle includes. Unknown/duplicate keys,
null fields and trailing JSON are rejected. The JSON-body, deadline, workspace rate and
concurrent-upload bounds from [auth-and-objects](auth-and-objects.md) also apply to imports.
The upload semaphore is acquired before source discovery/hash/copy work starts.

Authentication and workspace write permission precede source access; each named root has
its own workspace allowlist. Source errors and host paths are not exposed verbatim. A
successful 201 receipt has the same object ID/length/digest shape and metadata Location as
binary upload. File edits after import never update the immutable object.

## Snapshot and publication semantics

The configured root remains open. Linux resolves every component with directory-relative
no-follow opens, rejecting special files and hard links. macOS/Windows use traversal-resistant
`os.Root` with identity/link checks; Windows also checks reparse points on opened inputs.

The first pass computes a bounded file/digest preview. The copy pass reopens through the
held root and verifies identity, size, timestamps and the same digest. Bundles also check
directory and ignore-file changes. A mismatch returns `INPUT_CHANGED` rather than silently
publishing replacement bytes. This is not a globally atomic directory snapshot or a
security boundary against the trusted administrator, mount changes or adversarial same-user
filesystem manipulation.

The importer streams through a pipe into the existing verified upload path. Producer errors
arrive before successful EOF. Only verified bytes can be published, and only a subsequent
ownership commit permits a successful response. Token authority is revalidated before
commit. Uncertain metadata acknowledgements preserve complete blobs for recovery, never
speculatively delete them. Tests use an explicitly nondurable metadata fixture; durable
SQLite admission is still M3 work.

## Bounds and evidence

| Bundle bound | Default |
|---|---|
| Compressed archive | 100 MiB |
| Expanded file payloads | 500 MiB |
| Manifest | 4 MiB |
| Files / visited source entries | 10,000 / 100,000 |
| Depth / copy buffer | 32 / 64 KiB |

Expanded framing overhead is separately bounded. Raw imports use an operator-selected
maximum up to 2 GiB. File counts, path lengths and observed byte counts remain bounded even
when declarations are absent or malformed. These are local policy values, not Kaggle limits.

Run the finite synthetic smoke:

```text
go run ./cmd/packagesmoke
```

It creates two bundle files, verifies create/inspect identity, streams a 262,144-byte raw
input and rejects another workspace's root access. It exits and removes temporary files.
It does not authenticate to a provider, start an HTTP runtime or execute the bundled Python.
Separate HTTP component tests exercise import through a real loopback listener and blob store.

Targeted Linux verification used Go 1.23.2 with an external temporary module file because
network/toolchain downloads were unavailable. The committed module remains Go 1.27.1.
Targeted vet, race tests, 25 repeated runs, a CGo-free smoke build and bounded archive fuzzing
passed. Whole-repository contracts and native platform evidence come from the unchanged
offline CI; do not describe targeted local checks as a full production-runtime test.

See [ADR-0005](decisions/0005-bundle-format-and-rooted-import.md). HTTPS ingestion (M2-08),
runner extraction/execution (M2-09) and production runtime composition are not part of this PR.
