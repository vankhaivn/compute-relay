# Safe bundles and local import

Package explicitly selected source without running it. The local control plane inspects bundles
but does not extract or execute uploaded code; the remote runner enforces its own containment.

## Bundle commands

Given a source directory containing `main.py` and optionally `src`:

```sh
./compute-relay bundle preview --root ./job --include main.py --include src
./compute-relay bundle create --root ./job --include main.py --include src --output ./job.tar.gz
./compute-relay bundle inspect --file ./job.tar.gz
```

Use only includes that exist. Selection must be nonempty and nonoverlapping; `.` is not a blanket
include. Output must be new, outside the source root, with an existing parent. Creation uses
flushed temporary bytes and a no-overwrite link; unsupported filesystems fail rather than
replace existing files. All operations have finite contexts.

Preview reports selected paths, lengths, hashes, executable flags and `manifest_sha256`.
Pass that digest as `--expect-manifest-sha256 DIGEST` to create only the reviewed snapshot.
Source changes fail rather than silently packaging replacement bytes. Inspect requires a regular
archive file. No command installs requirements or executes `main.py`.

## Format and paths

Bundle v1 is one gzip stream of regular-file USTAR members:

```text
.compute-relay/bundle.json
code/main.py
code/src/helper.py
```

The manifest version is `compute-relay/bundle/v1`; each sorted file records relative path,
bytes, SHA-256 and executable flag. Empty files are supported, empty directories omitted.
Manifest and compressed-bundle hashes are different identities. Determinism applies to the
same implementation/snapshot, not all future compressors.

Reject links, devices/FIFOs/directories, PAX/GNU/sparse extensions, duplicate/extra members,
case/prefix collisions, bad headers/digests/checksums, concatenated gzip and trailing data.
Portable paths use ASCII `/` components, never absolute/drive/UNC paths, `.`/`..`, backslashes,
reserved Windows names or trailing dots/spaces. Bundle paths are bounded to 240 bytes and
components to 100 bytes, subject to USTAR representation.

## Exclusions and snapshot safety

Default/protected exclusions cover known secret/config/state/VCS/cache/build paths and cannot
be re-included. [`.computeignore`](../.computeignore.example) adds exclusion-only component/root
patterns with whole-component `**`. Negation, absolute paths, escapes and parent traversal are
not supported. Bounds are 64 KiB, 256 rules and 1,024 bytes/64 components per rule.

A rooted two-pass preview/copy checks identity, length, timestamps, digests and directory/ignore
changes. Source changes fail. Obvious credential-pattern checks are heuristics, not proof that
arbitrary data is secret-free; review selections before upload. The trusted operator/mount
boundary is not a globally atomic directory snapshot or hostile-user sandbox.

## Optional local import

`POST /v1/workspaces/{w}/objects/import` is disabled in the normal server. Explicit composition
uses named absolute `localinput.RootSpec` roots, workspace allowlists/protected paths,
`objects.NewImporter` and `api.Config.LocalImports`. Applications supply a root name and relative
file path or explicit bundle includes, never an unrestricted host directory.

```json
{"root":"source","kind":"file","path":"inputs/prompts.jsonl"}
```

```json
{"root":"source","kind":"bundle","includes":["main.py","src"]}
```

An optional `sha256` binds the final raw file/bundle. Authority and transfer admission precede
source discovery. Rooted no-follow/link/reparse checks and stream completion protect the input
boundary; verified bytes commit through [object storage](auth-and-objects.md). Later edits never
update an existing object. The optional component is not a shipped TOML/serve enablement path.
