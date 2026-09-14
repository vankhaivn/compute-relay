# ADR-0005: Use a strict bundle format and rooted, explicit local imports

- **Status:** accepted
- **Date:** 2026-09-14
- **Scope:** M2-07
- **Requirements:** DAT-01, DAT-02, SEC-02
- **Related decision:** [ADR-0004](0004-workspace-auth-and-atomic-objects.md)

## Context

The proposal requires immutable `.tar.gz` bundles, explicit inclusion and exclusions,
previewable file identities, bounded archive validation, and opt-in named-root local
imports. Treating every tar variant as equivalent would introduce hidden metadata,
platform-specific path interpretation and extraction hazards before a runner exists.

The runtime must copy inputs without executing them. Local import must preserve the
existing workspace authorization and verified-byte/ownership-commit boundary, not create
an alternate admission database or expose arbitrary host paths.

## Decision

Use a versioned regular-file-only USTAR/gzip subset:

- manifest version `compute-relay/bundle/v1`;
- first member `.compute-relay/bundle.json`, followed by sorted `code/<relative-path>`
  members matching its file size, SHA-256 and executable flag;
- fixed owner/timestamps and normalized modes for reproducible output from the same
  snapshot and compressor implementation;
- ASCII portable paths, case-collision rejection on every host, no reserved/control paths,
  and limits before allocating or copying large content;
- no symlinks, hard links, devices, directories, PAX/GNU/sparse extensions, concatenated
  gzip streams or trailing archive data;
- raw header checks before the standard tar parser can consume extension records; and
- inspection without local extraction. Remote extraction remains runner work with its own
  containment and acceptance tests; inspection is not permission to call an unsafe extractor.

Packaging requires explicit included files/directories. Empty selections and `.` are
rejected. Built-in credential/VCS/cache/state exclusions cannot be re-included. The bounded
`.computeignore` grammar is exclusion-only, deliberately not advertised as full Git syntax.
A bounded heuristic credential scan supplements exclusions but is not a secret scanner.

Keep an operator-configured source directory open while reading. Linux opens every component
relative to its directory descriptor with `O_NOFOLLOW`; other target hosts use `os.Root`
and identity/link checks. Source operations reject links and special files. Preview and
copy passes compare file identity, size, modification time and digest; tree/ignore changes
also invalidate a bundle plan. This proves identity of the copied snapshot, not an atomic
snapshot of a whole filesystem or protection against a malicious host administrator.

A local import request selects a named root allowed for its authenticated workspace, not
an absolute path. `file` imports one raw file; `bundle` requires explicit included paths.
An `io.Pipe` provides bounded backpressure and propagates producer failures before successful
EOF. The existing object service performs final verification, token revalidation and
ownership commit. Complete blobs survive ambiguous metadata acknowledgements as in ADR-0004.

Local CLI creation writes a private temporary file outside the source tree, flushes it and
publishes with a no-overwrite hard link. A filesystem without this operation fails clearly;
there is no overwriting fallback. This publication link is not a link inside the archive.

## Alternatives

Accepting all tar/PAX/GNU variants improves interoperability but expands the attack surface
and makes bounded metadata/path accounting harder. Defer that expansion to an explicit
format revision and adversarial test corpus.

ZIP was considered but is unnecessary for the approved tar.gz default. Shelling out to
system tar would vary across hosts and obscure bounds and error semantics. Neither is used.

A lexical path-prefix check followed by an ordinary path open is insufficient for source
containment. Copying directly from arbitrary absolute request paths is rejected outright.

## Consequences and limits

- Bundles from arbitrary tar utilities may not conform; use the project's bundle commands.
- Unicode filenames, empty directories and advanced archive metadata are not in v1.
- Bundle paths are at most 240 ASCII bytes with components at most 100 bytes and must also
  fit USTAR's prefix/name fields. Raw local-file paths may be up to 1,024 bytes.
- Two passes add local I/O but bind published bytes to a reviewed preview.
- Rooted access does not constrain a privileged operator's mount changes or guarantee
  interruptibility of stalled filesystem I/O. Roots should be trusted local filesystems.
- The operator must exclude custom runtime state/cache directories with `Protected` paths;
  known default state names are excluded automatically.
- Production TOML/root composition and persistent ownership remain M3/M5 work. The HTTP
  import component is disabled when no importer is configured.
- No extraction, HTTPS ingestion, remote runner, provider call or GPU task is introduced.

## Verification

Tests cover deterministic create/inspect identity, explicit selections, default/custom
exclusions, secret canaries across read boundaries, traversal/link/device/collision/header
corpora, compressed/expanded limits, truncation and checksum/tail failures, mutable files,
changed directories, cancellation and consumer failure. Linux additionally exercises
concurrent symlink swaps and FIFOs. HTTP tests cover workspace/scope/root isolation, strict
JSON, metadata visibility and the existing ambiguous-commit behavior.

The finite `packagesmoke` command verifies two bundle files and a 262,144-byte raw snapshot
without running the bundled Python text. Targeted Linux race/repeat/fuzz/build checks are
separate from exact-toolchain/native CI evidence.

## Primary implementation references

- [Go traversal-resistant file access](https://go.dev/blog/osroot)
- [Go archive/tar documentation](https://pkg.go.dev/archive/tar)
- [Go tar reader source](https://go.dev/src/archive/tar/reader.go)

See [`../packaging-and-import.md`](../packaging-and-import.md) for commands, format and limits.
