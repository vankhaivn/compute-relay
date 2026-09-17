# ADR-0005: Strict bundles and rooted explicit import

Status: accepted. Requirements: DAT-01/02, SEC-02.

## Decision

Use manifest-bound regular-file USTAR/gzip bundles with normalized metadata and sorted
`code/` members. Check raw headers, paths, exact size/digest and full gzip completion before
accepting. Reject links/special files, directories, PAX/GNU/sparse extensions, collisions,
concatenated streams and trailing data. Inspection never extracts or executes locally.

Require explicit nonoverlapping includes, fixed/protected exclusions and bounded exclusion-only
`.computeignore`. Credential heuristics supplement human selection review, not universal secret
proof. Hold source roots open, use traversal-resistant no-follow/link checks and compare identity,
size/time/hash plus directory/ignore changes between preview and copy.

Local import names an operator allowlisted root and relative selection, not an arbitrary host path.
Producer errors precede successful pipe EOF; existing object publication rechecks authority.
CLI output is a new flushed file published by no-overwrite hard link outside the source root.

## Reason and consequences

Generic system tar/ZIP and lexical prefix checks widen parsing/platform/containment risks.
The stricter format trades interoperability and extra read I/O for reviewable identity. v1 omits
Unicode filenames, empty directories and advanced metadata. It is not an atomic whole-filesystem
snapshot or hostile-host sandbox. Unsupported no-overwrite linking fails, not overwrites.

See [format and commands](../packaging-and-import.md),
[Go rooted access](https://go.dev/blog/osroot) and [tar API](https://pkg.go.dev/archive/tar).
