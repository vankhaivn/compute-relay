# ADR-0029: File credentials for self-hosted installations

Status: accepted (2026-10-12). Refines ADR-0025 and ADR-0028. Requirements: SEC-01, MGT-02.

## Decision

Use an installation-scoped private file as the default managed credential store for new
installations. Keep macOS Keychain available through `--managed-credential-store keychain`.
Record the first selection durably; older installations with an initialized request-fingerprint
key retain Keychain. Refuse a conflicting selection or incomplete state instead of switching stores.

The existing vault port owns immutable credential generations and the request-fingerprint key
in either backend. SQLite retains only references and keyed fingerprints. File values are binary-safe
base64 in `credentials/secrets.env`, not encrypted and never sourced into a shell or environment.
Only private filesystem permissions and the trusted operator boundary protect the file.

Use the installation process lock, serialized updates, a private temporary file and atomic
replacement. Preserve exact replay after a lost acknowledgement and preserve the original file
after a failed publication. Secret bytes never enter jobs, SQLite, arguments, API reads or logs.
Missing initialized keys and corrupt files are failures, not permission to create a new identity.

## Reason and alternatives

Self-hosted operators need restart and binary replacement without editing macOS access lists.
File storage also removes the operating-system restriction on managed credentials. Keychain
remains useful when operators want its encryption and application access controls. Stable code
signing can improve Keychain upgrades but is not a prerequisite for local builds and headless use.

Reading credentials only from inherited environment variables would not provide the existing
write-only administration, immutable generation and request replay semantics. Automatically
falling back from Keychain would hide an access failure and split an installation's identity.

## Consequences

- The file and its backups contain recoverable secrets. Same-user processes remain trusted;
  this is not encrypted storage or a boundary against the host administrator.
- Binary names, paths and signing identities do not affect file access. Reuse the same runtime
  root after stopping the previous process.
- Preserve the credential directory and backend marker with coordinated installation backups.
  Atomic replacement does not make the file and SQLite one transaction; existing recovery applies.
- Unix directory sync supplements file sync; network filesystems and arbitrary power-loss recovery
  are not qualified.
- No implicit export, migration or fallback between file and Keychain is introduced. The
  [operator guide](../../managed-connections.md#choose-credential-storage) owns setup and recovery.
