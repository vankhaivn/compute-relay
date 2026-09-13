# Security policy

## Supported versions

Compute Relay has no released runtime version yet. Security fixes currently target the default branch. A version support table will be added before the first release.

## Reporting a vulnerability

Do not open a public issue for a vulnerability, credential exposure, or private-data incident.

Use GitHub's private vulnerability reporting entry on the repository **Security** tab when it is available. If that channel is unavailable, contact the repository owner privately through the contact information on their GitHub profile before sending sensitive details. Share only the minimum information needed to establish a private channel.

A useful report includes:

- affected commit or version;
- component and deployment context;
- reproduction steps or a minimal proof of concept;
- realistic impact and prerequisites;
- whether provider credentials, workspace tokens, private data, or remote resources may be affected; and
- any safe mitigation already identified.

Do not include live credentials or third-party private data. Redact tokens, account identifiers, URLs with sensitive query strings, and artifact contents.

## Scope priorities

High-priority areas include:

- provider credential or runtime-token disclosure;
- cross-workspace authorization bypass;
- arbitrary host-file access or path traversal;
- command injection on the runtime host;
- archive extraction and URL-ingestion vulnerabilities;
- SSRF into local, private, link-local, or metadata endpoints;
- artifact identity confusion across jobs or attempts;
- unsafe duplicate compute submission after ambiguous outcomes;
- destructive cleanup of resources not owned by the connector; and
- unsafe default network exposure.

Provider service vulnerabilities should be reported to the provider. Reports about this repository should explain how project code or documentation creates the risk.

## Credential incidents

If a credential appears in a commit, issue, pull request, log, or artifact:

1. revoke or rotate it at the source immediately;
2. treat removal from Git history as secondary to revocation;
3. preserve only sanitized evidence needed for investigation; and
4. review generated bundles, provider resources, caches, and diagnostic exports for copies.

The project will not ask users to paste provider secrets into chat or public issue threads.

## Disclosure

Maintainers will acknowledge a valid private report when practicable, investigate impact, coordinate a fix, and credit reporters who request recognition. No response-time or bounty promise is made at this pre-release stage.
