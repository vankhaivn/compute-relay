# Security policy

Compute Relay is pre-release; fixes target `main`.

## Reporting

Do not open a public issue for a vulnerability, credential exposure or private-data incident. Use
GitHub private vulnerability reporting on the **Security** tab, or contact the owner privately via
their GitHub profile first.

Include the affected commit, component, reproduction steps, impact and prerequisites, and whether
credentials, tokens, private data or remote resources are affected. Redact tokens, account IDs,
sensitive URLs and artifact contents; never send live credentials.

High-priority areas: credential or token disclosure, cross-workspace access, path traversal or
host-file access, command injection, archive extraction, SSRF into private or metadata addresses,
artifact mix-ups across jobs, duplicate compute after ambiguous outcomes, cleanup of resources Relay
does not own, unsafe network exposure. Provider-side vulnerabilities go to the provider.

## Leaked credentials

1. Revoke or rotate at the source immediately — this matters more than rewriting Git history.
2. Keep only sanitized evidence.
3. Check bundles, provider resources, caches and exports for copies.

Reports are acknowledged when practicable; there is no response-time promise or bounty.
