# Kaggle client configuration and account check

The explicit serve mode reads this configuration through `--provider-config` and performs the
read-only account check below before it starts workers. A passing check is not a dispatch permit.

## Locked client environment

Use [tools/kaggle-client](../../../tools/kaggle-client/README.md). Pins are Python 3.11.16,
uv 0.12.13, Kaggle 2.2.4 and SDK 0.1.35 in that directory's manifests/lock. The preflight accepts
the Python 3.11 series and exact client/SDK distribution versions. Use the absolute interpreter
inside the locked environment, not a PATH lookup or shell wrapper. Version strings are not
cryptographic installation integrity; the interpreter/packages remain operator-controlled code.

## Private non-secret configuration

All five fields are required. Adapt paths/account; keep this regular JSON file outside Git:

```json
{
  "instance_id": "kaggle-personal",
  "revision": "preflight-1",
  "account_name": "your_canonical_username",
  "credential_ref": "env:CR_KAGGLE_TOKEN",
  "python_executable": "/absolute/compute-relay/tools/kaggle-client/.venv/bin/python"
}
```

Configuration is bounded to 8 KiB, closed against unknown/duplicate/case-alias/null fields and
immutable after construction. The account parser is a conservative lowercase ASCII subset, not
every possible upstream username.

Provision the selected environment variable securely. Only explicit allowlisted `env:NAME`
references and opaque API tokens are resolved by this component. Values are 1–8192 printable
ASCII bytes with no whitespace/newline or `Bearer ` prefix. No home search, credential file,
legacy username/key, OAuth/browser, `.env` loading or automatic fallback exists. Every authorized
call reads a fresh value and still requires the same account.

## Read-only account check

Installed metadata is checked before any credential access, without importing the authenticated
client or contacting Kaggle. The fixed helper then introspects the token and compares the
server-returned account before reading quota. Mismatch stops without fallback. Reports omit raw account/token/path/exception data and separate
local readiness, authentication, account match and quota-data availability. `quota=available`
means an object was supplied, not positive remaining capacity; absent data stays unknown and is
never invented allowance.

## Process and transport boundaries

The token travels over stdin, not arguments, environment or files. Python runs isolated with
an empty temporary home/cwd and restricted environment. The official SDK constructs/parses
requests; a documented private `_session` seam is bound to the pinned layout and fails on change.
Only the two exact production HTTPS RPCs are allowed in order, with verified TLS, no redirects,
retries or ambient proxies. Bodies must be strict bounded JSON before SDK decoding.

Bounds are 64 KiB responses, 16 KiB stdout/stderr, 5/10-second connect/read, a 30-second parent
child deadline, independent 25-second watchdog and a serialized 60-second service budget.
The fixed leaf helper is not a general process-tree supervisor. Buffer clearing does not erase
all immutable strings, OS copies or retained user data. Never share full private configuration
or environment dumps. See [ADR-0015](../decisions/0015-read-only-kaggle-preflight.md) and
[research](../research/kaggle-interface-review.md).
