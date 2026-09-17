# Operator validation checklist

**Start here when validating a fresh clone with a local agent.** This is the final handoff
for evidence still missing from development: the real host, real account and controlled live
execution. It is not a claim that all planned product features exist.

Record every outcome in [validation-results.md](validation-results.md). For failures, add a
report using [bug-report-template.md](bug-report-template.md). The next agent must be able to
reproduce the failure from the recorded commit, configuration shape, command and observations
without receiving a secret or guessing whether compute already started.

## 1. Rules for the executing agent

Read [current status](../status.md), [AGENTS.md](../../AGENTS.md), this checklist and any existing
run/bug records before acting. Work on a validation branch. Never erase an older failure to
make a new run look successful. Record the source commit and dirty-code diff before the first
check; a different binary/configuration is a different evidence context.

Run one stage at a time. Inspect the actual result and record it before moving on. An unexpected
failure blocks its dependents, not unrelated safe local checks. Do not label a skipped test,
missing implementation or unavailable toolchain as a pass. Do not change production code or
weaken assertions during a validation-only run; report the bug first.

Use only operator-owned synthetic inputs. Never print, paste, commit or upload tokens, full
credential environments, private databases, acceptance records, raw logs or signed URLs. Store
raw evidence outside the checkout with private permissions. Share reviewed excerpts and hashes,
not an archive of the state directory. Use aliases for private account names and host paths.

An instruction to run tests does not authorize all provider effects. Record separate approvals
for authenticated reads, private staging, one bounded GPU attempt, fault/timeout experiments
and exact-target cleanup. Missing approval means `blocked-authorization`; do not enable billing,
use another account or switch to public resources. Never place real provider secrets in CI.

## 2. Fill values before running

Copy the run template in the results ledger. Keep actual private values in a separate operator
file outside Git. The examples below use POSIX shell; on Windows use equivalent PowerShell
variables, `.exe` binary names and private-directory ACLs. Do not blindly paste POSIX paths.

| Value | Fill locally | May be committed? |
|---|---|---|
| `RUN_ID` | Unique report ID, for example `RUN-20260917-01`. | Yes. |
| source SHA / dirty diff | Output of `git rev-parse HEAD`; record non-doc changes explicitly. | Sanitized SHA/diff only. |
| `CR_WORK` | New absolute private directory outside the checkout; parent exists. | Alias only. |
| `CR_ROOT` | `$CR_WORK/runtime`, initially nonexistent. | Alias only. |
| `CR_ACCEPT_ROOT` | `$CR_WORK/acceptance`, initially nonexistent and separate from normal runtime. | Alias only. |
| `CR_BIN`, `CR_PREFLIGHT`, `CR_ACCEPT_BIN` | Absolute paths to the three retained executables below. | Binary hashes, not private paths. |
| `CR_CONFIG` | Absolute non-secret Kaggle preflight JSON path under the private work directory. | Sanitized shape, not real account/path values. |
| expected account / token reference | Actual canonical account and `env:CR_KAGGLE_TOKEN`. | Alias/reference syntax only; never the token value. |
| machine shape | Explicit `NvidiaTeslaT4` or `NvidiaTeslaP100` selection. | Yes. |
| local endpoint | Literal loopback, normally `http://127.0.0.1:7331`. | Yes, without credentials. |
| approvals | UTC time, approving operator, allowed effect, budget and target scope. | Yes, sanitized. |

Choose `CR_WORK` once. If it already exists, inspect its records; do not delete/reinitialize it
or choose a fresh acceptance root merely to bypass an uncertain submission.

```sh
umask 077
export CR_WORK=/absolute/private/validation-run
mkdir -m 700 "$CR_WORK"
export CR_ROOT="$CR_WORK/runtime"
export CR_ACCEPT_ROOT="$CR_WORK/acceptance"
export CR_BIN="$CR_WORK/compute-relay"
export CR_PREFLIGHT="$CR_WORK/kagglepreflight"
export CR_ACCEPT_BIN="$CR_WORK/kaggleacceptance"
export CR_CONFIG="$CR_WORK/kaggle-preflight.json"
```

The work directory creation must succeed before continuing. Do not run with placeholder paths.
A local GPU is not required: the acceptance payload runs on Kaggle, not on this control host.

## 3. Run register and dependencies

All rows begin unverified for a new operator run. The results ledger distinguishes `pass-offline`
from `pass-live`; a checklist row is not a blanket milestone signoff.

| ID | Check | Depends on | Expected evidence / gate contribution |
|---|---|---|---|
| V01 | Source, toolchain, locked builds | Filled values | Reproducible executable/version record on this host. |
| V02 | Offline repository, SDK and runner checks | V01 | Local component evidence; no live gate closes. |
| V03 | Local installation, authority and HTTP lifecycle | V01 | Usable local host, locks and readiness boundary. |
| V04 | Profile, bundle, upload, contextual validation and receipt replay | V03 | Real application admission; work remains queued. |
| V05 | Reopen, conflict, authority and local controls | V04 | Original receipts/state and denied unauthorized reads. |
| V06 | Published-artifact delivery regression | V02 | Existing real HTTP/SQLite/CLI tests; manual fresh-runtime E2E remains a feature gap. |
| V07 | Local Kaggle pins and missing-credential check | V01 + locked client | Local setup without account or compute claims. |
| V08 | Authorized account preflight | V07 + read approval | M1-02/M4-01 account evidence; quota precision and account conditions remain explicit. |
| V09 | Fixed GPU experiment preparation | V08 | Original job/input/configuration frozen locally. |
| V10 | One private staging/GPU submission | V09 + staging/GPU approval | Actual staging/execution evidence; preserve the durable handoff. |
| V11 | Separate-process resume and verified results | V10 + read approval | Scoped M4-06 evidence and contributions to M1-03/04/05; not full M1 acceptance. |
| V12 | Explicit collection-only recovery, when needed | Failed collection from V11 | Same pin/attempt, no new compute. Otherwise `not-run`, not a fabricated fault pass. |
| V13 | Live logs/quota and genuine multi-page provider output | V11 + reviewed probe | Remaining M1-05/capability evidence; fixed six-file output does not prove multi-page live behavior. |
| V14 | Controlled provider timeout/cancellation conclusion | Separate procedure + approval | M1-06; unknown/manual cancellation remains honest. |
| V15 | Deliberate provider-response loss and recovery | Separate procedure + approval | M1-07; a normal restart is not induced ambiguity. |
| V16 | Exact-owned cleanup and coordinated recovery checks | Resolved activity + separate approval | Retention/cleanup/backup evidence; no reset or unreviewed deletion. |
| V17 | General runtime and release acceptance | Missing product integration | M5/M6; `blocked-implementation` until the required code exists. |
| V18 | Review evidence and update gates | Applicable rows + bug dispositions | M1-08/scoped M4 decision with exact evidence, limitations and reviewer. |

## 4. Local checks

### V01 — Source and build

From the clone root, inspect source identity and versions before any account use:

```sh
git rev-parse HEAD
git status --short
go version
python3 --version
uv --version
go build -trimpath -o "$CR_BIN" ./cmd/compute-relay
go build -trimpath -o "$CR_PREFLIGHT" ./cmd/kagglepreflight
go build -trimpath -o "$CR_ACCEPT_BIN" ./cmd/kaggleacceptance
"$CR_BIN" help
"$CR_ACCEPT_BIN" --help
```

Use `go.mod`, `tools/kaggle-client/pyproject.toml`, `.python-version` in that directory and
`uv.lock` as the pin sources. Failure to obtain a required toolchain is `blocked-environment`,
not permission to lower versions or rewrite locks. Record all exit codes and executable SHA-256
hashes using a native checksum tool or Python `hashlib`. Retain the acceptance executable:
**do not rebuild it between prepare, submit, resume, collect and status.**

### V02 — Offline checks

```sh
go run ./cmd/devtool check
go run ./cmd/devtool test-race
./scripts/kaggle-client.sh sync
./scripts/kaggle-client.sh check
```

On Windows use `scripts/kaggle-client.ps1` or `.cmd`. The client wrapper uses the locked Python
environment. Record root-test counts, skips and failure summaries, not just a green exit.
Read the [runner check instructions](../../runner/README.md) and run the supported runner checks
on Linux with the pinned environment; record OS-specific skips rather than claiming native
Linux execution from a different host. Never invoke the runner's `--execute` with arbitrary
application code locally as a substitute for GPU acceptance.

Expected: offline suites succeed under the committed stack. Package/tool downloads are network
setup, not provider probes. A fake provider, mocked SDK transport or synthetic CUDA tensor is
**not** live evidence. Store full logs privately; attach only sanitized failure excerpts.

### V03 — Local installation and access

```sh
"$CR_BIN" init --root "$CR_ROOT"
"$CR_BIN" workspace create --root "$CR_ROOT" --id app
"$CR_BIN" token issue --root "$CR_ROOT" --workspace app --scope read --scope write --scope operate --ttl 24h --output "$CR_ROOT/app-token"
"$CR_BIN" state --root "$CR_ROOT"
```

Expected: new installation, one private token file and a non-secret token receipt. Never display
the token file. Save the receipt's token ID privately for V05. Repeating `init` must reject the
existing root, not overwrite it. Do not mutate database or identity markers to make a test pass.

### V04 — Configure before serving, then use the application API

Save the complete profile example from [application CLI](../application-cli.md) in
`$CR_WORK/profile.json`; its `local-cpu` name is admission policy only. Apply and grant it:

```sh
"$CR_BIN" profile apply --root "$CR_ROOT" --file "$CR_WORK/profile.json"
"$CR_BIN" profile grant --root "$CR_ROOT" --workspace app --name local-cpu
"$CR_BIN" profile show --root "$CR_ROOT" --name local-cpu
```

Create a synthetic `main.py` under `$CR_WORK/job-source` which would write `answer.json` using
`CC_OUTPUT_DIR`. Do not execute it. Package it with the documented tool, not generic `tar`:

```sh
"$CR_BIN" bundle preview --root "$CR_WORK/job-source" --include main.py
"$CR_BIN" bundle create --root "$CR_WORK/job-source" --include main.py --output "$CR_WORK/code.tar.gz"
"$CR_BIN" bundle inspect --file "$CR_WORK/code.tar.gz"
"$CR_BIN" serve --root "$CR_ROOT" --listen 127.0.0.1:7331
```

Leave serve in the foreground in one terminal. In a second terminal with the same non-secret
path variables, use the application token file:

```sh
"$CR_BIN" object upload --workspace app --token-file "$CR_ROOT/app-token" --file "$CR_WORK/code.tar.gz"
```

Use the returned `object_id` in the complete job example in the application guide and save
`$CR_WORK/job.json`. Keep `inputs: []` explicit. Then:

```sh
"$CR_BIN" validate --file "$CR_WORK/job.json"
"$CR_BIN" job validate --workspace app --token-file "$CR_ROOT/app-token" --file "$CR_WORK/job.json"
"$CR_BIN" job submit --workspace app --token-file "$CR_ROOT/app-token" --file "$CR_WORK/job.json" --idempotency-key validation-job-001
```

Expected: schema/contextual validation without admission, followed by one durable job/attempt
receipt. Record the actual job/attempt IDs as private variables `CR_JOB_ID` and `CR_ATTEMPT_ID`.
Never guess IDs. Explicitly repeat **the same** submit once and compare the original IDs and
receipt. This is intentional idempotency validation, not an automatic retry loop.

```sh
"$CR_BIN" job status --workspace app --token-file "$CR_ROOT/app-token" --id "$CR_JOB_ID"
```

Expected: local queued admission; server reports `dispatch_enabled=false`. Do not wait for GPU
or results: no worker runs in this server. Mark attempts to test that absent feature as
`blocked-implementation`, not as a runtime bug.

### V05 — Conflict, reopen and authority

Make a separate copy of the job with a changed name. Submit that copy using the original key:
expect a conflict and no new attempt. Preserve the original file/key. Stop serve gracefully,
run `state`, restart serve with the same root and explicitly replay the original request:
expect the original IDs. Attempting local administration while serve owns the root must fail
with a lock error; do not remove lock files.

With the queued job, request local cancellation using the recorded IDs:

```sh
"$CR_BIN" job cancel --workspace app --token-file "$CR_ROOT/app-token" --id "$CR_JOB_ID" --attempt "$CR_ATTEMPT_ID" --idempotency-key validation-cancel-001
```

Inspect current operation/job using the returned operation ID. Expected: local dispatch
prevention, not proof of remote termination. Replay must preserve the original control receipt.

Stop serve, revoke the application token using its receipt ID, restart and repeat a read:
expect authentication denial, not secret disclosure or a successful stale grant. Keep this
negative test last, or explicitly issue a new token for subsequent authorized local work.

### V06 — Artifact delivery on this host

```sh
go test -count=1 -race ./internal/artifactwire ./internal/appclient ./internal/appcli ./internal/api ./internal/store/sqlite
```

Record the actual integration tests and skips. They exercise committed publications, HTTP/CLI
downloads, permissions, expiry and late-stream failures. They are offline component evidence.
A newly queued application job from V04 has no publication: an empty/not-found artifact result
is expected. **Do not seed SQL or open the acceptance directory with normal `serve`** to fake an
application end-to-end pass. Manual arbitrary-job → published-artifact testing waits for V17.

## 5. Credential-scoped and live checks

### V07 — Pinned environment and negative credential setup

After locked client installation, create `CR_CONFIG` privately using
[preflight configuration](../providers/kaggle-preflight.md). It must contain the actual account,
`credential_ref: "env:CR_KAGGLE_TOKEN"`, and the absolute locked Python executable. No token value
belongs in the file. The operator provisions that variable securely; do not print its contents.

```sh
"$CR_PREFLIGHT" --config "$CR_CONFIG"
```

Expected: exact local pins ready, authentication not checked, `batch_ready=false`. Run a
missing-token negative in a child environment without modifying the operator's original secret:

```sh
env -u CR_KAGGLE_TOKEN "$CR_PREFLIGHT" --config "$CR_CONFIG" --allow-read-only
```

Expected: sanitized credential-unavailable failure, no token lookup fallback and no provider
mutation. On Windows use an equivalent isolated child environment. Do not dump environment data.

### V08 — Explicit read-only account authorization

Only after approval for authentication/quota reads:

```sh
"$CR_PREFLIGHT" --config "$CR_CONFIG" --allow-read-only
```

Expected: verified authentication and matched account. Record unavailable/unknown quota honestly;
preflight availability is not a numerical allowance or GPU reservation. An optional invalid-token
negative must use one synthetic invalid value in an isolated child environment and explicit
approval for the expected failed request; no guessing, repeated guessing or account rotation.

If auth or account matching fails, stop before staging. Capture the sanitized report and actual
exit code. Do not expose raw provider responses, Authorization headers or credential file bytes.

### V09 — Freeze the fixed experiment locally

Select one supported shape, then build **no new executable** after this step:

```sh
"$CR_ACCEPT_BIN" prepare --root "$CR_ACCEPT_ROOT" --config "$CR_CONFIG" --machine-shape NvidiaTeslaT4
"$CR_ACCEPT_BIN" status --root "$CR_ACCEPT_ROOT"
```

Expected: `prepared-local`, immutable challenge/input and one admitted fixed job; no provider
acceptance. This utility has its own directory format. Keep normal runtime and acceptance roots
separate. Do not edit `acceptance.json`, process markers, object pins or the selected account.

### V10 — One authorized private staging/GPU attempt

Record approval for the synthetic private upload and **one GPU attempt** requesting 120 seconds
remote wall, 30 seconds setup and 15 seconds finalization, with internet disabled and no billed
fallback. These are requested budgets, not verified provider timeout enforcement. The check can
leave resources behind; a local interrupt does not cancel remote compute.

```sh
"$CR_ACCEPT_BIN" submit --root "$CR_ACCEPT_ROOT" --allow-private-staging --allow-gpu
```

Inspect the actual status. `resume-required` means a durable submission boundary exists,
not necessarily a completed or confirmed execution. A quota block is not a pass. On error or
response loss, retain the root, original binary and receipt; do not create a new root, erase
intent rows or automatically call submit again. Record whether remote activity is confirmed,
possible or unknown. Use the existing read-only recovery path when the retained state permits it.

### V11 — New process, same attempt, verified result

Run a new executable process, not an in-process call and not `go run`:

```sh
"$CR_ACCEPT_BIN" resume --root "$CR_ACCEPT_ROOT" --allow-read-only
"$CR_ACCEPT_BIN" status --root "$CR_ACCEPT_ROOT"
```

Require a qualifying live report with GPU arithmetic and distinct-process restart verified,
original job/attempt/challenge, all six expected files and independent hashes. Record the UTC
run time, source and executable hashes, client/environment versions, sanitized report and its
hash. Preserve private raw evidence. `status` exit 0 alone is not a GPU pass.

A blocked/needs-attention/restart-unverified report stays blocked or failed. Normal restart is
not a provider-response-loss fault experiment. Even `passed-live` retains
`full_m1_acceptance=false`, unverified provider timeout enforcement and unobservable execution
count/hardware release where applicable. Submit findings for V18 review before changing gates.

### V12 — Only when collection failed

Record the failed operation and existing snapshot first. An interrupted accepted transfer may
resume; a committed failed collection needs one explicit new key for a transfer-only retry:

```sh
"$CR_ACCEPT_BIN" collect --root "$CR_ACCEPT_ROOT" --allow-read-only --collection-key validation-collect-001
```

Use the same root/binary/account/attempt and original pin. Reuse that key only to recover its
receipt; do not invent repeated new keys. Require exact recovered bytes and no new submission.
If no real transfer failure occurred, record `not-run` for fault recovery; a happy-path download
is not evidence of recovering an actual failure. Do not disrupt a live transfer without a
separately reviewed, bounded fault procedure.

## 6. Checks needing additional implementation or a reviewed procedure

Do not invent commands for these gaps. Record their exact prerequisite and owner in the ledger.

**V13 — Quota/log/multiple-page evidence.** The fixed experiment has six results and the normal
server lacks public provider log/quota routes. It does not force multiple provider pages or
fully test live log delay/reservations. Use a separately reviewed bounded probe of the existing
Monitor/LogReader/ArtifactReader ports, with real account data kept private. Document actual
page counts, cursor behavior, units, missing fields and identity checks. Until that probe and
approval exist, use `blocked-procedure`, not an offline SDK test as live evidence.

**V14 — Timeout/cancellation.** There is no timeout-fault mode in the fixed acceptance CLI.
Review a separate finite workload/probe and recovery plan before consuming quota. Compare
requested budget with observed provider/runner results; local timeout is not remote timeout.
Current cancellation is manual without a verified session target. Do not pass kernel IDs as
session IDs or use deletion as cancellation. A reviewed unsupported conclusion is an optional
capability result, not proof that cancellation worked.

**V15 — Deliberate ambiguous submission.** Review a fault mechanism that loses acknowledgement
at the intended boundary without allowing another mutation. Define how intent, resource identity,
wire-call count and recovery will be evidenced before running it. Never implement this by retrying
SaveKernel, deleting a journal, or creating a fresh root after uncertainty. Existing offline
fault tests and V11 process separation do not close this live gate.

**V16 — Cleanup/backup.** The acceptance adapter has no cleanup apply command. Retain exact
owned resource IDs privately and resolve remote activity first. Any provider-console/manual
cleanup needs separate approval for those exact targets, with before/after observations; never
use a prefix sweep. A cleanup preview is not deletion. A coordinated restore test must preserve
SQLite, both blob roots and identity/process markers, quiesce writers, and activate only one copy.
There is no general backup CLI yet: review the library-level procedure before testing, not an
ad-hoc copy of a live `runtime.db`. Raw databases do not belong in Git or the bug report.

**V17 — Product/release gaps.** General worker/provider registration, remaining log/cleanup
surfaces, strict TOML, doctor, thin client/LLM examples, packaging and full release hardening are
not completed by this checklist. Mark their application E2E cases `blocked-implementation`.
After code exists, add exact clean-install, concurrency, shutdown, full recovery and platform
procedures before promoting support. A documentation edit cannot close these rows.

**V18 — Review.** Map evidence to M1-02…M1-08, the scoped M4-06 experiment and M5/M6 requirements.
List unresolved failures and unsupported capabilities. Record reviewer, UTC date and exact evidence
IDs for each accepted gate. Update [current status](../status.md) and the feasibility ledger only
for claims actually established; never turn all blocked rows green after one successful GPU job.

## 7. Failure handoff and retest

For every unexpected result create `BUG-<run>-NN` from the bug template in the results ledger
(or a linked reviewed issue). Include expected/actual behavior, exact source/binary identity,
minimal command with private values aliased, error/exit code, reproduction count, relevant
sanitized evidence and possible local/remote effects. Mark downstream checks blocked by that ID.

After a fix, retain the original failure and append a retest with the fix SHA and new evidence.
Do not overwrite the original acceptance binary or falsify its record to run patched code.
Resolve any old remote activity and obtain approval before a new live experiment; a new binary
may require a new separately authorized run rather than transparent old-state recovery.

Review the report diff for secrets, then commit **only** the sanitized validation/bug documents
on a report branch and open a PR. Preserve raw logs/state privately for targeted questions.
The fixing agent should reproduce from this handoff, add a regression, push a focused fix PR,
and request the same failing check again. A code change without an observed retest is
`fix-pending-verification`, not a closed validation failure.
