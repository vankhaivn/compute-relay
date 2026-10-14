# Operator validation checklist

A repeatable procedure for re-qualifying a fresh clone, host or provider account. It is not a
record of completed runs and not a claim that planned features exist.

Keep the full run record outside Git. If a failure or changed support conclusion needs handoff,
open a focused issue/PR using the [bug report template](bug-report-template.md) with the minimal
sanitized commit, configuration, command and observation needed to reproduce it.

## 1. Rules

- Read [current status](../status.md), [CLAUDE.md](../../CLAUDE.md), this checklist and any
  issue/PR evidence linked by the task. Record the source commit and any dirty diff before the
  first check; a different binary or configuration is a different evidence context.
- Run one check at a time and record its actual result. An unexpected failure blocks its
  dependents, not unrelated local checks. A skip, missing implementation or unavailable toolchain
  is not a pass. Do not change production code or weaken assertions during a validation-only run.
- Use only synthetic inputs. Never print, paste, commit or upload tokens, credential environments,
  private databases, raw logs or signed URLs. Share reviewed excerpts and hashes, with aliases for
  account names and host paths.
- A request to run tests does not authorize provider effects. Record separate approvals for
  authenticated reads, private staging, each bounded GPU or CPU attempt, fault experiments and
  exact-target cleanup. Missing approval is `blocked-authorization`; never enable billing, use
  another account or switch to public resources.

## 2. Values

Choose a new private work directory once. If it already exists, inspect it; never delete or
reinitialize it to bypass an uncertain submission.

```sh
umask 077
export CR_WORK=/absolute/private/qualification-run
mkdir -m 700 "$CR_WORK"
export CR_ROOT="$CR_WORK/runtime"
export CR_BIN="$CR_WORK/compute-relay"
export CR_CONFIG="$CR_WORK/kaggle.json"
```

`CR_ROOT` must not exist yet. A local GPU is not required.

## 3. Local checks

**V01 — Source and build.** Record versions, exit codes and the executable SHA-256:

```sh
git rev-parse HEAD
git status --short
go version
go build -trimpath -o "$CR_BIN" ./cmd/compute-relay
"$CR_BIN" help
```

`go.mod` and the files under `tools/kaggle-client` are the pin sources. A missing toolchain is
`blocked-environment`, not permission to lower versions or rewrite locks.

**V02 — Offline checks.**

```sh
go run ./cmd/devtool check
go run ./cmd/devtool test-race
./scripts/kaggle-client.sh sync
./scripts/kaggle-client.sh check
```

Run the [runner checks](../../runner/README.md) on Linux with the pinned environment. Record test
counts and skips, not just a green exit. Fakes, mocked SDK transport and synthetic tensors are
offline evidence only.

**V03 — Installation and access.**

```sh
"$CR_BIN" init --root "$CR_ROOT"
"$CR_BIN" workspace create --root "$CR_ROOT" --id app
"$CR_BIN" token issue --root "$CR_ROOT" --workspace app --scope read --scope write --scope operate --ttl 24h --output "$CR_ROOT/app-token"
"$CR_BIN" state --root "$CR_ROOT"
```

Expected: a new installation, one private token file and a non-secret receipt. Repeating `init`
must reject the existing root.

**V04 — Admission and replay.** Apply and grant the profile example from
[application CLI](../application-cli.md), package a synthetic `main.py` with `bundle create`,
start `serve` in the foreground, upload the bundle, then validate and submit the job example:

```sh
"$CR_BIN" validate --file "$CR_WORK/job.json"
"$CR_BIN" job validate --workspace app --token-file "$CR_ROOT/app-token" --file "$CR_WORK/job.json"
"$CR_BIN" job submit --workspace app --token-file "$CR_ROOT/app-token" --file "$CR_WORK/job.json" --idempotency-key validation-job-001
```

Expected: validation without admission, then one durable job/attempt receipt and
`dispatch_enabled=false`. Repeat the same submit once and compare the original IDs.

**V05 — Conflict, reopen and authority.** Submit a changed copy with the original key: expect a
conflict and no new attempt. While serve runs, `state` must fail with a lock error; never remove
lock files. Stop serve, restart on the same root and replay the original request: expect the
original IDs. Cancel the queued attempt with an explicit key and replay it: expect the original
control receipt and local dispatch prevention only. Last, revoke the token and repeat a read:
expect authentication denial.

**V06 — Artifact delivery.**

```sh
go test -count=1 -race ./internal/artifactwire ./internal/appclient ./internal/appcli ./internal/api ./internal/store/sqlite
```

These cover committed publications, downloads, permissions, expiry and late-stream failures. A job
admitted in V04 has no publication; never seed SQL to fake an end-to-end pass.

## 4. Provider checks

**V07 — Explicit Kaggle mode.** Follow the [Kaggle runtime](../kaggle-runtime.md) with
`--max-provider-attempts 1`. First start serve with the token variable unset: expect startup to
fail without credential details before any worker starts. With read approval, a normal start
verifies the account. With staging and GPU approval, submit one job, observe it to a terminal state, and
download its published result. Record whether remote activity is confirmed, possible or unknown;
on error or response loss keep the root and binary and never submit again automatically.

**V08 — Managed mode.** Follow [managed connections](../managed-connections.md): configure and
check one connection, grant one authorization for one attempt, then observe, collect and read logs
for that attempt. Stopping serve mid-attempt and restarting it on the same root checks
reconciliation without a second submission.

**V09 — SSH host.** First run the local engine check on a machine with Docker; it explicitly installs the repository image for the managed pipeline, retains the legacy
base-image fixture, and runs containers through an in-process SSH server:

```sh
COMPUTE_RELAY_SSH_DOCKER_E2E=1 go test -count=1 -run 'TestRealDockerOverSSH|TestManagedSSHDockerPipeline' ./internal/provider/sshhost ./internal/runtimehost
```

The managed pipeline uses the engine's verified architecture. An Intel claim requires an
amd64 engine; `COMPUTE_RELAY_SSH_DOCKER_PLATFORM=linux/amd64` only selects emulation for the
legacy `TestRealDockerOverSSH` fixture. Then, with approval for the exact host, follow [SSH hosts](../ssh-hosts.md): verify a
password and a key connection with the host's own fingerprint and refuse a wrong one, explicitly
install the repository image and confirm the resulting frozen image ID, run one
authorized CPU job to a published result across a Relay restart, cancel a second authorized job,
and request cleanup for each attempt. Confirm on the host that no `compute-relay-` container
remains.

For cleanup response-loss and restart qualification, separately authorize a disposable Linux-amd64
Docker daemon running inside an exact container, an image installation and one CPU job with a
120-second remote wall limit. The daemon must start without containers. Supply its full 64-character
container ID and a new absolute evidence directory:

```sh
COMPUTE_RELAY_CLEANUP_DOCKER_CONTAINER='<full-disposable-engine-container-id>' \
COMPUTE_RELAY_CLEANUP_EVIDENCE='/absolute/private/new-cleanup-evidence' \
go test -count=1 -timeout 20m -run '^TestManagedSSHCleanupRecovery$' ./internal/runtimehost
```

This opt-in check runs real Docker commands through loopback SSH, withholds a completed deletion's
response, reopens the original runtime, advances the lease clock and recovers only the original
IDs. It checks idempotent replay, preservation of a replacement at the old name, unrelated volume
and container contents, image inventory and verified local inputs/results. It leaves private
evidence and disposable-engine fixtures for inspection, including on failure. Review exact engine
and volume identities before separately authorized teardown; never use prune or reuse an
interrupted evidence directory. Ordinary tests and CI skip this live check.

**Gaps needing a reviewed procedure.** Do not invent commands for these; record them as
`blocked-procedure` until a bounded plan and approval exist:

- Provider timeout and cancellation. Local timeout is not remote timeout, and cancellation stays
  manual without a verified session target.
- Deliberate submission-response loss. A normal restart does not induce this ambiguity; never
  simulate it by retrying, deleting the journal or creating a new root.
- Cleanup and backup. Cleanup apply is the attempt-scoped control only; any other cleanup needs
  approval for exact targets. A restore test preserves SQLite, both blob roots and identity markers and activates
  only one copy.

Update [current status](../status.md) and the
[feasibility record](research/kaggle-feasibility.md) only for claims a run actually established.

## 5. Failure handoff and retest

For each unexpected result, open a focused issue from the bug template with expected and actual
behavior, source and binary identity, the minimal command with private values aliased, the exit
code and possible local or remote effects. Mark dependent checks blocked by it.

After a fix, keep the original failure and add a retest with the fix SHA and new evidence. Resolve
old remote activity and get approval before a new live run. A code change without an observed
retest is `fix-pending-verification`, not a closed failure.
