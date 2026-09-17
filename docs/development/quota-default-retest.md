# Retest: omitted quota billing flag

For **BUG-RUN-20260917-01-01**, reported in
[RUN-20260917-01 at 6a7d95d](https://github.com/vankhaivn/compute-relay/blob/6a7d95d/docs/development/validation-results.md).
Keep that original failure and append the observed retest in [validation results](validation-results.md).
The fix is [PR #29](https://github.com/vankhaivn/compute-relay/pull/29); code/test checkpoint
`4ed6de067119ba78d1646daccb91d65efb715a80`. Use the actual merged source SHA for the retest build.
Status remains **fix-pending-verification**, not a live pass.

## What changed

Monitor now interprets only an absent `isPayToScaleEnabled` as the pinned quota schema's false
scalar default. The same interpretation applies at the fresh free-policy check before SaveKernel.
True, explicit null and non-boolean flags still block. Total/used/reserved duration messages remain
mandatory for known numeric quota; zero/exhaustion, rounding, reservations, identity and no-retry
rules are unchanged. The representative 108000s/0s/0s shape should yield 108000 remaining seconds,
not unknown. A fixed quota gate does not guarantee subsequent live staging/execution will succeed.

## Important: the original root is pinned to the old executable

The harness hashes its executable and checks that hash against `acceptance.json` before opening
an existing experiment. Changing either embedded Python helper changes the executable hash.
**A new fixed binary cannot directly submit/resume/status the old root.** This fix does not add
an upgrade/rebind command. Do not overwrite the old binary, edit the recorded hash, remove state
or copy old records into a new installation to bypass the check.

Preserve the original root and binary as evidence. Retesting on exactly the same saved root with
a changed executable requires a separately reviewed pre-dispatch migration; it is not supported
by the current CLI. The supported alternative below uses a distinct, explicitly authorized run,
not the same attempt, and only after confirming the old run never entered preparation.

## Verify the pre-dispatch stopping point first

On the operator host, with no acceptance process running:

```sh
"$CR_ACCEPT_BIN" status --root "$CR_ACCEPT_ROOT"
```

Here both variables still refer to the **original** retained executable/root. Inspect the saved
V10 report and, through read-only inspection of the existing `state/runtime.db`, confirm no
preparation/submission intents or provider-owned resources were recorded. Check that submission/
resume process markers are absent. Keep the database and raw inspection output private; never
issue SQL writes or open a missing database in a mode that creates it.

For this reported bug, `quota-blocked` is returned before the dispatch engine is constructed.
Do not generalize `execution=not_submitted` to proof of no staging: other failure paths can have
staging effects. If the retained report/state is inconsistent, missing or ambiguous, stop and
record the blocker. Do not prepare another live experiment while effects remain unresolved.

## Supported fresh-run retest after review and merge

Once the no-intent/no-effect evidence is confirmed and the operator authorizes the new experiment,
choose new private paths outside the checkout (`CR_RETEST_BIN`, `CR_RETEST_ROOT`) and a new run ID.
Their parents must exist; the executable name/root must not already exist. Leave `CR_ACCEPT_BIN`
and `CR_ACCEPT_ROOT` untouched. Record the original and new source, binary and configuration hashes.

From the merged clone:

```sh
go test -count=1 -run TestMonitorProtoJSONQuotaDefaultReachesGoWithoutInventingAllowance ./internal/provider/kaggle
./scripts/kaggle-client.sh test
go build -trimpath -o "$CR_RETEST_BIN" ./cmd/kaggleacceptance
"$CR_RETEST_BIN" --help
"$CR_RETEST_BIN" prepare --root "$CR_RETEST_ROOT" --config "$CR_CONFIG" --machine-shape NvidiaTeslaT4
```

Use Windows equivalents where needed. Keep the original configuration/account/shape unless a
reviewed change is recorded. Prepare creates a new local job/attempt/challenge, not new provider
resources. Retain this new binary unchanged through all modes.

Only with renewed approval for private staging and one 120-second GPU attempt:

```sh
"$CR_RETEST_BIN" submit --root "$CR_RETEST_ROOT" --allow-private-staging --allow-gpu
```

Inspect and record the actual report/exit before proceeding. Passing the corrected quota gate may
expose a different provider-compatibility issue; report it separately. Do not blindly rerun submit.
For a recorded `resume-required` boundary, use a separate process with this **new run's** binary:

```sh
"$CR_RETEST_BIN" resume --root "$CR_RETEST_ROOT" --allow-read-only
```

Keep all original V10 evidence. Record whether quota passed, staging/submission actually occurred,
and whether subsequent GPU/artifact/restart checks qualified. Retest V10 does not automatically
close V11–V18 or M1/M4 gates. Any new uncertain effect retains its own root and recovery boundary.
