# Finite RTMPS delivery example

This example copies one local MP4 to one RTMPS ingest endpoint for a fixed UTC interval. It uses the existing verified Linux/amd64 SSH connection and Compute Relay public API; it does not install an image or change connection settings. The remote workload loops the input, copies H.264/AAC without transcoding, and stops at the earlier of the configured end time and the admitted remote wall budget. See [current qualification limits](../../docs/status.md).

## Requirements and bounds

Use Python 3.11 and a POSIX controller host (the session lock uses `fcntl`), plus the native `compute-relay` client on PATH for bundle operations and verified artifact downloads. The API URL must use HTTPS; plain HTTP is accepted only for `localhost`, `127.0.0.1`, or `::1`. Use a private, regular token file with mode `0600` or stricter, containing a token authorized to read, write, operate, and execute in the workspace. The existing SSH selection must allow remote internet and the requested wall budget; the operator sets internet policy with `serve --managed-allow-internet`. Keep controller credentials out of command arguments, config, session files and captured diagnostics. Pass the token as a protected file path.

The runtime is pinned in [`runtime.lock.json`](runtime.lock.json): Linux/amd64, FFmpeg LGPL-3.0-or-later build, archive and extracted-member SHA-256 hashes, source version, and upstream source/build links. It requires glibc 2.28 and Linux 4.18 or newer. No binary is shipped here. `fetch_runtime.py` downloads only that HTTPS archive, checks its size and hash, and rejects insecure redirects. Upstream retention or availability can change; a failed download has no fallback. Preserve any `.partial` file for inspection rather than overwriting it. The workload extracts the pinned FFmpeg/ffprobe and `LICENSE.txt` into scratch and verifies all three members. Preserve the upstream license and notices when retaining or redistributing these tools; see the [pinned build recipe](https://github.com/BtbN/FFmpeg-Builds/tree/autobuild-2026-09-28-13-06) and [FFmpeg source](https://github.com/FFmpeg/FFmpeg/tree/ff48edd8b2).

Provide an immutable MP4 no larger than 1 GiB with exactly one H.264 `yuv420p` video stream and one AAC-LC audio stream (44.1 or 48 kHz, one or two channels), matching the supplied even width and height (each 2–4096). Duration must be 1 second to 6 hours. The optional CA file is limited to 1 MiB. The controller also enforces 2 GiB per uploaded object, 4 GiB aggregate non-bundle inputs, a 100 MiB bundle, and 500 MiB expanded bundle limit. Those API limits are separate from the 1 GiB media limit.

Before any RTMPS endpoint is supplied, provide a recent remote free-space observation. Required space is `2 × staged input bytes + expanded runtime bytes + 64 MiB`; the observation must be no more than 10 minutes old (and not over 5 seconds in the future). Use the smallest available-space reading across the host staging filesystem and Docker's writable storage. On Docker Desktop, the host and Linux VM can have different limits; host free space alone is insufficient. Re-observe and pass fresh values on every prepare or continue. The workload separately checks available scratch space before extraction. Preparation requires `--wall-seconds` greater than the 30-second setup plus 10-second finalization allowance and at most 24 hours. These are admission bounds, not a tested multi-hour support claim. Keep the host awake: powered-off or suspended machines cannot run deadline checks. Docker and the engine must remain available for the job lifecycle; suspend/resume, reboot, and engine restart behavior are not qualified by this example.

Progress records are emitted every 10 seconds and are at most 1 KiB each; the final summary is at most 16 KiB. They report output timestamps, frames, elapsed time and the last actual advance, and do not establish receiver health. Silent startup or lack of both timestamp and frame advance stops the child within 30 seconds plus bounded termination time. The endpoint is supplied after preparation readiness. The workload checks runtime, media and CA inputs before reading the secret file, then passes the endpoint in FFmpeg's local process arguments. Use a trusted operator-controlled host and workload; see [workload secret exposure limits](../../docs/workload-secrets.md).

## Run one delivery

Choose a fixed whole-second UTC end time in the future, a media file, its actual dimensions, a ready connection ID, and a current remote free-space reading with its observation timestamp. Create a private session directory and fetch the pinned runtime:

```sh
umask 077
mkdir -p "$HOME/.compute-relay/finite-rtmps"
python3 examples/finite-rtmps/fetch_runtime.py \
  --output "$HOME/.compute-relay/finite-rtmps/ffmpeg.tar.xz"
```

Prepare inputs and the job. The token file must already exist with restrictive permissions. Replace the example values, including the free-space observation, with current values:

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example \
  --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  prepare \
  --media /data/immutable-input.mp4 \
  --runtime "$HOME/.compute-relay/finite-rtmps/ffmpeg.tar.xz" \
  --connection VERIFIED_CONNECTION_ID \
  --width 1920 --height 1080 \
  --end-at 2026-10-12T20:00:00Z \
  --wall-seconds 28800 \
  --remote-free-bytes 5000000000 \
  --space-observed-at 2026-10-12T12:00:00Z
```

The end time is persisted in the session and cannot be changed by a rerun. Supply a recent disk observation that meets the computed requirement; the illustrative timestamp and byte count above are not current values. The controller creates a private config, bundle, and session journal, then uploads immutable inputs and prepares the attempt. On an uncertain write, first use `observe`; do not invent a new idempotency key or alter the original request. An explicit rerun may replay only the exact recorded intent/key. A new upload may leave an unreferenced immutable object, but does not itself start compute.

Start only after preparation reports ready. In an interactive terminal, omit `--endpoint-stdin` to receive a hidden prompt. For noninteractive use, pipe the endpoint through stdin and include `--endpoint-stdin`; do not place its value in command arguments or environment variables:

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  start

# Or, for a noninteractive invocation, use a protected secret file on stdin:
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  start --endpoint-stdin < /secure/path/rtmps-endpoint
```

The input file must contain only the endpoint and a final newline at most; protect it as a secret. The controller submits it as an ephemeral workload secret and does not write its value to the session journal. The workload's FFmpeg command line can expose the endpoint to local process inspection on the remote host.

Observe at any point, including after a client interruption or uncertain request:

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  observe --pages 5
```

Observation returns job/readiness state and bounded progress records when available. The session remembers when an attempt's completed log has been fully read, including across controller restarts. Missing, old, or unavailable logs do not prove inactivity, receiver failure, or successful delivery. The application/operator remains responsible for external session finalization, credential revocation, receiver health, and destination visibility. If the controller disappears, a separate durable finalizer is needed to ensure the application session is finalized.

To request cancellation, then observe until the original attempt is confirmed inactive. Cancellation is intent; it is not proof that the process stopped:

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" cancel
```

After inactivity is confirmed, finish retrieves and verifies available artifacts and requests exact-attempt cleanup. For an inactive attempt with an available result, it uses the native artifact client because Python's `http.client` cannot verify the mandatory final HTTP trailer. Preserve original outputs and session state if download or cleanup is uncertain; do not delete them to make a retry appear successful.

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" finish
```

## Explicit continuation

A continuation is a new attempt, not an automatic retry. Use it only after the prior attempt is confirmed inactive, `finish` has succeeded, its execution state is retryable (`failed`, `cancelled`, or `timed_out`), and the original end time has not passed. Pass a fresh remote-space observation. The original end time remains fixed:

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  continue \
  --remote-free-bytes CURRENT_FREE_BYTES \
  --space-observed-at CURRENT_UTC_OBSERVATION_TIME
```

The controller records the new attempt identity and prepares it; start it explicitly after readiness. If retry admission succeeded but preparation was interrupted, use `prepare` with fresh space observations on the same session to finish preparing that recorded attempt. If any write result is uncertain, observe the original job/attempt and replay only the exact persisted request intent. Never retry under a fresh key while the original effect is unknown. The application decides whether an explicit continuation reuses its external session and supplies credentials for that decision.

## Offline checks

```sh
uv run --no-project --python 3.11.16 python examples/finite-rtmps/check.py
```

These synthetic tests exercise watchdogs, clock jumps, input rejection and public-client recovery without downloading FFmpeg or contacting a provider. Use the [controlled receiver fixture](fixtures/README.md) for separately authorized live qualification.
