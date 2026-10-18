# Finite RTMPS delivery example

Streams one local MP4 in a loop to one RTMPS endpoint until a fixed UTC end time, using an existing
SSH connection and Relay's public API. H.264/AAC is copied without transcoding. It demonstrates
[prepare and start](../../docs/prepare-start.md) with the endpoint passed as a workload secret.

An 8-hour 1080p run was tested once with an earlier Relay build on a macOS Intel Docker host with a
local receiver; public ingest, LAN RTMPS, longer runs and host interruptions are untested.

## Requirements

| Item | Requirement |
|---|---|
| Controller | Python 3.11 on a POSIX host, `compute-relay` on `PATH` |
| Relay URL | HTTPS, or plain HTTP only for `localhost`/`127.0.0.1`/`::1` |
| Token | File with mode `0600`, scopes read, write, operate, execute |
| Connection | Verified SSH `linux/amd64` selection with internet allowed (`serve --managed-allow-internet`) |
| Media | ≤1 GiB MP4, one H.264 `yuv420p` stream + one AAC-LC stream (44.1/48 kHz, 1–2 ch), even width/height 2–4096, 1 s–6 h |
| Wall time | More than 40 s (30 s setup + 10 s finalization), at most 24 h |
| Remote free space | ≥ `2 × staged input bytes + expanded runtime + 64 MiB`, observed within the last 10 minutes, on the smaller of host staging and Docker storage |

FFmpeg is pinned in [`runtime.lock.json`](runtime.lock.json) (Linux amd64, LGPL, hash-checked; needs
glibc 2.28, Linux 4.18). Nothing is bundled; keep its license when redistributing.

## Run

```sh
umask 077
mkdir -p "$HOME/.compute-relay/finite-rtmps"
python3 examples/finite-rtmps/fetch_runtime.py --output "$HOME/.compute-relay/finite-rtmps/ffmpeg.tar.xz"
```

Prepare (uploads inputs and prepares the attempt; the end time is then fixed for the session):

```sh
python3 examples/finite-rtmps/controller.py \
  --url https://relay.example --workspace WORKSPACE_ID \
  --token-file /secure/path/workspace-token \
  --session "$HOME/.compute-relay/finite-rtmps/session" \
  prepare --media /data/input.mp4 \
  --runtime "$HOME/.compute-relay/finite-rtmps/ffmpeg.tar.xz" \
  --connection CONNECTION_ID --width 1920 --height 1080 \
  --end-at 2026-10-20T20:00:00Z --wall-seconds 28800 \
  --remote-free-bytes 5000000000 --space-observed-at 2026-10-20T12:00:00Z
```

The other subcommands take the same four leading options (`--url`, `--workspace`, `--token-file`,
`--session`):

| Subcommand | Effect |
|---|---|
| `start` | Prompts for the endpoint, or `start --endpoint-stdin < /secure/path/endpoint`. Never put it in arguments or env. |
| `observe --pages 5` | Job state plus progress records (every 10 s). Safe anytime. |
| `cancel` | Requests cancellation; then observe until confirmed inactive. |
| `finish` | Downloads and verifies artifacts, then cleans up the attempt. |
| `continue --remote-free-bytes N --space-observed-at UTC` | New attempt after a finished `failed`/`cancelled`/`timed_out` one, before the end time. Start it again explicitly. |

If a request result is uncertain, run `observe` and let the controller replay its recorded request;
never retry with a new key. The workload stops if output timestamps and frames stop advancing for
30 s. The endpoint is visible to process inspection on the remote host. Your application still owns
receiver health, external session finalization and credential revocation.

## Checks

```sh
uv run --no-project --python 3.11.16 python examples/finite-rtmps/check.py
```

Offline tests only (no FFmpeg download, no provider). For a live test, see the
[controlled receiver fixture](fixtures/README.md).
