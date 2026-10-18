# Controlled receiver fixture

`receiver.py` is an optional RTMPS receiver for an authorized live test of the example. It is not
part of the job or of Relay. It wraps the MediaMTX release pinned in
[`receiver.lock.json`](receiver.lock.json): verify the archive and both member hashes, keep its MIT
`LICENSE`, and never use a floating image or install it in the workload container.

## Container setup

Run it in its own container from an existing Linux amd64 Python image, with `receiver.py`,
`mediamtx` and `LICENSE` staged under `/fixture`:

| Setting | Value |
|---|---|
| User | `65534:65534` |
| Limits | 1 CPU, 512 MiB, 128 PIDs |
| Hardening | drop all capabilities, `no-new-privileges`, no restart |
| Logs | `local` driver, `max-size=1m,max-file=2` |
| tmpfs (`0700`, `rw,noexec,nosuid,nodev`) | `/private` 64 KiB, `/data` 128 MiB |
| Port | container 1936 on host loopback only, e.g. `127.0.0.1:19360:1936` |

Never expose it publicly. Confirm the workload's hostname reaches this listener on the target engine.

## Use

```sh
python3 -E -s /fixture/receiver.py supervise --end-at END_UNIX_SECONDS
```

`END_UNIX_SECONDS` is at most nine hours ahead. Within 30 s, run `receiver.py deliver` in the same
container with JSON on stdin containing exactly `key_pem`, `cert_pem` and `password` — a fresh
synthetic certificate whose SAN matches the RTMPS hostname, and a random 32–128 character
alphanumeric password. Give the certificate to the workload as its CA input; never put the key or
password in Docker arguments, labels, env, logs or workload inputs.

The supervisor starts one receiver (publisher `relay-publisher`, path `finite-test`, credentials in
query parameters `user` and `pass`) and stops it at the deadline, on signal, or when `/data` exceeds
112 MiB.

| Action | Effect |
|---|---|
| `observe` | Path readiness, codec tracks, byte counters |
| `records` | Retained recording names, sizes, times |
| `sample` | Copy the previous complete segment to `/data/sample.mp4` (≤16 MiB) |
| `stall` | `SIGSTOP` the receiver (authorized fault test) |
| `disconnect` | Terminate the receiver (authorized fault test) |

## Checks to perform

- Export samples with a bounded read through `docker exec` (archive copy may miss tmpfs). Compare
  codec properties and payload hashes with the source across loops; verify inserted SPS/PPS against
  the original codec config before normalizing hashes. Compare received media time with wall time.
- TLS negatives: an unrelated CA and a wrong-SAN certificate must both fail to publish.
- Watchdog: with media advancing, stop the application and Relay, then stall or disconnect the
  receiver. Wait until the original workload is confirmed inactive, restore Relay, retrieve verified
  results, scan them for the synthetic credential, and clean up only the exact containers involved.

Not covered: public routing, host suspend, reboot, engine restart, platform session finalization.
