# Controlled receiver fixture

`receiver.py` is an optional Linux fixture for an operator-authorized qualification. It is
not part of the job, the Relay provider or an external media platform. Download the exact
MediaMTX release in [`receiver.lock.json`](receiver.lock.json), verify the archive and both
extracted member hashes, and preserve its MIT `LICENSE`. Do not use a floating image or install
the receiver into the workload container.

Stage `receiver.py`, the verified `mediamtx` executable and `LICENSE` under `/fixture` in a
separate owned container based on an existing compatible Linux amd64 Python image. Run as
`65534:65534`, with at most one CPU, 512 MiB memory and 128 PIDs. Drop all capabilities, enable
`no-new-privileges`, disable automatic restart, and bound the Docker log driver. With the
`local` driver, `max-size=1m,max-file=2` retains at most two rotated files. The staged files
must be readable, and `mediamtx` executable, by that user.

Mount two private tmpfs directories, owned by that user with mode `0700` and flags
`rw,noexec,nosuid,nodev`: `/private` (64 KiB) and `/data` (128 MiB). Publish container TCP
1936 only on the target host's loopback, for example `127.0.0.1:19360:1936`. Confirm that the
workload's configured hostname reaches this private listener on the target Docker engine.
The fixture keeps its HTTP API and metrics inside the receiver container's loopback and
disables other media protocols. Do not expose the fixture as a public service.

Start exactly one supervisor with an absolute UTC Unix timestamp, no more than nine hours
ahead:

```sh
python3 -E -s /fixture/receiver.py supervise --end-at END_UNIX_SECONDS
```

Within 30 seconds, invoke `receiver.py deliver` in that container with a JSON object on stdin
containing exactly `key_pem`, `cert_pem`, and `password`. Use a freshly generated synthetic
certificate/key and a random alphanumeric password of 32–128 characters. The certificate's
SAN must match the RTMPS hostname. Supply the public certificate as the workload's CA input;
keep the key and password in memory or protected temporary storage. Do not put their values
in Docker arguments, labels, environment variables, logs or ordinary workload inputs.

Delivery is one-shot. It writes the key, certificate and MediaMTX configuration only inside
`/private`, then starts one receiver process. The supervisor uses wall time and Linux
`CLOCK_BOOTTIME`; it stops the receiver at its deadline, upon signal, or if `/data` exceeds
112 MiB. Child diagnostics are discarded. fMP4 recording segments have a minimum duration
of 15 seconds and a retention target of 60 seconds; the tmpfs limit remains the hard bound.
The synthetic publisher is `relay-publisher`, path `finite-test`, with credentials supplied
in RTMPS query parameters `user` and `pass` as documented by the pinned MediaMTX release.

Available in-container actions:

| Action | Effect |
|---|---|
| `observe` | Read only the fixed path's readiness, codec tracks and byte counters. |
| `records` | List retained recording names, sizes and modification times. |
| `sample` | Copy the previous completed segment to `/data/sample.mp4`, bounded to 16 MiB. |
| `stall` | Send `SIGSTOP` to the exact receiver child for an explicitly authorized fault test. |
| `disconnect` | Terminate that child for an explicitly authorized disconnect test. |

Export a sample through a bounded read in `docker exec` while the container is running;
Docker archive copy may not expose tmpfs contents. Compare H.264/AAC codec properties and
compressed payload hashes with the original input across several loops. A remuxer may
insert H.264 SPS/PPS parameter sets into keyframes: verify those against the original codec
configuration before normalizing packet hashes. Measure received media time against elapsed
time. For TLS negatives, separately supply an unrelated CA and a certificate with a different
SAN; neither may publish media.

For watchdog tests, first establish advancing media, stop the application and Relay, and
then stall or disconnect this receiver. Inspect the original workload identity until it is
confirmed inactive; do not replace it from an observation timeout. Restore Relay, retrieve
verified results, scan retained output for the synthetic credential, and clean up only the
exact stopped workload and receiver containers. Preserve failures and unrelated resources.

The fixture does not test public routing, host suspend, reboot, engine restart or any
platform's session-finalization behavior.
