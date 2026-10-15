# SSH content cache and application images

Managed SSH jobs can reuse immutable input bytes on one verified Docker machine. Relay keeps the
cache in labelled Docker volumes, keyed by installation, machine, engine, SHA-256 and byte count.
LAN, Tailscale and other connection aliases to the same machine and engine share the same entries.
Kaggle does not use this cache and continues to upload all job objects.

## Declare content identities

The job schema still requires a workspace-owned `object_id` for the bundle and every object input.
Managed SSH v3 preparation caches these objects using the digest and size frozen at admission.
Optionally add both `sha256` and `bytes` beside the object ID to assert their expected identity;
admission compares them with Relay's retained metadata before accepting the job. Omitting both
fields preserves the legacy request shape and still permits caching from frozen object metadata.
Supplying only one is invalid. The bundle is bounded at 100 MiB and each input object at 2 GiB.

```json
{
  "bundle": {
    "object_id": "obj_bundle",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "bytes": 123456
  },
  "inputs": [
    {
      "name": "source",
      "source": {
        "kind": "object",
        "object_id": "obj_source",
        "sha256": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "bytes": 234567
      },
      "target": "inputs/source.bin"
    }
  ]
}
```

`object_id` preserves workspace ownership and provides Relay's local bytes on a cache miss. The
digest and size identify cache content; they do not make an object public or interchangeable across
workspaces at admission. Duplicate references with the same digest and size share one machine
entry. A different size is a different key.

## Prepare and use

During SSH preparation Relay checks each exact cache generation on the verified engine. An entry
is a Docker named volume with one verified `blob` file. On a miss, one durable writer streams the
workspace object into a new labelled volume and checks the declared length and SHA-256. A fixed
read-only verifier checks the result on the host; only digest and size evidence crosses SSH. A hit
sends no object bytes over SSH. Transfer progress counts bytes actually sent, while readiness's
`staged_bytes` remains the logical total of all declared inputs.

The attempt mounts each cache volume read-only. The stopped-container identity, mount set and
readiness revision bind the exact generation; Relay re-verifies cache bytes before readiness and
Start. The runner checks input bytes again before use. A missing or corrupt entry is not used: Relay
retires that generation and, when the original write is conclusively finished, may populate a new
generation. An uncertain writer remains unresolved; lease expiry does not authorize a second
write. Attempts keep their exact mounted generation until cleanup confirms the mount is gone.

References are tracked per workspace and attempt, while physical bytes are charged once per
machine. A queued, preparing, ready, running, uncertain, or still-mounted attempt pins its key.
Cache eviction may remove only a verified generation with no durable pins or observed container
mounts, using least-recently-used order. Cache entries are not mutable workspaces and are not
deleted by per-attempt result retention.

## Optional application image

An application may also declare a workspace-owned image archive at the top level of a job:

```json
{
  "application_image": {
    "object_id": "obj_image",
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "bytes": 456789
  }
}
```

All three fields are required when `application_image` is present. The archive is bounded at 2 GiB.
Relay verifies the original archive identity locally, accepts one `linux` image for the verified
host architecture, and normalizes it to a tag-free load archive with Relay ownership labels.
The outer archive must contain regular metadata and layer files; linked outer members, extra
unreferenced members and mixed Docker/OCI layouts are rejected. Links inside image layers remain
part of the image. The
image must not declare a non-empty entrypoint or Docker volumes: Relay's fixed runner command must
remain the process, and anonymous volumes would escape the ownership inventory. The derived Docker
image ID is based on the rewritten image config and therefore differs from the source archive
digest. Relay reuses the exact verified derived ID on later jobs; it never imports source tags into
the host daemon. Kaggle rejects this field as unsupported rather than silently using a different
runtime. Omitting it preserves the standard SSH image behavior.

Application images are not installed packages. Relay does not run a Dockerfile build, registry
pull, package installation, `docker tag`, or host filesystem write for this feature. Image archive
references and verified derived images are retained and evicted independently of attempt data.
Only unreferenced images are eligible for least-recently-used eviction; images used by queued,
preparing, running or uncertain work remain pinned.

The job schema is [job-spec v1alpha1](../api/schemas/job-spec.v1alpha1.schema.json). SSH cache
staging and image behavior require the managed SSH v3 path. See [SSH hosts](ssh-hosts.md) for host
qualification and [host storage](host-storage.md) for the per-machine budget and inventory.
