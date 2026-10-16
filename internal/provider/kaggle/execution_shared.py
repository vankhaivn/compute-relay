

import os


SHARED_INPUT_ROOT = Path("/kaggle/input")


# Appended only for attempts that mount shared input datasets; attempts without
# them keep the original bootstrap source byte for byte.
def shared_staging(payload, staging):
    owner = payload["dataset_owner"]
    input_root = SHARED_INPUT_ROOT
    mounts = {payload["dataset_slug"]: staging}
    for slug in payload["shared_datasets"]:
        if type(slug) is not str or not slug or "/" in slug or "\\" in slug or slug in mounts:
            raise RuntimeError("Compute Relay shared input identity is invalid")
        mount = None
        for candidate in (input_root / "datasets" / owner / slug, input_root / slug):
            mount = resolved_directory(candidate)
            if mount is not None:
                break
        if mount is None:
            mount = fallback_staging(input_root / "datasets", slug)
        if mount is None:
            raise RuntimeError("Compute Relay shared input mount is unavailable")
        mounts[slug] = mount
    # The runner opens inputs relative to one root without following links, so stage
    # from the mounts' common parent and name each file through its dataset directory.
    root = Path(os.path.commonpath([str(mount) for mount in mounts.values()]))
    if root == Path("/") or not root.is_relative_to(input_root.resolve()):
        raise RuntimeError("Compute Relay staging mounts do not share a dataset root")
    manifest = dict(payload["manifest"])
    manifest["bundle"] = dict(manifest["bundle"],
                              path=str((staging / manifest["bundle"]["path"]).relative_to(root)))
    if len(manifest["inputs"]) != len(payload["input_files"]):
        raise RuntimeError("Compute Relay staged inputs do not match the manifest")
    inputs = []
    for entry, (slug, name) in zip(manifest["inputs"], payload["input_files"]):
        if slug not in mounts:
            raise RuntimeError("Compute Relay staged input names an unknown dataset")
        inputs.append(dict(entry, path=str((mounts[slug] / name).relative_to(root))))
    manifest["inputs"] = inputs
    return root, manifest


def run_remote_shared(payload):
    global staging_paths
    attempt_paths = staging_paths
    staging, marker = attempt_paths(payload)
    root, manifest = shared_staging(payload, staging)
    staging_paths = lambda _payload: (root, marker)
    try:
        run_remote(dict(payload, manifest=manifest))
    finally:
        staging_paths = attempt_paths
