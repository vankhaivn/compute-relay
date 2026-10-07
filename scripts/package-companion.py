"""Build-only native companion assembler; consumes pinned tools without global installs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import urllib.request


PYTHON = "3.11.16"
UV = "0.12.13"
GO = "go1.27.1"
PYTHON_ID = "cpython-3.11.16-macos-aarch64-none"
PYTHON_BUILD = "20260901"
LICENSE_ARCHIVE = (
    "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/"
    "cpython-3.11.16%2B20260901-aarch64-apple-darwin-pgo%2Blto-full.tar.zst"
)
LICENSE_ARCHIVE_SHA = "19b8d2af1719e3fe8cd4edfa2e5030dab776233c05044f4348dbe3c0af7d1b2f"


def run(*args, cwd=None, env=None):
    return subprocess.check_output([str(a) for a in args], cwd=cwd, env=env, timeout=300).decode()


def sha_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_objects(text):
    decoder = json.JSONDecoder()
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        yield value
        text = text.lstrip()[end:]


def source_inventory(repo, env):
    """Hash only compiler inputs and the packaging recipe, with relative paths."""
    paths = {"go.mod", "go.sum", "LICENSE", "scripts/package-companion.py",
             "scripts/package-companion.sh", "scripts/package-companion-qualify.py",
             "tools/kaggle-client/uv.lock", "tools/kaggle-client/pyproject.toml",
             "tools/kaggle-client/.python-version", "tools/kaggle-client/THIRD_PARTY.md",
             "tools/kaggle-client/src/compute_relay_kaggle_probe/dependencies.py"}
    packages = run("go", "list", "-deps", "-json", "./cmd/compute-relay", "./cmd/companionpack", cwd=repo, env=env)
    for package in json_objects(packages):
        if not package.get("Module", {}).get("Main"):
            continue
        directory = Path(package["Dir"])
        for key in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles", "SysoFiles", "EmbedFiles"):
            for name in package.get(key, []):
                paths.add((directory / name).relative_to(repo).as_posix())
    if len(paths) > 5000:
        raise RuntimeError("compiler input inventory exceeds bound")
    entries = []
    for name in sorted(paths):
        source = repo / name
        if source.is_symlink() or not source.is_file() or source.stat().st_size > 16 * 1024 * 1024:
            raise RuntimeError("invalid compiler input")
        entries.append({"path": name, "sha256": sha_file(source)})
    raw = json.dumps(entries, separators=(",", ":"), sort_keys=True).encode()
    return entries, hashlib.sha256(raw).hexdigest()


def copy_python(source, destination):
    # uv's standalone installation is relocatable. Reject an unexpected external
    # link before materializing internal links; never copy virtual environments.
    for item in source.rglob("*"):
        if item.is_symlink() and not item.resolve().is_relative_to(source.resolve()):
            raise RuntimeError("standalone Python contains an external link")
    shutil.copytree(source, destination, symlinks=False)
    for item in (destination / "bin").iterdir():
        if item.name != "python3.11":
            item.unlink()  # Discard build-path shebangs and package installers.
    site = destination / "lib/python3.11/site-packages"
    shutil.rmtree(site)
    site.mkdir()
    return site


def download_licenses(work, share, cached):
    archive = Path(cached).resolve() if cached else work / "python-full.tar.zst"
    if not cached:
        with urllib.request.urlopen(LICENSE_ARCHIVE, timeout=60) as response, archive.open("xb") as out:
            count = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                count += len(chunk)
                if count > 64 * 1024 * 1024:
                    raise RuntimeError("Python license source exceeds download bound")
                out.write(chunk)
    if sha_file(archive) != LICENSE_ARCHIVE_SHA:
        raise RuntimeError("Python license source checksum mismatch")
    members = run("tar", "-tf", archive).splitlines()
    selected = [name for name in members if name.startswith("python/licenses/") and not name.endswith("/")]
    selected += ["python/PYTHON.json"]
    if len(selected) < 2 or len(selected) > 100:
        raise RuntimeError("missing Python distribution license notices")
    target = share / "licenses/python-build-standalone"
    target.mkdir(parents=True)
    for name in selected:
        # Read named bytes from the checksum-pinned upstream archive; no extraction.
        data = subprocess.check_output(["tar", "-xOf", str(archive), name], timeout=60)
        if len(data) > 4 * 1024 * 1024:
            raise RuntimeError("oversized Python notice")
        (target / Path(name).name).write_bytes(data)
    (target / "source.json").write_text(json.dumps({"url": LICENSE_ARCHIVE, "sha256": LICENSE_ARCHIVE_SHA}, indent=2) + "\n")


def go_notices(repo, share, env):
    modules = list(json_objects(run("go", "list", "-m", "-json", "all", cwd=repo, env=env)))
    linked = set()
    for binary in ("compute-relay", "companionpack"):
        info = json.loads(run("go", "version", "-m", "-json", share.parent / "bin" / binary, env=env))
        linked.update(m["Path"] for m in info.get("Deps", []))
    records = []
    for module in modules:
        if module["Path"] not in linked:
            continue
        if module.get("Replace"):
            raise RuntimeError("local/replaced Go modules are not distributable")
        root = Path(module["Dir"])
        names = sorted(p for p in root.rglob("*") if p.is_file() and
                       p.name.lower().startswith(("license", "copying", "notice", "copyright")))
        if not names:
            raise RuntimeError("Go module license notice missing: " + module["Path"])
        target = share / "licenses/go" / (module["Path"] + "@" + module["Version"])
        licenses = []
        for source in names:
            relative = source.relative_to(root)
            output = target / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, output)
            licenses.append(output.relative_to(share).as_posix())
        records.append({"module": module["Path"], "version": module["Version"], "sum": module.get("Sum"), "license_files": licenses})
    goroot = Path(run("go", "env", "GOROOT", cwd=repo, env=env).strip())
    target = share / "licenses/go-toolchain"
    target.mkdir()
    for name in ("LICENSE", "PATENTS"):
        if (goroot / name).is_file():
            shutil.copyfile(goroot / name, target / name)
    (share / "go-dependencies.json").write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="new output directory outside the checkout")
    parser.add_argument("--python-install", help="optional existing pinned uv standalone installation")
    parser.add_argument("--python-license-archive", help="optional checksum-pinned upstream full archive")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    output = Path(args.output).resolve()
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError("only native darwin-arm64 is qualified by this recipe")
    if output.is_relative_to(repo):
        raise RuntimeError("raw package output must be outside the checkout")
    if run("uv", "--version").split()[1] != UV or run("go", "env", "GOVERSION", cwd=repo).strip() != GO:
        raise RuntimeError("builder needs uv 0.12.13 and Go 1.27.1")
    go_root = Path(run("go", "env", "GOROOT", cwd=repo).strip())
    if (repo / "tools/kaggle-client/.python-version").read_text().strip() != PYTHON:
        raise RuntimeError("Python pin disagrees with client lock")
    output.mkdir(mode=0o700)  # Every build owns a new directory and never overwrites.
    work = Path(tempfile.mkdtemp(prefix="companion-build-", dir=output))
    stage = work / "payload"
    (stage / "bin").mkdir(parents=True)
    share = stage / "share"
    share.mkdir()
    (work / "home").mkdir(mode=0o700)
    (work / "tmp").mkdir(mode=0o700)
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(work / "home"),
           "TMPDIR": str(work / "tmp"), "LANG": "C", "CGO_ENABLED": "0", "GOOS": "darwin", "GOARCH": "arm64",
           "GOTOOLCHAIN": "local", "GOFLAGS": "-mod=readonly", "GOWORK": "off", "UV_NO_CONFIG": "1", "UV_NO_PROGRESS": "1",
           "GOMODCACHE": run("go", "env", "GOMODCACHE", cwd=repo).strip(),
           "GOCACHE": run("go", "env", "GOCACHE", cwd=repo).strip(),
           "UV_CACHE_DIR": run("uv", "cache", "dir").strip()}
    # The user's bootstrap `go` may select the pinned downloaded toolchain. Invoke
    # that exact toolchain directly after selection, with no automatic fallback.
    env["PATH"] = str(go_root / "bin") + os.pathsep + env.get("PATH", "")
    env["GOROOT"] = str(go_root)
    run("go", "mod", "verify", cwd=repo, env=env)
    before, source_hash = source_inventory(repo, env)
    revision = run("git", "rev-parse", "HEAD", cwd=repo).strip()
    dirty = bool(run("git", "status", "--porcelain", "--untracked-files=normal", cwd=repo))
    build_time = run("git", "show", "-s", "--format=%cI", "HEAD", cwd=repo).strip()
    version = "companion-candidate." + source_hash[:12] if dirty else "companion." + revision[:12]
    identity = "github.com/vankhaivn/compute-relay/internal/buildinfo."
    flags = "-buildid= -X " + identity + "Version=" + version + " -X " + identity + "Commit=" + revision + " -X " + identity + "BuiltAt=" + build_time
    for name in ("compute-relay", "companionpack"):
        run("go", "build", "-trimpath", "-buildvcs=false", "-ldflags=" + flags, "-o", stage / "bin" / name, "./cmd/" + name, cwd=repo, env=env)
    source = Path(args.python_install).resolve() if args.python_install else work / "standalone" / PYTHON_ID
    if not args.python_install:
        run("uv", "python", "install", "--no-bin", "--no-registry", "--install-dir", source.parent, PYTHON_ID, env=env)
    if (source / "BUILD").read_text().strip() != PYTHON_BUILD:
        raise RuntimeError("standalone Python build revision mismatch")
    site = copy_python(source, stage / "client/python")
    python = stage / "client/python/bin/python3.11"
    if run(python, "-I", "-c", "import platform; print(platform.python_version())").strip() != PYTHON:
        raise RuntimeError("standalone Python patch mismatch")
    project = repo / "tools/kaggle-client"
    requirements = share / "requirements.txt"
    run("uv", "export", "--locked", "--no-dev", "--no-emit-project", "--no-annotate", "--no-header",
        "--no-python-downloads", "--python", python, "--output-file", requirements, cwd=project, env=env)
    run("uv", "pip", "install", "--python", python, "--target", site, "--no-deps", "--require-hashes",
        "--only-binary", ":all:", "--no-python-downloads", "--link-mode", "copy", "-r", requirements, env=env)
    # uv's generated console scripts embed the build interpreter. Runtime uses
    # python modules, so none of those checkout/path-specific launchers are shipped.
    if (site / "bin").exists():
        shutil.rmtree(site / "bin")
    inventory_script = project / "src/compute_relay_kaggle_probe/dependencies.py"
    inventory = json.loads(run(python, "-I", "-B", "-c",
        "import json,runpy,sys; print(json.dumps(runpy.run_path(sys.argv[1])['collect_dependency_inventory']()))", inventory_script))
    locked = json.loads(run(python, "-I", "-B", "-c",
        "import json,tomllib,sys; print(json.dumps({p['name']:p['version'] for p in tomllib.load(open(sys.argv[1],'rb'))['package']}))", project / "uv.lock"))
    for package in inventory["packages"]:
        name = package["name"].lower().replace("_", "-")
        if locked.get(name) != package["version"]:
            raise RuntimeError("installed Python distribution differs from lock: " + name)
    (share / "python-dependencies.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    for name in ("uv.lock", "pyproject.toml", "THIRD_PARTY.md"):
        shutil.copyfile(project / name, share / name)
    shutil.copyfile(repo / "LICENSE", share / "LICENSE")
    shutil.copyfile(repo / "scripts/package-companion-qualify.py", share / "qualify.py")
    download_licenses(work, share, args.python_license_archive)
    go_notices(repo, share, env)
    (share / "source-files.json").write_text(json.dumps(before, indent=2, sort_keys=True) + "\n")
    # Replace build-host caches with reproducible checked-hash bytecode. Helpers
    # use Python -I, which ignores PYTHONDONTWRITEBYTECODE; shipping valid caches
    # prevents ordinary imports from changing the verified installed payload.
    for cache in list(stage.rglob("__pycache__")):
        shutil.rmtree(cache)
    for bytecode in list(stage.rglob("*.pyc")):
        bytecode.unlink()
    run(python, "-I", "-B", "-c",
        "import compileall,py_compile,sys; ok=compileall.compile_dir(sys.argv[1],quiet=1,force=True,stripdir=sys.argv[1],invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH); raise SystemExit(0 if ok else 1)", stage)
    after, after_hash = source_inventory(repo, env)
    if after != before or after_hash != source_hash or run("git", "rev-parse", "HEAD", cwd=repo).strip() != revision:
        raise RuntimeError("compiler inputs changed during build; retain this attempt and rebuild from stable sources")
    build = {"platform": "darwin-arm64", "revision": revision, "source_sha256": source_hash,
             "worktree_modified": dirty, "go_version": GO, "python_version": PYTHON,
             "python_distribution": PYTHON_ID, "uv_version": UV, "kaggle_version": "2.2.4", "kagglesdk_version": "0.1.35"}
    metadata = work / "build.json"
    metadata.write_text(json.dumps(build, indent=2) + "\n")
    name = "compute-relay-companion-darwin-arm64-" + revision[:12] + ("-candidate-" + source_hash[:12] if dirty else "")
    archive = output / (name + ".tar.gz")
    packer = stage / "bin/companionpack"
    packed = json.loads(run(packer, "pack", "--source", stage, "--output", archive, "--metadata", metadata))
    installer = output / "companionpack-darwin-arm64"
    shutil.copyfile(packer, installer)
    installer.chmod(0o755)
    (output / (archive.name + ".sha256")).write_text(packed["sha256"] + "  " + archive.name + "\n")
    (output / (installer.name + ".sha256")).write_text(sha_file(installer) + "  " + installer.name + "\n")
    print(json.dumps({"archive": str(archive), "sha256": packed["sha256"], "installer": str(installer), "build": build}, indent=2))


if __name__ == "__main__":
    main()
