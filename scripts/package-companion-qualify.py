"""Credential-free native qualification. Run with the installed bundle's Python -I -B."""
from __future__ import annotations

import argparse
import http.client
import importlib.metadata
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--scratch", required=True, help="new private scratch directory, outside bundle")
    args = parser.parse_args()
    bundle = Path(args.bundle).resolve()
    scratch = Path(args.scratch).resolve()
    if scratch.is_relative_to(bundle) or bundle.is_relative_to(scratch):
        raise RuntimeError("bundle and scratch must be separate")
    scratch.mkdir(mode=0o700)
    (scratch / "home").mkdir(mode=0o700)
    (scratch / "tmp").mkdir(mode=0o700)
    env = {"PATH": "/usr/bin:/bin", "HOME": str(scratch / "home"), "TMPDIR": str(scratch / "tmp"),
           "LANG": "C", "PYTHONDONTWRITEBYTECODE": "1", "KAGGLE_CONFIG_DIR": str(scratch / "home")}

    def run(*command, json_output=True):
        result = subprocess.run([str(part) for part in command], cwd=scratch, env=env,
                                capture_output=True, timeout=30, check=False)
        if result.returncode:
            # Do not reflect stdout/stderr that could later contain a token.
            raise RuntimeError("qualification child command failed: " + Path(command[0]).name)
        return json.loads(result.stdout) if json_output else result.stdout.decode()

    discovery = run(bundle / "bin/companionpack", "discover", "--root", bundle)
    relay = discovery["executable"]
    python = discovery["managed_python"]
    if Path(sys.executable).resolve() != Path(python).resolve():
        raise RuntimeError("qualification must run with this bundle's Python")
    if sys.version_info[:3] != (3, 11, 16) or Path(sys.prefix).resolve() != bundle / "client/python":
        raise RuntimeError("interpreter is not the relocated bundle")
    expected = json.loads((bundle / "share/python-dependencies.json").read_text())["packages"]
    wanted = sorted((p["name"].lower(), p["version"]) for p in expected)
    actual = sorted((p.metadata["Name"].lower(), p.version) for p in importlib.metadata.distributions())
    if wanted != actual:
        raise RuntimeError("installed package inventory differs from manifest")
    # Match runtime helpers, including -I without -B. Their imports must not add
    # unmanifested bytecode or otherwise modify the installed payload.
    run(python, "-I", "-c", "import ssl,ctypes,sqlite3,bz2,lzma,requests,kagglesdk; ssl.create_default_context()", json_output=False)
    version = run(python, "-I", "-B", "-m", "kaggle", "--version", json_output=False)
    if "2.2.4" not in version:
        raise RuntimeError("Kaggle local version mismatch")
    root = scratch / "state"
    initial = run(relay, "init", "--root", root)
    run(relay, "workspace", "create", "--root", root, "--id", "companion-check")
    token_file = root / "read-token"
    run(relay, "token", "issue", "--root", root, "--workspace", "companion-check", "--scope", "read", "--output", token_file)
    token = token_file.read_text().strip()

    def lifecycle(managed):
        command = [relay, "serve", "--root", str(root), "--listen", "127.0.0.1:0"]
        if managed:
            command += ["--managed-python", python, "--managed-machine-shape", "NvidiaTeslaT4",
                        "--managed-max-wall-seconds", "1800", "--managed-max-workers", "2"]
        with (scratch / ("managed.stderr" if managed else "local.stderr")).open("xb") as diagnostic:
            process = subprocess.Popen(command, cwd=scratch, env=env, stdout=subprocess.PIPE, stderr=diagnostic)
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(process.stdout, selectors.EVENT_READ)
                    if not ready.select(20):
                        raise RuntimeError("runtime did not announce readiness")
                announcement = json.loads(process.stdout.readline(4096))
                host, port = announcement["address"].rsplit(":", 1)
                if host != "127.0.0.1":
                    raise RuntimeError("runtime did not use literal loopback")
                connection = http.client.HTTPConnection(host, int(port), timeout=5)
                connection.request("GET", "/readyz", headers={"Authorization": "Bearer " + token})
                response = connection.getresponse()
                body = response.read(65537)
                mode = response.getheader("X-Compute-Relay-Mode")
                connection.close()
                if response.status != 200 or len(body) > 65536:
                    raise RuntimeError("authenticated readiness failed")
                if managed and mode not in ("managed-workers", "managed-connections"):
                    raise RuntimeError("managed mode absent")
                if managed:
                    connection = http.client.HTTPConnection(host, int(port), timeout=5)
                    connection.request("GET", "/v1/info", headers={"Authorization": "Bearer " + token})
                    response = connection.getresponse()
                    info = response.read(65537)
                    connection.close()
                    if response.status != 200 or len(info) > 65536 or not {"managed_connections", "attempt_authorization"}.issubset(json.loads(info)["features"]):
                        raise RuntimeError("managed API features absent")
                if not managed and (mode != "local-admission-only" or announcement["dispatch_enabled"]):
                    raise RuntimeError("safe default changed")
                process.send_signal(signal.SIGINT)
                if process.wait(timeout=20):
                    raise RuntimeError("graceful shutdown failed")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                process.stdout.close()
        reopened = run(relay, "state", "--root", root)
        if reopened["installation_id"] != initial["installation_id"]:
            raise RuntimeError("restart changed installation identity")
        return mode

    modes = [lifecycle(False), lifecycle(True)]
    # Validate that checks did not mutate or add caches to the installed payload.
    run(bundle / "bin/companionpack", "discover", "--root", bundle)
    print(json.dumps({"qualified": True, "platform": discovery["manifest"]["build"]["platform"],
                      "build": discovery["manifest"]["build"], "python_packages": len(actual),
                      "modes": modes, "provider_accounts": 0, "provider_calls": 0,
                      "compute_attempts": 0, "path": env["PATH"]}, indent=2))


if __name__ == "__main__":
    main()
