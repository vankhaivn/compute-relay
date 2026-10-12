#!/usr/bin/env python3
"""Download only the immutable runtime selected by runtime.lock.json."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import urllib.parse
import urllib.request

from workload import Failure, read_json, require, verify_file


class HTTPSRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        require(urllib.parse.urlsplit(new_url).scheme == "https", "insecure_download_redirect")
        return super().redirect_request(request, response, code, message, headers, new_url)


def fetch(output, lock):
    expected = lock["archive"]
    if output.exists():
        verify_file(output, expected, 200 << 20)
        return
    require(urllib.parse.urlsplit(expected["url"]).scheme == "https", "insecure_download_url")
    require(0 < expected["bytes"] <= 200 << 20, "runtime_size_limit")
    partial = output.with_name(output.name + ".partial")
    # An interrupted file remains evidence; never overwrite or silently resume it.
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), HTTPSRedirect())
    started, total, digest = time.monotonic(), 0, hashlib.sha256()
    with os.fdopen(descriptor, "wb") as target:
        with opener.open(expected["url"], timeout=30) as response:
            require(response.status == 200, "runtime_download_failed")
            while data := response.read(1 << 20):
                require(time.monotonic() - started <= 300, "runtime_download_timeout")
                total += len(data)
                require(total <= expected["bytes"], "runtime_size_limit")
                digest.update(data)
                target.write(data)
        require(total == expected["bytes"] and digest.hexdigest() == expected["sha256"], "runtime_digest_mismatch")
        target.flush()
        os.fsync(target.fileno())
    os.link(partial, output)
    partial.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        lock = read_json(Path(__file__).with_name("runtime.lock.json"))
        fetch(args.output, lock)
        print(json.dumps({"runtime_version": lock["source_version"],
                          "bytes": lock["archive"]["bytes"], "sha256": lock["archive"]["sha256"]}))
        return 0
    except Failure as exc:
        print(str(exc), file=sys.stderr)
    except Exception:
        print("runtime_download_failed; preserve any partial file", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
