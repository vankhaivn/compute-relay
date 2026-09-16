"""Read-only artifact bridge. The launcher injects fixed core and contract modules."""
import contextlib
import hashlib
import importlib.metadata
import io
import json
import os
import sys
import threading

API = "https://api.kaggle.com/v1/"
AUTH = "security.OAuthService/IntrospectToken"
KERNEL = "kernels.KernelsApiService/"
DOWNLOAD = KERNEL + "DownloadKernelOutput"
ALLOWED = {AUTH, KERNEL + "GetKernel", KERNEL + "GetKernelSessionStatus",
           KERNEL + "ListKernelSessionOutput", DOWNLOAD}
MAX_REQUEST = 128 << 10
MAX_RESPONSE = 3 << 20


def chunks(response, limit):
    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
        raise ValueError("encoded artifact response")
    declared = response.headers.get("Content-Length")
    if declared is not None and (not declared.isascii() or not declared.isdecimal() or int(declared) > limit):
        raise ValueError("invalid response size")
    size = 0
    for data in response.iter_content(chunk_size=65536):
        if len(data) > limit - size:
            raise ValueError("response exceeds bound")
        size += len(data)
        yield data
    if declared is not None and size != int(declared):
        raise ValueError("response differs from declared size")


class Guard:
    def __init__(self, client, token):
        import requests
        self.requests = requests
        self.session = getattr(client.http_client(), "_session", None)
        if type(self.session) is not requests.Session:
            raise ValueError("SDK transport changed")
        self.session.trust_env = False
        self.session.cookies.clear()
        self.session.headers["Accept-Encoding"] = "identity"
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        self.cloud = requests.adapters.HTTPAdapter(max_retries=0)
        self.session.send = self.send
        self.token, self.expected, self.calls, self.last = token, None, 0, None

    def close(self):
        self.cloud.close()

    def call(self, operation, method, request):
        if operation not in ALLOWED or self.expected is not None or self.calls >= 300:
            raise ValueError("operation outside collection scope")
        self.expected, self.last = API + operation, None
        self.calls += 1
        try:
            result = method(request)
            if self.expected is not None:
                raise ValueError("SDK did not issue the expected read")
            return result
        finally:
            self.expected = None

    def send(self, request, **kwargs):
        if request.method != "POST" or request.url != self.expected or request.headers.get("Authorization") != "Bearer " + self.token:
            raise ValueError("unexpected account request")
        self.expected = None
        download = request.url == API + DOWNLOAD
        response = self.session.get_adapter(request.url).send(
            request, stream=True, timeout=(5, 30), proxies={}, verify=True, cert=None)
        if download and response.status_code in (302, 303, 307):
            code, url = response.status_code, response.headers.get("Location", "")
            response.close()
            clean = self.requests.Response()
            clean.status_code = code
            clean.headers = {"Location": contract.signed_url(url), "Content-Type": "application/octet-stream"}
            clean._content, clean._content_consumed = b"", True
            return clean
        if download and response.status_code == 200:
            if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/octet-stream":
                return response
            response.close()
            raise ValueError("unexpected artifact content type")
        try:
            if response.status_code != 200:
                response.raise_for_status()
                raise ValueError("redirect or unexpected HTTP result")
            if download or response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                raise ValueError("unexpected metadata content type")
            raw = b"".join(chunks(response, MAX_RESPONSE))
            self.last = contract.strict_json(raw)
            response._content, response._content_consumed = raw, True
            return response
        finally:
            response.close()

    def storage(self, url):
        request = self.requests.Request("GET", contract.signed_url(url), headers={"Accept-Encoding": "identity"}).prepare()
        if "Authorization" in request.headers or "Cookie" in request.headers:
            raise ValueError("account credential entered storage request")
        response = self.cloud.send(request, stream=True, timeout=(5, 30), proxies={}, verify=True, cert=None)
        if response.status_code != 200:
            response.close()
            raise ValueError("storage error; redirects and retries forbidden")
        return response


def validate_request(r, mode):
    if type(r) is not dict or set(r) != {"protocol", "execution", "identity", "outputs", "max_files", "max_bytes", "target"}:
        raise ValueError("invalid artifact request")
    if type(r["protocol"]) is not int or r["protocol"] != 1:
        raise ValueError("invalid artifact protocol")
    core.validate_request(r["execution"], "observe")
    identity = r["identity"]
    keys = {"job_id", "attempt_id", "attempt_nonce", "bundle_sha256", "input_manifest_sha256"}
    if type(identity) is not dict or set(identity) != keys or any(type(v) is not str or not 1 <= len(v) <= 256 for v in identity.values()):
        raise ValueError("invalid attempt identity")
    for key in ("bundle_sha256", "input_manifest_sha256"):
        if not contract.DIGEST.fullmatch(identity[key]):
            raise ValueError("invalid input identity")
    contract.declarations_valid(r["outputs"])
    if type(r["max_files"]) is not int or not 1 <= r["max_files"] <= contract.MAX_FILES or type(r["max_bytes"]) is not int or not 1 <= r["max_bytes"] <= contract.MAX_BYTES:
        raise ValueError("invalid artifact budget")
    if mode == "catalog":
        if r["target"] is not None:
            raise ValueError("catalog cannot carry a transfer target")
    elif mode == "fetch":
        f = r["target"]
        if type(f) is not dict or set(f) != {"path", "bytes", "sha256"}:
            raise ValueError("invalid pinned target")
        contract.entry(f["path"], f["bytes"], f["sha256"])
        if f["bytes"] > r["max_bytes"] or not target_allowed(r, f["path"]):
            raise ValueError("target outside approved outputs")
        if f["path"] in contract.CONTROL_LIMITS and f["bytes"] > contract.CONTROL_LIMITS[f["path"]]:
            raise ValueError("control file exceeds bound")
    else:
        raise ValueError("invalid read-only mode")
    return r


def target_allowed(r, path):
    if path in contract.CONTROL_LIMITS:
        return True
    if not path.startswith("outputs/"):
        return False
    rel = path[len("outputs/"):]
    return any(d["kind"] == "file" and rel == d["path"] or
               d["kind"] == "directory" and rel.startswith(d["path"] + "/") for d in r["outputs"])


def operate(r, token, mode, sink):
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import (
        ApiGetKernelRequest, ApiGetKernelSessionStatusRequest,
        ApiListKernelSessionOutputRequest, ApiDownloadKernelOutputRequest)
    execution = r["execution"]
    with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
        guard = Guard(client, token)
        try:
            auth = IntrospectTokenRequest()
            auth.token = token
            identity = guard.call(AUTH, client.security.oauth_client.introspect_token, auth)
            if type(guard.last.get("active")) is not bool or guard.last["active"] is not True or identity.active is not True or identity.username != execution["owner"]:
                raise ValueError("account identity not verified")
            api = client.kernels.kernels_api_client

            def check(version):
                query = ApiGetKernelRequest()
                query.user_name, query.kernel_slug = execution["owner"], execution["slug"]
                if version:
                    query.version_label = version
                guard.call(KERNEL + "GetKernel", api.get_kernel, query)
                core.check_kernel(guard.last, execution, execution["kernel_id"])

            def terminal():
                query = ApiGetKernelSessionStatusRequest()
                query.user_name, query.kernel_slug, query.version_label = execution["owner"], execution["slug"], "1"
                guard.call(KERNEL + "GetKernelSessionStatus", api.get_kernel_session_status, query)
                if core.normalize_status(guard.last) not in ("COMPLETE", "ERROR"):
                    raise ValueError("collectible termination not established")

            def download(path, limit, destination):
                query = ApiDownloadKernelOutputRequest()
                query.owner_slug, query.kernel_slug = execution["owner"], execution["slug"]
                query.version_number, query.file_path = 1, contract.PREFIX + path
                response = guard.call(DOWNLOAD, api.download_kernel_output, query)
                if response.status_code in (302, 303, 307):
                    url = response.headers["Location"]
                    response.close()
                    response = guard.storage(url)
                size, digest = 0, hashlib.sha256()
                try:
                    for data in chunks(response, limit):
                        size += len(data)
                        digest.update(data)
                        if destination is not None and destination.write(data) != len(data):
                            raise ValueError("incomplete artifact destination")
                finally:
                    response.close()
                return contract.entry(path, size, digest.hexdigest())

            check("")
            check("1")
            terminal()
            listed, cursors, cursor = set(), set(), ""
            for _ in range(contract.MAX_PAGES):
                query = ApiListKernelSessionOutputRequest()
                query.user_name, query.kernel_slug, query.version_label = execution["owner"], execution["slug"], "1"
                query.page_size = 100
                if cursor:
                    query.page_token = cursor
                guard.call(KERNEL + "ListKernelSessionOutput", api.list_kernel_session_output, query)
                page = guard.last
                files = page.get("files")
                if type(files) is not list or len(files) > 100:
                    raise ValueError("missing or oversized output page")
                for f in files:
                    if type(f) is not dict or not contract.safe_path(f.get("fileName")) or f["fileName"] in listed:
                        raise ValueError("invalid or repeated output name")
                    listed.add(f["fileName"])
                    if len(listed) > contract.MAX_LIST_FILES:
                        raise ValueError("output catalog exceeds bound")
                cursor = page.get("nextPageToken", "")
                if cursor is None:
                    cursor = ""
                if type(cursor) is not str or len(cursor) > 2048 or any(ord(c) < 32 or ord(c) > 126 for c in cursor):
                    raise ValueError("invalid provider cursor")
                if not cursor:
                    break
                if not files or cursor in cursors:
                    raise ValueError("cyclic or empty output pagination")
                cursors.add(cursor)
            else:
                raise ValueError("output page limit exceeded")
            contract.collision_free(listed)
            if contract.PREFIX + contract.MANIFEST not in listed:
                raise ValueError("result manifest unavailable")
            raw = io.BytesIO()
            download(contract.MANIFEST, contract.CONTROL_LIMITS[contract.MANIFEST], raw)
            manifest = raw.getvalue()
            selected = contract.select_manifest(manifest, r["identity"], r["outputs"], listed, r["max_bytes"], r["max_files"])
            if mode == "catalog":
                total = sum(f["bytes"] for f in selected)
                for path, cap in contract.CONTROL_LIMITS.items():
                    if path == contract.MANIFEST or contract.PREFIX + path not in listed:
                        continue
                    if len(selected) >= r["max_files"]:
                        raise ValueError("control files exceed file budget")
                    file = download(path, min(cap, r["max_bytes"] - total), None)
                    total += file["bytes"]
                    selected.append(file)
                result = {"protocol": 1, "files": sorted(selected, key=lambda f: f["path"])}
            else:
                target = r["target"]
                if contract.PREFIX + target["path"] not in listed:
                    raise ValueError("pinned artifact missing")
                if target["path"] == contract.MANIFEST:
                    file = contract.entry(target["path"], len(manifest), hashlib.sha256(manifest).hexdigest())
                    if file != target or sink.write(manifest) != len(manifest):
                        raise ValueError("pinned manifest changed")
                else:
                    if target["path"] not in contract.CONTROL_LIMITS and target not in selected:
                        raise ValueError("artifact no longer matches the original manifest pin")
                    file = download(target["path"], target["bytes"], sink)
                    if file != target:
                        raise ValueError("artifact differs from pinned bytes")
                result = None
            terminal()
            check("")
            return result
        finally:
            guard.close()


def main(sink):
    if len(sys.argv) != 3 or sys.argv[1] not in ("catalog", "fetch"):
        return 2
    if sys.version_info[:2] != (3, 11) or importlib.metadata.version("kaggle") != "2.2.4" or importlib.metadata.version("kagglesdk") != "0.1.35":
        return 2
    token = sys.stdin.buffer.readline(8194)
    if not token.endswith(b"\n") or not 1 <= len(token)-1 <= 8192 or any(b < 33 or b > 126 for b in token[:-1]):
        return 2
    line = sys.stdin.buffer.readline(MAX_REQUEST + 2)
    if not line.endswith(b"\n") or len(line)-1 > MAX_REQUEST or sys.stdin.buffer.read(1):
        return 2
    mode = sys.argv[1]
    request = validate_request(contract.strict_json(line), mode)
    result = operate(request, token[:-1].decode("ascii"), mode, sink)
    if mode == "catalog":
        encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) > 8 << 20 or sink.write(encoded) != len(encoded):
            return 2
    sink.flush()
    return 0


def bounded_main():
    if len(sys.argv) != 3 or not sys.argv[2].isdecimal() or not 1 <= int(sys.argv[2]) <= 1800:
        return 2
    timer = threading.Timer(int(sys.argv[2]), lambda: os._exit(3))
    timer.daemon = True
    timer.start()
    sink = sys.stdout.buffer
    try:
        with open(os.devnull, "w", encoding="utf-8") as quiet:
            with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
                try:
                    return main(sink)
                except Exception:
                    return 2
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(bounded_main())
