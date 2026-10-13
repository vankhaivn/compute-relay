"""One explicit name-based delete after original identity checks; recovery only reads."""
import contextlib
import hashlib
import importlib.metadata
import json
import os
import re
import sys
import threading
from urllib.parse import urlsplit

API = "https://api.kaggle.com/v1/"
AUTH = "security.OAuthService/IntrospectToken"
KERNEL = "kernels.KernelsApiService/"
DATASET = "datasets.DatasetApiService/"
READS = {AUTH, KERNEL + "GetKernel", KERNEL + "GetKernelSessionStatus", DATASET + "GetDataset", DATASET + "DownloadDataset"}
DELETES = {KERNEL + "DeleteKernel", DATASET + "DeleteDataset"}


def result(status):
    return dict(protocol=1, status=status)


def validate_request(r, mode):
    if mode not in ("preview", "apply", "recover") or set(r) != {"protocol", "owner", "slug", "kind", "id", "sha256", "accept_name_based_delete"}:
        raise ValueError("invalid cleanup request")
    if type(r["protocol"]) is not int or r["protocol"] != 1 or type(r["accept_name_based_delete"]) is not bool:
        raise ValueError("invalid cleanup protocol")
    if r["kind"] not in ("kernel", "dataset"):
        raise ValueError("invalid kind")
    for key in ("owner", "slug", "id", "sha256"):
        if type(r[key]) is not str:
            raise ValueError("invalid identity")
    prefix = "cre" if r["kind"] == "kernel" else "crs"
    if (not re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,49}", r["owner"])
            or not re.fullmatch(prefix + r"-[a-f0-9]{40}", r["slug"])
            or not re.fullmatch(r"[a-f0-9]{64}", r["sha256"])):
        raise ValueError("invalid original identity")
    core.identifier(r["id"])
    if mode == "apply" and not r["accept_name_based_delete"]:
        raise ValueError("name-based deletion not accepted")
    return r


def marker_bytes(response):
    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
        raise ValueError("encoded marker")
    declared = response.headers.get("Content-Length")
    if declared is not None and (not declared.isdecimal() or int(declared) > 65536):
        raise ValueError("marker too large")
    data = bytearray()
    for chunk in response.iter_content(chunk_size=65536):
        if len(chunk) > 65536 - len(data):
            raise ValueError("marker too large")
        data.extend(chunk)
    if declared is not None and int(declared) != len(data):
        raise ValueError("incomplete marker")
    return bytes(data)


class Guard:
    def __init__(self, client, token, mode):
        import requests
        self.requests = requests
        self.session = getattr(client.http_client(), "_session", None)
        if type(self.session) is not requests.Session:
            raise ValueError("unsupported SDK session")
        self.session.trust_env = False
        self.session.cookies.clear()
        self.session.headers["Accept-Encoding"] = "identity"
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        self.cloud = requests.adapters.HTTPAdapter(max_retries=0)
        self.session.send = self.send
        self.token, self.mode = token, mode
        self.expected, self.last = None, None
        self.calls, self.delete_sent = 0, False
        self.marker = None

    def close(self):
        self.cloud.close()

    def call(self, operation, method, request):
        if self.expected is not None or operation not in READS | DELETES or self.calls >= 12:
            raise ValueError("unexpected operation")
        if operation in DELETES and (self.mode != "apply" or self.delete_sent):
            raise ValueError("delete not authorized")
        self.expected, self.last = API + operation, None
        self.calls += 1
        try:
            value = method(request)
            if self.expected is not None:
                raise ValueError("SDK did not send request")
            return value
        finally:
            self.expected = None

    def send(self, request, **kwargs):
        if self.expected is None or request.url != self.expected or request.method != "POST" or request.headers.get("Authorization") != "Bearer " + self.token:
            raise ValueError("unexpected request")
        self.expected = None
        operation = request.url[len(API):]
        if operation in DELETES:
            if self.mode != "apply" or self.delete_sent:
                raise ValueError("delete retry forbidden")
            self.delete_sent = True  # Transport failure after this point stays unknown.
        response = self.session.get_adapter(request.url).send(request, stream=True, timeout=(5, 10), proxies={}, verify=True, cert=None)
        try:
            if operation == DATASET + "DownloadDataset":
                if response.status_code in (302, 303, 307):
                    location = response.headers.get("Location", "")
                    parsed = urlsplit(location)
                    if (not 1 <= len(location) <= 16384 or any(ord(c) < 33 or ord(c) > 126 for c in location)
                            or "\\" in location or parsed.scheme != "https" or parsed.netloc not in ("storage.googleapis.com", "storage.googleapis.com:443") or parsed.fragment):
                        raise ValueError("untrusted marker redirect")
                    response.close()
                    cloud_request = self.requests.Request("GET", location, headers={"Accept-Encoding": "identity"}).prepare()
                    cloud = self.cloud.send(cloud_request, stream=True, timeout=(5, 10), proxies={}, verify=True, cert=None)
                    try:
                        if cloud.status_code != 200:
                            raise ValueError("marker read failed")
                        self.marker = marker_bytes(cloud)
                    finally:
                        cloud.close()
                elif response.status_code == 200 and response.headers.get("Content-Type", "").split(";", 1)[0].lower() == "application/octet-stream":
                    self.marker = marker_bytes(response)
                else:
                    response.raise_for_status()
                    raise ValueError("invalid marker response")
                # SDK raw downloads return HttpDownload; the verified bytes stay local.
                clean = self.requests.Response()
                clean.status_code = 200
                clean.headers = {"Content-Type": "application/octet-stream"}
                clean._content, clean._content_consumed = self.marker, True
                return clean
            if response.status_code != 200:
                response.raise_for_status()
                raise ValueError("unconfirmed response")
            self.last = core.read_json_response(response)
            return response
        finally:
            response.close()


def check_identity(raw, r):
    meta = raw.get("metadata") if r["kind"] == "kernel" else raw
    if type(meta) is not dict:
        raise core.IdentityMismatch("missing metadata")
    try:
        identity = core.identifier(meta.get("id"))
    except ValueError as exc:
        raise core.IdentityMismatch("invalid ID") from exc
    if identity != r["id"]:
        raise core.IdentityMismatch("replacement")
    expected = {"ref": r["owner"] + "/" + r["slug"], "currentVersionNumber": 1, "isPrivate": True}
    for key, value in expected.items():
        if type(meta.get(key)) is not type(value) or meta[key] != value:
            raise core.IdentityMismatch("identity, version or privacy changed")
    if r["kind"] == "kernel":
        blob = raw.get("blob")
        if meta.get("slug") != r["slug"] or type(blob) is not dict or type(blob.get("source")) is not str or hashlib.sha256(blob["source"].encode()).hexdigest() != r["sha256"]:
            raise core.IdentityMismatch("source changed")
    elif meta.get("description") != "Compute Relay staging " + r["sha256"] + ". Existing licenses remain applicable.":
        raise core.IdentityMismatch("marker changed")


def operate(r, token, mode):
    import requests
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import ApiGetKernelRequest, ApiGetKernelSessionStatusRequest, ApiDeleteKernelRequest
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest, ApiDownloadDatasetRequest, ApiDeleteDatasetRequest
    validate_request(r, mode)
    with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
        guard = Guard(client, token, mode)
        try:
            with contextlib.redirect_stdout(sys.stderr):
                auth = IntrospectTokenRequest()
                auth.token = token
                guard.call(AUTH, client.security.oauth_client.introspect_token, auth)
                if guard.last.get("active") is not True or guard.last.get("username") != r["owner"]:
                    return result("identity_mismatch")
                kernel = r["kind"] == "kernel"
                api = client.kernels.kernels_api_client if kernel else client.datasets.dataset_api_client
                get = ApiGetKernelRequest() if kernel else ApiGetDatasetRequest()
                delete = ApiDeleteKernelRequest() if kernel else ApiDeleteDatasetRequest()
                for query in (get, delete):
                    if kernel:
                        query.user_name, query.kernel_slug = r["owner"], r["slug"]
                    else:
                        query.owner_slug, query.dataset_slug = r["owner"], r["slug"]
                service = KERNEL if kernel else DATASET
                def read():
                    try:
                        guard.call(service + ("GetKernel" if kernel else "GetDataset"), api.get_kernel if kernel else api.get_dataset, get)
                    except requests.exceptions.HTTPError as exc:
                        # Permission denial (including Kaggle's missing-name 403) is
                        # never proof of deletion or absence.
                        if getattr(exc.response, "status_code", None) == 404:
                            return False
                        raise
                    check_identity(guard.last, r)
                    return True
                if not read():
                    return result("absent")
                if kernel:
                    status = core.kernel_status_request(ApiGetKernelSessionStatusRequest, r["owner"], r["slug"])
                    guard.call(KERNEL + "GetKernelSessionStatus", api.get_kernel_session_status, status)
                    if core.normalize_status(guard.last) not in ("COMPLETE", "ERROR"):
                        return result("active")
                else:
                    download = ApiDownloadDatasetRequest()
                    download.owner_slug, download.dataset_slug = r["owner"], r["slug"]
                    download.dataset_version_number, download.file_name, download.raw = 1, "relay-stage.bin", True
                    guard.call(DATASET + "DownloadDataset", api.download_dataset, download)
                    if guard.marker is None or hashlib.sha256(guard.marker).hexdigest() != r["sha256"]:
                        raise core.IdentityMismatch("marker bytes changed")
                if not read():
                    return result("absent")
                if mode == "preview":
                    return result("would_delete")
                if mode == "recover":
                    return result("unknown")
                guard.call(service + ("DeleteKernel" if kernel else "DeleteDataset"), api.delete_kernel if kernel else api.delete_dataset, delete)
                allowed = {"errorMessage"} if kernel else {"error"}
                if set(guard.last) - allowed or any(value not in (None, "") for value in guard.last.values()):
                    return result("unknown")
                return result("unknown" if read() else "deleted")
        except core.IdentityMismatch:
            return result("unknown" if guard.delete_sent else "identity_mismatch")
        except Exception:
            return result("unknown")
        finally:
            guard.close()


def main():
    # Pin check precedes reading credentials; no home/config discovery is allowed.
    if (sys.version_info[:2] != (3, 11) or importlib.metadata.version("kaggle") != "2.2.4" or importlib.metadata.version("kagglesdk") != "0.1.35"):
        return result("unknown")
    token = sys.stdin.buffer.readline(8194)
    if not token.endswith(b"\n") or not 1 <= len(token) - 1 <= 8192:
        return result("unknown")
    token = token[:-1].decode("ascii")
    if any(ord(c) < 33 or ord(c) > 126 for c in token):
        return result("unknown")
    raw = sys.stdin.buffer.readline(16386)
    if not raw.endswith(b"\n") or len(raw) > 16385 or sys.stdin.buffer.read(1):
        return result("unknown")
    return operate(core.strict_json(raw), token, sys.argv[1])


def bounded_main():
    try:
        seconds = int(sys.argv[2])
        if not 1 <= seconds <= 300:
            raise ValueError("invalid deadline")
        watchdog = threading.Timer(seconds, lambda: os._exit(124))
        watchdog.daemon = True
        watchdog.start()
        try:
            value = main()
        finally:
            watchdog.cancel()
    except Exception:
        value = result("unknown")
    print(json.dumps(value, separators=(",", ":")))


if __name__ == "__main__":
    bounded_main()
