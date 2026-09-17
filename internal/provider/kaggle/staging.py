"""One fixed staging invocation. No workload, kernel, delete, update or retry path."""
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
DATASET = "datasets.DatasetApiService/"
INTROSPECT = "security.OAuthService/IntrospectToken"
START = "blobs.BlobApiService/StartBlobUpload"
MAX_JSON = 1 << 20
MAX_TOTAL = (4 << 30) + (100 << 20) + (64 << 10)
DIGEST = re.compile(r"[a-f0-9]{64}\Z")
ACCOUNT = re.compile(r"[a-z0-9][a-z0-9_-]{1,49}\Z")
SLUG = re.compile(r"crs-[a-f0-9]{40}\Z")
UPLOAD_COMPLETE = b"\x00compute-relay/staging-upload-complete/v1\n"


class IdentityError(Exception):
    pass


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate field")
            result[key] = value
        return result
    def invalid(_):
        raise ValueError("invalid constant")
    result = json.loads(data.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid)
    if not isinstance(result, dict):
        raise ValueError("object required")
    return result


def pins_ready():
    return (sys.version_info[:2] == (3, 11)
            and importlib.metadata.version("kaggle") == "2.2.4"
            and importlib.metadata.version("kagglesdk") == "0.1.35")


def validate_request(r):
    if set(r) != {"protocol", "owner", "slug", "license", "marker_sha256", "files"}:
        raise ValueError("request fields")
    if type(r["protocol"]) is not int or r["protocol"] != 1:
        raise ValueError("protocol")
    if not ACCOUNT.fullmatch(r["owner"]) or not SLUG.fullmatch(r["slug"]):
        raise ValueError("identity")
    if r["license"] not in ("copyright-authors", "other", "unknown") or not DIGEST.fullmatch(r["marker_sha256"]):
        raise ValueError("license or digest")
    files = r["files"]
    if not isinstance(files, list) or not 2 <= len(files) <= 66:
        raise ValueError("file count")
    names = ["relay-stage.bin", "code.bin"] + [f"input-{i:03d}.bin" for i in range(len(files) - 2)]
    for i, f in enumerate(files):
        if set(f) != {"name", "bytes", "sha256"} or f["name"] != names[i] or not DIGEST.fullmatch(f["sha256"]):
            raise ValueError("file identity")
        limit = 64 << 10 if i == 0 else 100 << 20 if i == 1 else 2 << 30
        if type(f["bytes"]) is not int or not 0 <= f["bytes"] <= limit:
            raise ValueError("file size")
    if files[0]["bytes"] == 0 or files[0]["sha256"] != r["marker_sha256"] or sum(f["bytes"] for f in files) > MAX_TOTAL:
        raise ValueError("manifest size")


def empty_result(status="unknown"):
    return dict(protocol=1, status=status, dataset_id="", owner="", slug="", version=0,
                marker_sha256="", private=False, verified_files=0, verified_bytes=0)


def description(r):
    return "Compute Relay staging " + r["marker_sha256"] + ". Existing licenses remain applicable."


def signed_url(url):
    # An explicit conservative storage host, not a wildcard and not an app URL.
    # Different upstream upload/CDN hosts remain blocked until separately reviewed.
    if not isinstance(url, str) or not 1 <= len(url) <= 16384 or any(ord(c) < 33 or ord(c) > 126 for c in url) or "\\" in url:
        raise ValueError("storage URL")
    p = urlsplit(url)
    if p.scheme != "https" or p.netloc not in ("storage.googleapis.com", "storage.googleapis.com:443") or not p.path.startswith("/") or p.fragment:
        raise ValueError("storage destination")
    return url


def chunks(response, limit):
    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
        raise ValueError("unexpected content encoding")
    declared = response.headers.get("Content-Length")
    if declared is not None and (not declared.isdecimal() or int(declared) > limit):
        raise ValueError("response size")
    size = 0
    for part in response.iter_content(chunk_size=65536):
        size += len(part)
        if size > limit:
            raise ValueError("response overflow")
        yield part


def missing_dataset_error(exc):
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status == 404:
        return True
    if status != 403:
        return False
    data = getattr(response, "_content", None)
    if not isinstance(data, bytes):
        return False
    try:
        payload = strict_json(data)
    except (TypeError, ValueError, UnicodeError):
        return False
    error = payload.get("error")
    return (type(error) is dict and error.get("code") == 403
            and error.get("status") == "PERMISSION_DENIED"
            and error.get("message") == "Permission 'datasets.get' was denied")


class Guard:
    """Official SDK owns wire types; this pinned transport permits one expected call."""
    def __init__(self, client, token, mode, file_count):
        import requests
        self.requests = requests
        self.session = getattr(client.http_client(), "_session", None)
        if type(self.session) is not requests.Session:
            raise ValueError("SDK session layout")
        self.session.trust_env = False
        self.session.cookies.clear()
        self.session.headers["Accept-Encoding"] = "identity"
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        self.cloud = requests.adapters.HTTPAdapter(max_retries=0)
        self.token, self.mode, self.file_count = token, mode, file_count
        self.expected = None
        self.starts = self.creates = self.calls = 0
        self.session.send = self.send

    def close(self):
        self.cloud.close()

    def call(self, name, method, request):
        allowed = {INTROSPECT, START, DATASET + "CreateDataset", DATASET + "GetDataset",
                   DATASET + "GetDatasetStatus", DATASET + "ListDatasetFiles", DATASET + "DownloadDataset"}
        if name not in allowed or self.expected is not None or self.calls >= 300:
            raise ValueError("unexpected SDK operation")
        if name == START:
            if self.mode != "create" or self.starts >= self.file_count:
                raise ValueError("upload retry forbidden")
            self.starts += 1
        if name == DATASET + "CreateDataset":
            if self.mode != "create" or self.creates or self.starts != self.file_count or request.is_private is not True:
                raise ValueError("creation retry or public staging forbidden")
            self.creates += 1
        self.calls += 1
        self.expected = API + name
        try:
            result = method(request)
            if self.expected is not None:
                raise ValueError("SDK call produced no request")
            return result
        finally:
            self.expected = None

    def send(self, request, **kwargs):
        if request.method != "POST" or request.url != self.expected or request.headers.get("Authorization") != "Bearer " + self.token:
            raise ValueError("unexpected request or credential")
        self.expected = None  # A hidden retry cannot reuse this invocation.
        download = request.url == API + DATASET + "DownloadDataset"
        response = self.session.get_adapter(request.url).send(
            request, stream=True, timeout=(5, 30), proxies={}, verify=True, cert=None)
        if download and response.status_code in (302, 303, 307):
            # Preserve only the redirect metadata for HttpRedirect.prepare_from.
            # Do not let SDK error parsing consume the redirect body.
            location = response.headers.get("Location", "")
            response.close()
            clean = self.requests.Response()
            clean.status_code = response.status_code
            clean.headers = {"Location": signed_url(location), "Content-Type": "application/octet-stream"}
            clean._content = b""
            clean._content_consumed = True
            return clean
        if download and response.status_code == 200:
            if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/octet-stream":
                return response  # Caller must stream, hash and close it.
            response.close()
            raise ValueError("unexpected raw download type")
        try:
            if not 200 <= response.status_code < 300:
                # Kaggle uses a structured 403 for a missing dataset on GetDataset.
                # Preserve only that bounded JSON body so the caller can classify it narrowly.
                if (request.url == API + DATASET + "GetDataset" and response.status_code == 403
                        and response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/json"):
                    data = b"".join(chunks(response, MAX_JSON))
                    strict_json(data)
                    response._content, response._content_consumed = data, True
                response.raise_for_status()
                raise ValueError("redirect forbidden")
            if download or response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                raise ValueError("unexpected response type")
            data = b"".join(chunks(response, MAX_JSON))
            strict_json(data)
            response._content, response._content_consumed = data, True
            return response
        finally:
            response.close()

    def storage(self, method, url, body=None, size=None):
        headers = {"Accept-Encoding": "identity"}
        if size is not None:
            headers.update({"Content-Length": str(size), "Content-Type": "application/octet-stream"})
        request = self.requests.Request(method, signed_url(url), headers=headers, data=body).prepare()
        if "Authorization" in request.headers or "Cookie" in request.headers:
            raise ValueError("storage credential leak")
        response = self.cloud.send(request, stream=True, timeout=(5, 30), proxies={}, verify=True, cert=None)
        if not 200 <= response.status_code < 300:
            response.close()
            raise ValueError("storage error; no redirect or retry")
        return response


class UploadBody:
    def __init__(self, source, f):
        self.source, self.file = source, f
        self.remaining = f["bytes"]
        self.hash = hashlib.sha256()
    def __len__(self):
        return self.file["bytes"]
    def read(self, size=-1):
        if not self.remaining:
            return b""
        size = min(self.remaining, 65536, size if size > 0 else 65536)
        data = self.source.read(size)
        if not data or len(data) > size:
            raise ValueError("short or overlong staging stream")
        self.remaining -= len(data)
        self.hash.update(data)
        return data
    def verify(self):
        if self.remaining or self.hash.hexdigest() != self.file["sha256"]:
            raise IdentityError("upload bytes")


def upload(guard, client, r, source):
    from kagglesdk.blobs.types.blob_api_service import ApiBlobType, ApiStartBlobUploadRequest
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile
    files = []
    for f in r["files"]:
        request = ApiStartBlobUploadRequest()
        request.type, request.name = ApiBlobType.DATASET, f["name"]
        request.content_length, request.content_type = f["bytes"], "application/octet-stream"
        ticket = guard.call(START, client.blobs.blob_api_client.start_blob_upload, request)
        if not isinstance(ticket.token, str) or not 1 <= len(ticket.token) <= 8192:
            raise ValueError("upload reference")
        body = UploadBody(source, f)
        response = guard.storage("PUT", ticket.create_url, body if f["bytes"] else b"", f["bytes"])
        try:
            # Success requires actual consumption, hash and bounded acknowledged EOF.
            for _ in chunks(response, 65536):
                pass
            body.verify()
        finally:
            response.close()
        entry = ApiDatasetNewFile()
        entry.token = ticket.token
        files.append(entry)
    require_upload_complete(source)
    return files


def require_upload_complete(source):
    # EOF alone is unsafe: the Go stdin copier also closes its pipe on a source
    # read/Close error. Only the trailer emitted after all source checks grants
    # permission to finish upload and proceed to the one-shot create call.
    remaining = UPLOAD_COMPLETE
    while remaining:
        part = source.read(len(remaining))
        if not part or part != remaining[:len(part)]:
            raise ValueError("source completion not acknowledged")
        remaining = remaining[len(part):]
    if source.read(1) != b"":
        raise ValueError("trailing upload data")


def dataset_call(guard, client, r, request_type, method_name):
    request = request_type()
    request.owner_slug, request.dataset_slug = r["owner"], r["slug"]
    method = getattr(client.datasets.dataset_api_client, method_name)
    names = {"get_dataset": "GetDataset", "get_dataset_status": "GetDatasetStatus"}
    return guard.call(DATASET + names[method_name], method, request)


def matching_dataset(meta, r):
    if (type(meta.id) is not int or not 0 < meta.id < (1 << 63)
            or meta.ref != r["owner"] + "/" + r["slug"]
            or meta.current_version_number != 1 or meta.description != description(r)
            or meta.license_name != r["license"]):
        raise IdentityError("dataset identity or version")
    return meta.id


def observe(guard, client, r, meta=None):
    from kagglesdk.datasets.types.dataset_api_service import (ApiGetDatasetRequest,
        ApiGetDatasetStatusRequest, ApiListDatasetFilesRequest, ApiDownloadDatasetRequest)
    from kagglesdk.datasets.types.dataset_enums import DatabundleVersionStatus as State
    if meta is None:
        meta = dataset_call(guard, client, r, ApiGetDatasetRequest, "get_dataset")
    dataset_id = matching_dataset(meta, r)
    result = dict(empty_result("pending"), dataset_id=str(dataset_id), owner=r["owner"],
                  slug=r["slug"], version=1, marker_sha256=r["marker_sha256"], private=meta.is_private is True)
    if meta.is_private is not True:
        return result  # Durable caller preserves the discovered public identity, never submits.
    state = dataset_call(guard, client, r, ApiGetDatasetStatusRequest, "get_dataset_status").status
    if state in (State.NOT_YET_PERSISTED, State.BLOBS_RECEIVED, State.BLOBS_DECOMPRESSED,
                 State.BLOBS_COPIED_TO_SDS, State.INDIVIDUAL_BLOBS_COMPRESSED):
        return result
    if state != State.READY:
        return empty_result()
    expected = {f["name"]: f for f in r["files"]}
    seen, cursors, cursor = set(), set(), ""
    for _ in range(67):
        request = ApiListDatasetFilesRequest()
        request.owner_slug, request.dataset_slug = r["owner"], r["slug"]
        request.dataset_version_number, request.page_size = 1, 20
        if cursor:
            request.page_token = cursor
        page = guard.call(DATASET + "ListDatasetFiles", client.datasets.dataset_api_client.list_dataset_files, request)
        if page.error_message or not isinstance(page.dataset_files, list) or len(page.dataset_files) > 20:
            raise IdentityError("file listing")
        for f in page.dataset_files:
            if f.name not in expected or f.name in seen or f.total_bytes != expected[f.name]["bytes"]:
                raise IdentityError("file catalog mismatch")
            seen.add(f.name)
        cursor = page.next_page_token
        if not cursor:
            break
        if len(cursor) > 2048 or cursor in cursors:
            raise IdentityError("pagination")
        cursors.add(cursor)
    else:
        raise IdentityError("too many pages")
    if seen != set(expected):
        raise IdentityError("missing staging file")
    for f in r["files"]:
        request = ApiDownloadDatasetRequest()
        request.owner_slug, request.dataset_slug = r["owner"], r["slug"]
        request.dataset_version_number, request.file_name, request.raw = 1, f["name"], True
        response = guard.call(DATASET + "DownloadDataset", client.datasets.dataset_api_client.download_dataset, request)
        if response.status_code in (302, 303, 307):
            url = response.headers["Location"]
            response.close()
            response = guard.storage("GET", url)
        try:
            size, digest = 0, hashlib.sha256()
            for part in chunks(response, f["bytes"]):
                size += len(part)
                digest.update(part)
            if size != f["bytes"] or digest.hexdigest() != f["sha256"]:
                raise IdentityError("download digest")
        finally:
            response.close()
    final = dataset_call(guard, client, r, ApiGetDatasetRequest, "get_dataset")
    final_state = dataset_call(guard, client, r, ApiGetDatasetStatusRequest, "get_dataset_status").status
    if matching_dataset(final, r) != dataset_id or final.is_private is not True or final.last_updated != meta.last_updated or final_state != State.READY:
        raise IdentityError("dataset changed during verification")
    return dict(result, status="ready", verified_files=len(expected), verified_bytes=sum(f["bytes"] for f in expected.values()))


def stage(mode, token, r, source):
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest, ApiCreateDatasetRequest
    import requests
    with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
        guard = Guard(client, token, mode, len(r["files"]))
        try:
            auth = IntrospectTokenRequest()
            auth.token = token
            identity = guard.call(INTROSPECT, client.security.oauth_client.introspect_token, auth)
            if identity.active is not True or identity.username != r["owner"]:
                raise IdentityError("account mismatch")
            try:
                meta = dataset_call(guard, client, r, ApiGetDatasetRequest, "get_dataset")
            except requests.exceptions.HTTPError as exc:
                # Kaggle reports a missing dataset as either 404 or one precise
                # datasets.get PERMISSION_DENIED payload. Other 403s remain failures.
                if not missing_dataset_error(exc):
                    raise
                if mode == "observe":
                    return empty_result("not_found")
                meta = None
            if meta is not None:
                return observe(guard, client, r, meta)  # Never update/adopt a nonmatching resource.
            files = upload(guard, client, r, source)
            create = ApiCreateDatasetRequest()
            create.owner_slug, create.slug, create.title = r["owner"], r["slug"], r["slug"]
            create.license_name, create.description, create.is_private = r["license"], description(r), True
            create.files = files
            receipt = guard.call(DATASET + "CreateDataset", client.datasets.dataset_api_client.create_dataset, create)
            if receipt.error or receipt.invalid_tags:
                return empty_result()
            # No success/readiness claim from the create receipt alone.
            return observe(guard, client, r)
        finally:
            guard.close()


def main():
    result = empty_result()
    try:
        if len(sys.argv) != 3 or sys.argv[1] not in ("create", "observe") or not pins_ready():
            raise ValueError("local precondition")
        token = sys.stdin.buffer.readline(8194)
        if not token.endswith(b"\n") or not 1 <= len(token) - 1 <= 8192 or any(b < 33 or b > 126 for b in token[:-1]):
            raise ValueError("credential framing")
        raw = sys.stdin.buffer.readline(65538)
        if not raw.endswith(b"\n") or len(raw) > 65537:
            raise ValueError("request framing")
        request = strict_json(raw)
        validate_request(request)
        with open(os.devnull, "w", encoding="utf-8") as quiet:
            with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
                result = stage(sys.argv[1], token[:-1].decode("ascii"), request, sys.stdin.buffer)
    except IdentityError:
        result = empty_result("invalid")
    except Exception:
        result = empty_result()
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


def bounded_main():
    try:
        seconds = int(sys.argv[2])
        if not 1 <= seconds <= 3600:
            return 2
    except (IndexError, ValueError):
        return 2
    timer = threading.Timer(seconds, lambda: os._exit(3))
    timer.daemon = True
    timer.start()
    try:
        return main()
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(bounded_main())
