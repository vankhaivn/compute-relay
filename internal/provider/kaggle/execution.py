"""Fixed kernel SDK bridge. Generated remote source is data, never executed here."""
import contextlib
import hashlib
import importlib.metadata
import json
import os
import re
import sys
import threading

MAX_REQUEST = 3 << 20
MAX_SOURCE = 2 << 20
ROOT = "https://api.kaggle.com/v1/"
OPERATIONS = {
    "auth": "security.OAuthService/IntrospectToken",
    "quota": "kernels.KernelsApiService/GetAcceleratorQuotaStatistics",
    "get": "kernels.KernelsApiService/GetKernel",
    "save": "kernels.KernelsApiService/SaveKernel",
    "status": "kernels.KernelsApiService/GetKernelSessionStatus",
}
STATES = ("QUEUED", "RUNNING", "COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED", "NEW_SCRIPT")


def outcome(status):
    return dict(protocol=1, status=status, kernel_id="", reference="", version=0,
                source_sha256="", raw_state="")


def strict_json(raw):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate field")
            result[key] = value
        return result
    def invalid(_):
        raise ValueError("invalid JSON number")
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid)
    if not isinstance(result, dict):
        raise ValueError("expected object")
    return result


def read_json_response(response):
    if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise ValueError("invalid response type")
    declared = response.headers.get("Content-Length")
    if declared is not None and (not declared.isdecimal() or int(declared) > MAX_REQUEST):
        raise ValueError("response over limit")
    data = bytearray()
    for chunk in response.iter_content(chunk_size=65536):
        if len(chunk) > MAX_REQUEST - len(data):
            raise ValueError("response over limit")
        data.extend(chunk)
    parsed = strict_json(data)
    response._content = bytes(data)
    response._content_consumed = True
    return parsed


def missing_kernel_error(exc):
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
            and error.get("message") == "Permission 'kernels.get' was denied")


def identifier(value):
    if type(value) not in (int, str):
        raise ValueError("invalid numeric identity")
    text = str(value)
    if not re.fullmatch(r"[1-9][0-9]{0,18}", text) or int(text) > (1 << 63) - 1:
        raise ValueError("invalid numeric identity")
    return text


def validate_request(r, mode):
    if set(r) != {"protocol", "owner", "slug", "source", "source_sha256", "dataset", "wall_seconds", "internet", "gpu", "machine_shape", "kernel_id"}:
        raise ValueError("invalid request fields")
    if type(r["protocol"]) is not int or r["protocol"] != 1 or type(r["wall_seconds"]) is not int or not 1 <= r["wall_seconds"] <= 86400:
        raise ValueError("invalid protocol or budget")
    if type(r["internet"]) is not bool or type(r["gpu"]) is not bool:
        raise ValueError("invalid resource flags")
    for key in ("owner", "slug", "source", "source_sha256", "dataset", "machine_shape", "kernel_id"):
        if type(r[key]) is not str:
            raise ValueError("invalid text field")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,49}", r["owner"]) or not re.fullmatch(r"cre-[a-f0-9]{40}", r["slug"]):
        raise ValueError("invalid resource name")
    if not re.fullmatch(re.escape(r["owner"]) + r"/crs-[a-f0-9]{40}", r["dataset"]) or not re.fullmatch(r"[a-f0-9]{64}", r["source_sha256"]):
        raise ValueError("invalid frozen identity")
    if r["gpu"]:
        if r["machine_shape"] not in ("NvidiaTeslaT4", "NvidiaTeslaP100"):
            raise ValueError("explicit GPU shape required")
    elif r["machine_shape"]:
        raise ValueError("CPU request cannot select a GPU")
    if mode == "submit":
        if not r["source"] or len(r["source"].encode("utf-8")) > MAX_SOURCE or hashlib.sha256(r["source"].encode("utf-8")).hexdigest() != r["source_sha256"] or r["kernel_id"]:
            raise ValueError("invalid source")
    elif r["source"]:
        raise ValueError("read-only call must not supply source")
    if mode == "observe":
        identifier(r["kernel_id"])
    elif r["kernel_id"]:
        raise ValueError("unexpected kernel ID")
    return r


class IdentityMismatch(ValueError):
    pass


class Guard:
    def __init__(self, client, mode):
        import requests
        self.session = getattr(client.http_client(), "_session", None)
        if type(self.session) is not requests.Session:
            raise ValueError("unsupported SDK transport layout")
        self.session.trust_env = False
        self.session.cookies.clear()
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
        self.session.send = self.send
        self.mode = mode
        self.expected = None
        self.count = 0
        self.save_sent = False
        self.last = None

    def call(self, operation, method, request):
        if self.expected is not None or operation not in OPERATIONS:
            raise ValueError("invalid request sequence")
        if operation == "save" and (self.mode != "submit" or self.save_sent):
            raise ValueError("mutation not authorized")
        self.expected = operation
        self.last = None
        try:
            return method(request)
        finally:
            self.expected = None

    def send(self, request, **kwargs):
        operation = self.expected
        self.expected = None  # one wire call per public method; no hidden retry
        if operation not in OPERATIONS or self.count >= 10 or request.method != "POST" or request.url != ROOT + OPERATIONS[operation]:
            raise ValueError("unexpected provider request")
        self.count += 1
        if operation == "save":
            if self.mode != "submit" or self.save_sent:
                raise ValueError("duplicate kernel save")
            self.save_sent = True  # any subsequent failure is ambiguous
        response = self.session.get_adapter(request.url).send(
            request, timeout=(5, 10), stream=True, proxies={}, verify=True, cert=None)
        try:
            if not 200 <= response.status_code < 300:
                # Preserve only the bounded structured GetKernel 403 so absence can be
                # classified after HTTPError. Other error bodies remain unread.
                if operation == "get" and response.status_code == 403:
                    self.last = read_json_response(response)
                response.raise_for_status()
                raise ValueError("redirect rejected")
            self.last = read_json_response(response)
            return response
        finally:
            response.close()


def check_kernel(raw, r, expected_id=""):
    metadata, blob = raw.get("metadata"), raw.get("blob")
    if type(metadata) is not dict or type(blob) is not dict or type(blob.get("source")) is not str:
        raise IdentityMismatch("missing kernel identity")
    try:
        kernel_id = identifier(metadata.get("id"))
    except ValueError as exc:
        raise IdentityMismatch("missing numeric identity") from exc
    reference = r["owner"] + "/" + r["slug"]
    expected = {"ref": reference, "author": r["owner"], "slug": r["slug"],
                "language": "python", "kernelType": "script", "currentVersionNumber": 1,
                "isPrivate": True, "enableGpu": r["gpu"], "enableTpu": False,
                "enableInternet": r["internet"], "datasetDataSources": [r["dataset"]],
                "kernelDataSources": [], "competitionDataSources": [], "modelDataSources": []}
    for key, value in expected.items():
        if type(metadata.get(key)) is not type(value) or metadata[key] != value:
            raise IdentityMismatch("kernel metadata mismatch")
    if expected_id and kernel_id != expected_id:
        raise IdentityMismatch("kernel replacement")
    if r["gpu"] and metadata.get("machineShape") != r["machine_shape"]:
        raise IdentityMismatch("accelerator mismatch")
    if hashlib.sha256(blob["source"].encode("utf-8")).hexdigest() != r["source_sha256"]:
        raise IdentityMismatch("kernel source mismatch")
    return kernel_id


def normalize_status(raw):
    value = raw.get("status") if type(raw) is dict else None
    if type(value) is int and 0 <= value < len(STATES):
        return STATES[value]
    if type(value) is str and value.upper() in STATES:
        return value.upper()
    return "UNKNOWN"  # absence must not inherit the SDK's default QUEUED


def operate(r, token, mode):
    import requests
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import (
        ApiGetKernelRequest, ApiSaveKernelRequest, ApiGetKernelSessionStatusRequest,
        ApiGetAcceleratorQuotaStatisticsRequest)
    from kagglesdk.kernels.types.kernels_enums import KernelExecutionType
    guard = None
    try:
        with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
            guard = Guard(client, mode)
            auth = IntrospectTokenRequest()
            auth.token = token
            identity = guard.call("auth", client.security.oauth_client.introspect_token, auth)
            if type(guard.last.get("active")) is not bool or guard.last["active"] is not True or identity.active is not True or identity.username != r["owner"]:
                return outcome("rejected" if mode == "submit" else "unknown")
            api = client.kernels.kernels_api_client

            def get(version, expected_id=""):
                query = ApiGetKernelRequest()
                query.user_name, query.kernel_slug = r["owner"], r["slug"]
                if version:
                    query.version_label = version
                guard.call("get", api.get_kernel, query)
                return check_kernel(guard.last, r, expected_id)

            try:
                current_id = get("", r["kernel_id"])
            except requests.exceptions.HTTPError as exc:
                # Kaggle reports an absent kernel as either 404 or one precise
                # kernels.get PERMISSION_DENIED payload. Other 403s remain failures.
                if not missing_kernel_error(exc):
                    raise
                if mode != "submit":
                    return outcome("not_found")
                if r["gpu"]:
                    # Match ApiAcceleratorQuota/ProtoJSON: an omitted implicit
                    # bool is false, but explicit null/non-booleans/true block.
                    # This rechecks free-only policy, not numeric scheduling.
                    guard.call("quota", api.get_accelerator_quota_statistics, ApiGetAcceleratorQuotaStatisticsRequest())
                    quota = guard.last.get("gpuQuota")
                    if type(quota) is not dict or quota.get("isPayToScaleEnabled", False) is not False:
                        return outcome("rejected")
                save = ApiSaveKernelRequest()
                save.slug = r["owner"] + "/" + r["slug"]
                save.new_title, save.text = r["slug"], r["source"]
                save.language, save.kernel_type = "python", "script"
                save.is_private = True
                save.enable_gpu, save.enable_tpu = r["gpu"], False
                save.enable_internet = r["internet"]
                save.dataset_data_sources = [r["dataset"]]
                save.kernel_data_sources, save.competition_data_sources, save.model_data_sources = [], [], []
                save.session_timeout_seconds = r["wall_seconds"]
                save.kernel_execution_type = KernelExecutionType.SAVE_AND_RUN_ALL
                if r["gpu"]:
                    save.machine_shape = r["machine_shape"]
                guard.call("save", api.save_kernel, save)
                receipt = guard.last
                if receipt.get("ref") != save.slug or type(receipt.get("versionNumber")) is not int or receipt["versionNumber"] != 1 or receipt.get("error") not in (None, ""):
                    return outcome("unknown")
                if any(receipt.get(key) not in (None, []) for key in ("invalidTags", "invalidDatasetSources", "invalidKernelSources", "invalidCompetitionSources", "invalidModelSources")):
                    return outcome("unknown")
                current_id = identifier(receipt.get("kernelId"))
            # An already-existing exact kernel is observed, never updated/saved.
            get("1", current_id)
            status = ApiGetKernelSessionStatusRequest()
            status.user_name, status.kernel_slug, status.version_label = r["owner"], r["slug"], "1"
            try:
                guard.call("status", api.get_kernel_session_status, status)
                raw_state = normalize_status(guard.last)
            except Exception:
                raw_state = "UNKNOWN"
            get("", current_id)  # a manual rerun/replacement during the poll cannot pass
            return dict(outcome("found"), kernel_id=current_id,
                        reference=r["owner"] + "/" + r["slug"], version=1,
                        source_sha256=r["source_sha256"], raw_state=raw_state)
    except IdentityMismatch:
        return outcome("invalid")
    except Exception:
        # Nonzero/HTTP errors after the wire mutation might still have created a
        # version. Never treat an error body as proof of rejection or repeat save.
        return outcome("unknown" if mode != "submit" or guard is None or guard.save_sent else "rejected")


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("submit", "observe", "reconcile"):
        return 2
    mode = sys.argv[1]
    if sys.version_info[:2] != (3, 11) or importlib.metadata.version("kaggle") != "2.2.4" or importlib.metadata.version("kagglesdk") != "0.1.35":
        return 2
    token = sys.stdin.buffer.readline(8194)
    if not token.endswith(b"\n") or not 1 <= len(token) - 1 <= 8192 or any(b < 33 or b > 126 for b in token[:-1]):
        return 2
    line = sys.stdin.buffer.readline(MAX_REQUEST + 2)
    if not line.endswith(b"\n") or len(line) - 1 > MAX_REQUEST or sys.stdin.buffer.read(1):
        return 2
    r = validate_request(strict_json(line), mode)
    result = operate(r, token[:-1].decode("ascii"), mode)
    return result


def bounded_main():
    if len(sys.argv) != 3 or not sys.argv[2].isdecimal() or not 1 <= int(sys.argv[2]) <= 300:
        return 2
    timer = threading.Timer(int(sys.argv[2]), lambda: os._exit(3))
    timer.daemon = True
    timer.start()
    try:
        with open(os.devnull, "w", encoding="utf-8") as quiet:
            with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
                try:
                    result = main()
                except Exception:
                    return 2
        if type(result) is not dict:
            return result
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(bounded_main())
