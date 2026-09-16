"""Read-only quota and version-bound log snapshots; no mutation or live SSE."""
import base64
import importlib.metadata
import json
import os
import re
import sys
import threading
import contextlib

# The Go launcher injects the exact existing execution helper as `core`, in a
# separate module namespace. Tests inject the same module; no remote source runs.
MAX_QUOTA_SECONDS = 366 * 24 * 60 * 60
MAX_LOG_BYTES = 64 << 10
DURATION = re.compile(r"(0|[1-9][0-9]{0,7})(?:\.([0-9]{1,9}))?s\Z")


def empty(status, reason):
    return dict(protocol=1, status=status, reason=reason, limit_ns="", used_ns="",
                reserved_ns="", text_b64="", availability="", truncated=False)


def duration_ns(value):
    # Inspect original protobuf-duration text before timedelta loses nanoseconds.
    if type(value) is not str:
        raise ValueError("duration must be explicit protobuf text")
    match = DURATION.fullmatch(value)
    if match is None:
        raise ValueError("invalid duration")
    seconds, fraction = match.groups()
    ns = int(seconds) * 1000000000 + int((fraction or "").ljust(9, "0"))
    if ns > MAX_QUOTA_SECONDS * 1000000000:
        raise ValueError("quota duration outside reviewed bound")
    return ns


def quota_result(raw):
    quota = raw.get("gpuQuota")
    if quota is None:
        return empty("unknown", "missing_quota")
    if type(quota) is not dict:
        raise ValueError("invalid quota object")
    # Never let an omitted SDK boolean default authorize free-only capacity.
    if type(quota.get("isPayToScaleEnabled")) is not bool or quota["isPayToScaleEnabled"]:
        return empty("unknown", "paid_or_unknown")
    names = ("totalTimeAllowed", "timeUsed", "timeReserved")
    if any(quota.get(name) is None for name in names):
        return empty("unknown", "missing_quota")
    values = [duration_ns(quota[name]) for name in names]
    return dict(empty("known", "none"), limit_ns=str(values[0]),
                used_ns=str(values[1]), reserved_ns=str(values[2]))


def log_result(raw, state, token):
    # An absent/null field differs from an explicitly empty log. Never reflect
    # artifact URLs, arbitrary diagnostics or SDK defaults as provider log bytes.
    if raw.get("log") is None:
        return empty("unavailable", "missing_log")
    text = raw["log"]
    if type(text) is not str:
        raise ValueError("invalid log field")
    text = text.replace(token, "[REDACTED]")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    data = text.encode("utf-8")  # reject unpaired surrogates rather than replacing
    truncated = len(data) > MAX_LOG_BYTES
    if truncated:
        data = data[:MAX_LOG_BYTES].decode("utf-8", errors="ignore").encode("utf-8")
    availability = "after_completion" if state in ("COMPLETE", "ERROR") else "delayed"
    return dict(empty("logs", "none"), text_b64=base64.b64encode(data).decode("ascii"),
                availability=availability, truncated=truncated)


def validate_request(request, mode):
    if type(request) is not dict or set(request) != {"protocol", "owner", "execution"}:
        raise ValueError("invalid monitor request")
    if type(request["protocol"]) is not int or request["protocol"] != 1:
        raise ValueError("invalid monitor protocol")
    owner = request["owner"]
    if type(owner) is not str or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,49}", owner):
        raise ValueError("invalid account")
    if mode == "quota":
        if request["execution"] is not None:
            raise ValueError("quota cannot select a kernel")
    elif mode == "logs":
        core.validate_request(request["execution"], "observe")
        if request["execution"]["owner"] != owner:
            raise ValueError("account mismatch")
    else:
        raise ValueError("invalid read-only mode")
    return request


def operate(request, token, mode):
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import (
        ApiGetAcceleratorQuotaStatisticsRequest, ApiGetKernelRequest,
        ApiGetKernelSessionStatusRequest, ApiListKernelSessionOutputRequest)
    # Extend only the isolated read-only child's dictionary. The execution helper
    # and its mutation paths are unchanged; Guard still rejects every save here.
    core.OPERATIONS["logs"] = "kernels.KernelsApiService/ListKernelSessionOutput"
    try:
        with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
            guard = core.Guard(client, "read_only")
            auth = IntrospectTokenRequest()
            auth.token = token
            identity = guard.call("auth", client.security.oauth_client.introspect_token, auth)
            if type(guard.last.get("active")) is not bool or guard.last["active"] is not True or identity.active is not True or identity.username != request["owner"]:
                return empty("unavailable", "read_unavailable")
            api = client.kernels.kernels_api_client
            if mode == "quota":
                guard.call("quota", api.get_accelerator_quota_statistics, ApiGetAcceleratorQuotaStatisticsRequest())
                return quota_result(guard.last)
            r = request["execution"]
            def check(version):
                query = ApiGetKernelRequest()
                query.user_name, query.kernel_slug = r["owner"], r["slug"]
                if version:
                    query.version_label = version
                guard.call("get", api.get_kernel, query)
                core.check_kernel(guard.last, r, r["kernel_id"])
            check("")
            check("1")
            status = ApiGetKernelSessionStatusRequest()
            status.user_name, status.kernel_slug, status.version_label = r["owner"], r["slug"], "1"
            try:
                guard.call("status", api.get_kernel_session_status, status)
                state = core.normalize_status(guard.last)
            except Exception:
                state = "UNKNOWN"
            query = ApiListKernelSessionOutputRequest()
            query.user_name, query.kernel_slug, query.version_label = r["owner"], r["slug"], "1"
            query.page_size = 1
            guard.call("logs", api.list_kernel_session_output, query)
            result = log_result(guard.last, state, token)
            check("")  # never release log bytes after a resource/source/privacy change
            return result
    except core.IdentityMismatch:
        return empty("invalid", "identity_mismatch")
    except Exception:
        return empty("unavailable", "read_unavailable")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("quota", "logs"):
        return 2
    if sys.version_info[:2] != (3, 11) or importlib.metadata.version("kaggle") != "2.2.4" or importlib.metadata.version("kagglesdk") != "0.1.35":
        return 2
    token = sys.stdin.buffer.readline(8194)
    if not token.endswith(b"\n") or not 1 <= len(token)-1 <= 8192 or any(b < 33 or b > 126 for b in token[:-1]):
        return 2
    line = sys.stdin.buffer.readline(8194)
    if not line.endswith(b"\n") or len(line)-1 > 8192 or sys.stdin.buffer.read(1):
        return 2
    request = validate_request(core.strict_json(line), sys.argv[1])
    return operate(request, token[:-1].decode("ascii"), sys.argv[1])


def bounded_main():
    timer = threading.Timer(25, lambda: os._exit(3))
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
