"""Read-only quota and bounded exact-identity log replay."""
import base64
import importlib.metadata
import json
import os
import re
import sys
import threading
import contextlib
import hashlib
import time
import io

# The Go launcher injects the exact existing execution helper as `core`, in a
# separate module namespace. Tests inject the same module; no remote source runs.
MAX_QUOTA_SECONDS = 366 * 24 * 60 * 60
MAX_LOG_BYTES = 64 << 10
DURATION = re.compile(r"(0|[1-9][0-9]{0,7})(?:\.([0-9]{1,9}))?s\Z")


def empty(status, reason):
    return dict(protocol=1, status=status, reason=reason, limit_ns="", used_ns="",
                reserved_ns="", text_b64="", availability="", truncated=False, replay=False, offset=0, prefix="")


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
    # ApiAcceleratorQuota's implicit bool defaults to false in SDK 0.1.35;
    # ProtoJSON may omit that value. Default only absence, not explicit null
    # or false-like strings/numbers. Duration messages still require evidence.
    if quota.get("isPayToScaleEnabled", False) is not False:
        return empty("unknown", "paid_or_unknown")
    names = ("totalTimeAllowed", "timeUsed", "timeReserved")
    if any(quota.get(name) is None for name in names):
        return empty("unknown", "missing_quota")
    values = [duration_ns(quota[name]) for name in names]
    return dict(empty("known", "none"), limit_ns=str(values[0]),
                used_ns=str(values[1]), reserved_ns=str(values[2]))


# A separate GET transport: never arm or loosen the SDK mutation guard.
class LogStreamGuard:
    def __init__(self, session, owner, slug):
        import requests
        if type(session) is not requests.Session:
            raise ValueError("unsupported stream transport")
        self.session = session
        self.url = "https://api.kaggle.com/v1/kernels/logs/stream/" + owner + "/" + slug
        self.used = False

    def open(self):
        import requests
        if self.used:
            raise ValueError("duplicate stream request")
        self.used = True
        headers = dict(self.session.headers)
        headers.pop("Content-Type", None)
        headers["Accept"] = "text/event-stream, */*"
        request = self.session.prepare_request(requests.Request("GET", self.url, headers=headers))
        if request.method != "GET" or request.url != self.url:
            raise ValueError("unexpected stream request")
        response = self.session.get_adapter(self.url).send(
            request, timeout=(3, 0.5), stream=True, proxies={}, verify=True, cert=None)
        if response.status_code != 200:
            response.close()
            raise ValueError("stream unavailable or redirect")
        return response


SECRET = re.compile(r"cr1_[A-Za-z0-9_-]{43}|gh[pousr]_[A-Za-z0-9]{20,255}|(?:authorization[=: ]+bearer[ ]+|(?:api[_-]?key|token|password|secret)[=:][ ]*)[^\s\"']{1,512}", re.I)
MAX_INPUT_BYTES = 32 << 20
MAX_EVENT_BYTES = 64 << 10


def stream_result(response, request, state, token):
    # Read raw bounded lines ourselves: requests.iter_lines may buffer an
    # arbitrarily large event before yielding it.
    response.raw.decode_content = True
    offset = request.get("log_offset", 0)
    prefix = request.get("log_prefix", "")
    limit = request.get("log_limit", 100)
    digest = hashlib.sha256()
    consumed, total, events = 0, 0, 0
    output = bytearray()
    pending = ""
    truncated = False
    matched = offset == 0 and (not prefix or prefix == digest.hexdigest())
    deadline = time.monotonic() + 2

    def accept(text):
        nonlocal consumed, matched, truncated
        data = SECRET.sub("[REDACTED]", text.replace(token, "[REDACTED]")).encode("utf-8")
        if consumed >= offset and len(output) + len(data) > MAX_LOG_BYTES:
            truncated = True
            return False
        digest.update(data)
        consumed += len(data)
        if consumed <= offset:
            if consumed == offset:
                if digest.hexdigest() != prefix:
                    raise core.IdentityMismatch("log replay changed")
                matched = True
            return True
        if not matched:
            raise core.IdentityMismatch("log replay boundary changed")
        output.extend(data)
        return output.count(b"\n") < limit

    sse = (response.headers.get("Content-Type") or "").lower().startswith("text/event-stream")
    if not sse:
        body = response.raw.read(MAX_INPUT_BYTES + 1)
        if len(body) > MAX_INPUT_BYTES:
            raise ValueError("oversized completed log")
        parsed = core.strict_json(b'{"events":' + body + b'}')["events"]
        events_blob = parsed if type(parsed) is list else [parsed]
        if len(events_blob) > 100000 or any(type(event) is not dict or type(event.get("data")) is not str for event in events_blob):
            raise ValueError("invalid completed log events")
        wire_blob = b"".join(b"data: " + json.dumps(event).encode("utf-8") + b"\n\n" for event in events_blob)
        response = type("Replay", (), {"headers": {"Content-Type": "text/event-stream"}, "raw": io.BytesIO(wire_blob)})()
        return stream_result(response, request, state, token)
    wire = bytearray()
    ended = False
    def wire_lines():
        nonlocal total, ended
        reader = getattr(response.raw, "read1", None)
        while time.monotonic() < deadline and total < MAX_INPUT_BYTES:
            chunk = reader(min(4096, MAX_INPUT_BYTES - total)) if reader else response.raw.read(1)
            if not chunk:
                ended = True
                return
            total += len(chunk)
            wire.extend(chunk)
            while b"\n" in wire:
                line, remainder = wire.split(b"\n", 1)
                if len(line) > MAX_EVENT_BYTES:
                    raise ValueError("oversized log event")
                wire[:] = remainder
                yield line.decode("utf-8").rstrip("\r")
            if len(wire) > MAX_EVENT_BYTES:
                raise ValueError("oversized log event")

    try:
        for line in wire_lines():
            if events >= 100000:
                break
            if sse:
                if not line or line.startswith(":") or line.startswith(("event:", "id:", "retry:")):
                    continue
                if not line.startswith("data:"):
                    raise ValueError("malformed SSE")
                payload = line[5:].lstrip()
                if payload == "END_OF_LOG":
                    if pending:
                        accept(pending + "\n")
                        pending = ""
                    wire.clear()
                    ended = True
                    break
                event = core.strict_json(payload.encode("utf-8"))
                if type(event) is not dict or type(event.get("data")) is not str:
                    raise ValueError("invalid log event")
                text = event["data"]
                events += 1
            else:
                # Completed blobs are handled separately below, with a fixed cap.
                text = line + "\n"
            pending += text.replace("\r\n", "\n").replace("\r", "\n")
            if len(pending.encode("utf-8")) > MAX_EVENT_BYTES:
                raise ValueError("oversized log line")
            stop = False
            while "\n" in pending:
                line, pending = pending.split("\n", 1)
                if not accept(line + "\n"):
                    stop = True
                    break
            if stop:
                break
    except Exception as exc:
        if not isinstance(exc, (TimeoutError, OSError)) and not (type(exc).__module__.startswith("urllib3.") and type(exc).__name__ in ("ReadTimeoutError", "ProtocolError")):
            raise
        # An idle or interrupted stream is never completion evidence.
    if ended and wire:
        raise ValueError("unterminated SSE event")
    if ended and pending:
        accept(pending + "\n")
    if not matched:
        if ended:
            return empty("reset", "log_changed")
        return empty("unavailable", "read_unavailable")
    truncated |= total >= MAX_INPUT_BYTES or events >= 100000
    availability = "after_completion" if state in ("COMPLETE", "ERROR") else "live"
    if availability == "after_completion" and not ended and not output:
        # A timed-out empty read is not log exhaustion, even after execution ends.
        availability = "delayed"
    return dict(empty("logs", "none"), text_b64=base64.b64encode(output).decode("ascii"),
                availability=availability, truncated=truncated, replay=True,
                offset=consumed, prefix=digest.hexdigest())


def validate_request(request, mode):
    if type(request) is not dict or not {"protocol", "owner", "execution"} <= set(request) or set(request) - {"protocol", "owner", "execution", "log_offset", "log_prefix", "log_limit"}:
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
    if mode == "logs":
        offset, prefix, limit = request.get("log_offset", 0), request.get("log_prefix", ""), request.get("log_limit", 100)
        if type(offset) is not int or not 0 <= offset <= MAX_INPUT_BYTES or type(prefix) is not str or (prefix and not re.fullmatch(r"[a-f0-9]{64}", prefix)) or (offset and not prefix) or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("invalid replay bounds")
    return request


def operate(request, token, mode):
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import (
        ApiGetAcceleratorQuotaStatisticsRequest, ApiGetKernelRequest,
        ApiGetKernelSessionStatusRequest)
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
            def check():
                query = ApiGetKernelRequest()
                query.user_name, query.kernel_slug = r["owner"], r["slug"]
                guard.call("get", api.get_kernel, query)
                core.check_kernel(guard.last, r, r["kernel_id"])
            check()
            status = core.kernel_status_request(ApiGetKernelSessionStatusRequest, r["owner"], r["slug"])
            try:
                guard.call("status", api.get_kernel_session_status, status)
                state = core.normalize_status(guard.last)
            except Exception:
                state = "UNKNOWN"
            response = LogStreamGuard(guard.session, r["owner"], r["slug"]).open()
            try:
                result = stream_result(response, request, state, token)
            except core.IdentityMismatch:
                result = empty("reset", "log_changed")
            finally:
                response.close()
            check()  # never release log bytes after a resource/source/privacy change
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
    timer = threading.Timer(9 if sys.argv[-1] == "logs" else 25, lambda: os._exit(3))
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
