"""Fixed one-shot helper. No CLI parser, workload, browser or mutation entry point."""
import contextlib
import importlib.metadata
import json
import os
import re
import sys
import threading

PROTOCOL = 1
MAX_TOKEN = 8192
MAX_RESPONSE = 65536
ACCOUNT = re.compile(r"[a-z0-9][a-z0-9_-]{1,49}\Z")
URLS = (
    "https://api.kaggle.com/v1/security.OAuthService/IntrospectToken",
    "https://api.kaggle.com/v1/kernels.KernelsApiService/GetAcceleratorQuotaStatistics",
)


def baseline(mode):
    return dict(protocol=PROTOCOL, mode=mode, local="ready",
                authentication="not_checked", account_binding="not_checked",
                quota="not_checked", batch_ready=False, problem="none")


def local_check(mode):
    report = baseline(mode)
    try:
        exact = (sys.version_info[:2] == (3, 11)
                 and importlib.metadata.version("kaggle") == "2.2.4"
                 and importlib.metadata.version("kagglesdk") == "0.1.35")
        if not exact:
            report.update(local="version_mismatch", problem="version_mismatch")
    except Exception:
        report.update(local="unavailable", problem="unavailable")
    return report


def harden_session(client):
    # Version-bound SDK v0.1.35 seam: public http_client(), private session field.
    # Fail closed if the reviewed layout changes. Service calls and wire marshalling
    # stay in the official SDK; the hook only restricts its requests transport.
    import requests
    session = getattr(client.http_client(), "_session", None)
    if type(session) is not requests.Session:
        raise ValueError("unrecognized SDK session")
    session.trust_env = False
    session.max_redirects = 0
    session.cookies.clear()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
    count = 0

    def send(request, **kwargs):
        nonlocal count
        if count >= len(URLS) or request.method != "POST" or request.url != URLS[count]:
            raise ValueError("unexpected preflight request")
        count += 1
        # Explicit trust settings; never forward proxy/cert/redirect overrides.
        # Invoke the requests adapter directly: Session.send can pre-read a
        # redirect body even with allow_redirects=False. The adapter neither
        # follows redirects nor consumes a response before this byte guard.
        response = session.get_adapter(request.url).send(
            request, timeout=(5, 10), stream=True, proxies={}, verify=True, cert=None)
        try:
            if not 200 <= response.status_code < 300:
                response.raise_for_status()
                raise ValueError("redirect rejected")
            if response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                raise ValueError("unexpected content type")
            declared = response.headers.get("Content-Length")
            if declared is not None and (not declared.isdecimal() or int(declared) > MAX_RESPONSE):
                raise ValueError("oversized response")
            data = bytearray()
            for chunk in response.iter_content(chunk_size=16384):
                if len(chunk) > MAX_RESPONSE - len(data):
                    raise ValueError("oversized response")
                data.extend(chunk)
            def unique(items):
                result = {}
                for key, value in items:
                    if key in result:
                        raise ValueError("duplicate response field")
                    result[key] = value
                return result
            def invalid_constant(_):
                raise ValueError("invalid JSON constant")
            parsed = json.loads(data.decode("utf-8"), object_pairs_hook=unique,
                                parse_constant=invalid_constant)
            if not isinstance(parsed, dict):
                raise ValueError("expected response object")
            # Requests' own Response cache is populated only after bounded EOF.
            # The SDK reads it through its ordinary response parser after return.
            response._content = bytes(data)
            response._content_consumed = True
            return response
        finally:
            response.close()

    session.send = send


def read_only(token, account):
    report = baseline("read_only")
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import ApiGetAcceleratorQuotaStatisticsRequest
    import requests
    try:
        # A nonempty explicit token prevents SDK environment/file auth fallback.
        # PROD is fixed; do not import kaggle, whose top-level import authenticates.
        with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token) as client:
            harden_session(client)
            request = IntrospectTokenRequest()
            request.token = token
            identity = client.security.oauth_client.introspect_token(request)
            if identity.active is not True:
                return dict(report, authentication="failed", problem="credential_rejected")
            username = identity.username
            if not isinstance(username, str) or not ACCOUNT.fullmatch(username):
                return dict(report, authentication="unavailable", problem="invalid_response")
            report["authentication"] = "verified"
            if username != account:
                return dict(report, account_binding="mismatch", problem="account_mismatch")
            report["account_binding"] = "matched"
            try:
                quota = client.kernels.kernels_api_client.get_accelerator_quota_statistics(
                    ApiGetAcceleratorQuotaStatisticsRequest())
                # Only endpoint/data availability here. Precision, units, age and
                # scheduler integration belong to M4-04, not an invented allowance.
                report["quota"] = "unknown" if quota.gpu_quota is None else "available"
            except Exception:
                report.update(quota="unavailable", problem="quota_unavailable")
            return report
    except requests.exceptions.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 401:
            return dict(report, authentication="failed", problem="credential_rejected")
        problem = "access_denied" if status == 403 else "provider_unavailable"
    except Exception:
        problem = "provider_unavailable"
    return dict(baseline("read_only"), authentication="unavailable", problem=problem)


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("local", "read_only") or not ACCOUNT.fullmatch(sys.argv[2]):
        return 2
    mode, account = sys.argv[1:]
    report = local_check(mode)
    # Keep diagnostics out of the machine protocol; exception text can contain a
    # token, provider body or host path. The parent never reflects raw output either.
    with open(os.devnull, "w", encoding="utf-8") as quiet:
        with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
            if mode == "read_only" and report["local"] == "ready":
                raw = sys.stdin.buffer.read(MAX_TOKEN + 1)
                if not raw or len(raw) > MAX_TOKEN or any(b < 33 or b > 126 for b in raw):
                    report.update(authentication="failed", problem="credential_unavailable")
                else:
                    try:
                        report = read_only(raw.decode("ascii"), account)
                    except Exception:
                        report.update(authentication="unavailable", problem="provider_unavailable")
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


def bounded_main(seconds=25):
    # Independent wall limit: a dead Go parent must not leave the fixed helper
    # waiting indefinitely on stdin or a dribbling provider connection.
    timer = threading.Timer(seconds, lambda: os._exit(3))
    timer.daemon = True
    timer.start()
    try:
        return main()
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(bounded_main())
