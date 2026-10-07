"""Account discovery and quota only; no configured account or mutation entry point."""
import contextlib
import json
import os
import sys
import threading

# Go injects only the fixed preflight transport and existing quota decoder.
# Their command entry points never run in these separate module namespaces.


def empty(status):
    return dict(protocol=1, status=status, account="", quota_status="",
                quota_reason="", limit_ns="", used_ns="", reserved_ns="")


def verified(account, quota):
    return dict(empty("verified"), account=account, quota_status=quota["status"],
                quota_reason=quota["reason"], limit_ns=quota["limit_ns"],
                used_ns=quota["used_ns"], reserved_ns=quota["reserved_ns"])


def discover(token):
    from kagglesdk.kaggle_client import KaggleClient
    from kagglesdk.kaggle_env import KaggleEnv
    from kagglesdk.security.types.oauth_service import IntrospectTokenRequest
    from kagglesdk.kernels.types.kernels_api_service import ApiGetAcceleratorQuotaStatisticsRequest
    import requests
    latest = []

    def capture(response):
        # The SDK's documented constructor callback sees only bodies already
        # bounded and strict-JSON checked by the fixed preflight transport.
        latest[:] = [response.json()]

    try:
        with KaggleClient(env=KaggleEnv.PROD, verbose=False, api_token=token,
                          response_processor=capture) as client:
            preflight.harden_session(client)
            request = IntrospectTokenRequest()
            request.token = token
            identity = client.security.oauth_client.introspect_token(request)
            raw = latest[0]
            if "active" in raw and type(raw["active"]) is not bool:
                return empty("invalid_response")
            if raw.get("active") is not True or identity.active is not True:
                return empty("credential_rejected")
            account = raw.get("username")
            if (type(account) is not str or not preflight.ACCOUNT.fullmatch(account)
                    or identity.username != account or token in account):
                return empty("invalid_response")
            try:
                client.kernels.kernels_api_client.get_accelerator_quota_statistics(
                    ApiGetAcceleratorQuotaStatisticsRequest())
                quota = quota_decoder.quota_result(latest[0])
            except Exception:
                quota = quota_decoder.empty("unavailable", "read_unavailable")
            return verified(account, quota)
    except requests.exceptions.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return empty("credential_rejected" if status == 401 else "unavailable")
    except (ValueError, TypeError):
        return empty("invalid_response")
    except Exception:
        return empty("unavailable")


def main():
    if len(sys.argv) != 1:
        return empty("invalid_response")
    if preflight.local_check("local")["local"] != "ready":
        return empty("unavailable")
    raw = sys.stdin.buffer.read(preflight.MAX_TOKEN + 1)
    if not raw or len(raw) > preflight.MAX_TOKEN or any(b < 33 or b > 126 for b in raw):
        return empty("credential_rejected")
    return discover(raw.decode("ascii"))


def bounded_main(seconds=25):
    timer = threading.Timer(seconds, lambda: os._exit(3))
    timer.daemon = True
    timer.start()
    try:
        with open(os.devnull, "w", encoding="utf-8") as quiet:
            with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
                try:
                    result = main()
                except Exception:
                    result = empty("unavailable")
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(bounded_main())
