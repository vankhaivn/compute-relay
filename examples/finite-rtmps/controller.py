#!/usr/bin/env python3
"""Explicit public-API steps for one finite RTMPS job; never a background retry loop."""

import argparse
import base64
from datetime import datetime, timezone
import getpass
import hashlib
import http.client
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
import urllib.parse
import uuid

from workload import Failure, MAX_MEDIA_BYTES, SCRATCH_RESERVE, digest, epoch, read_json, require, utc, verify_file

HERE = Path(__file__).resolve().parent
FEATURES = {"object_upload", "job_admission", "separate_prepare_start", "ephemeral_workload_secrets",
            "job_logs", "artifact_download", "job_cleanup", "job_retry"}
SETUP_SECONDS, FINALIZATION_SECONDS = 30, 10


def timestamp(value):
    return datetime.fromtimestamp(int(value), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def identifier(value):
    require(isinstance(value, str) and 1 <= len(value) <= 128 and
            all(c.isascii() and (c.isalnum() or c in "_-") for c in value), "invalid_identifier")
    return value


def private_token(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_size <= 16384, "invalid_token_file")
    if os.name == "posix":
        require(stat.S_IMODE(info.st_mode) & 0o077 == 0, "unprotected_token_file")
    value = path.read_text().strip()
    require(value and not any(c.isspace() for c in value), "invalid_token_file")
    return value


class API:
    def __init__(self, url, workspace, token):
        parsed = urllib.parse.urlsplit(url)
        require(parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username and
                not parsed.password and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment, "invalid_relay_url")
        require(parsed.scheme == "https" or parsed.hostname in {"127.0.0.1", "localhost", "::1"}, "relay_transport_requires_tls")
        self.scheme, self.host, self.port = parsed.scheme, parsed.hostname, parsed.port
        self.base = "/v1/workspaces/" + identifier(workspace)
        self.token, self.workspace = token, workspace
        self.last_request = 0

    def request(self, method, route, body=None, key=None, upload=None, expected=(200,)):
        require(route.startswith("/v1/") and "\r" not in route and "\n" not in route, "invalid_api_route")
        time.sleep(max(0, 0.3 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        connection_type = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(self.host, self.port, timeout=40)
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        try:
            if upload:
                path, metadata = upload
                headers.update({"Content-Type": "application/octet-stream", "Content-Length": str(metadata["bytes"]),
                                "X-Content-SHA256": metadata["sha256"]})
                with path.open("rb") as source:
                    connection.request(method, route, body=source, headers=headers)
            else:
                raw = json.dumps(body, separators=(",", ":")).encode() if body is not None else None
                if raw is not None:
                    headers["Content-Type"] = "application/json"
                connection.request(method, route, body=raw, headers=headers)
            response = connection.getresponse()
            raw = response.read((2 << 20) + 1)
            require(len(raw) <= 2 << 20, "oversized_api_response")
            # Do not print error bodies, headers or transport exception messages.
            require(response.status in expected, "api_status_" + str(response.status))
            value = json.loads(raw)
            require(isinstance(value, dict), "invalid_api_response")
            return value
        except (OSError, http.client.HTTPException, ValueError):
            raise Failure("request_uncertain_use_observe" if method != "GET" else "observation_unavailable") from None
        finally:
            connection.close()


class Session:
    def __init__(self, directory):
        self.directory = directory.resolve()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(not directory.is_symlink(), "invalid_session_directory")
        self.path = self.directory / "session.json"
        self.state = read_json(self.path, 2 << 20) if self.path.exists() else {
            "schema_version": 1, "id": uuid.uuid4().hex, "requests": {}, "downloads": {}, "attempts": []}
        require(self.state["schema_version"] == 1, "invalid_session")

    def save(self):
        temporary = self.directory / "session.json.pending"
        with temporary.open("x", encoding="utf-8") as target:
            os.chmod(temporary, 0o600)
            json.dump(self.state, target, indent=2, sort_keys=True)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, self.path)
        if os.name == "posix":
            directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def mutate(self, api, label, route, body, expected=(202,), resupply=False):
        # Secret values are deliberately absent from the application journal too.
        public = {k: v for k, v in body.items() if k != "values"}
        intents = self.state["requests"]
        if label not in intents:
            intents[label] = {"route": route, "body": public, "key": self.state["id"] + ":" + label}
            self.save()
        intent = intents[label]
        require(intent["route"] == route and intent["body"] == public, "changed_request_use_original_intent")
        if "receipt" in intent and not resupply:
            return intent["receipt"]
        receipt = api.request("POST", route, body, intent["key"], expected=expected)
        intent["receipt"] = receipt
        self.save()
        return receipt

    def intent(self, label):
        return self.state["requests"].get(label, {})

    def target(self, api):
        job = identifier(self.state["job_id"])
        attempt = identifier(self.state["attempt_id"])
        return api.base + "/jobs/" + job, attempt


def local_command(args, timeout=120):
    try:
        result = subprocess.run([str(x) for x in args], stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout)
        require(result.returncode == 0 and len(result.stdout) <= 2 << 20, "native_cli_failed_preserve_outputs")
        return json.loads(result.stdout)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        raise Failure("native_cli_uncertain_preserve_outputs") from None


def discover(api, connection_id, wall_seconds):
    require(FEATURES <= set(api.request("GET", "/v1/info")["features"]), "runtime_capability_missing")
    providers = api.request("GET", api.base + "/providers")["providers"]
    ssh = next((p for p in providers if p["type"] == "ssh"), None)
    require(ssh is not None and all(ssh["capabilities"].get(k) == "supported" for k in
            ("absolute_execution_deadline", "separate_prepare_start", "ephemeral_workload_secrets")), "provider_capability_missing")
    connection = api.request("GET", api.base + "/connections/" + identifier(connection_id))
    host, selection = connection.get("host", {}), connection.get("selection", {})
    require(connection["provider_type"] == "ssh" and connection["authentication"] == "verified" and
            connection["new_work"] == "enabled" and host.get("os") == "linux" and host.get("architecture") == "amd64" and
            host.get("image", {}).get("status") == "ready", "connection_not_ready_for_linux_amd64")
    require(selection.get("allow_remote_internet") is True and
            selection["max_remote_wall_seconds"] >= wall_seconds, "connection_policy_prevents_delivery")
    return selection["profile"]


def space_preflight(args, staged_bytes, expanded_bytes):
    require(args.remote_free_bytes is not None and args.space_observed_at, "recent_remote_space_observation_required")
    age = time.time() - epoch(args.space_observed_at)
    require(-5 <= age <= 600, "remote_space_observation_stale")
    required = 2 * staged_bytes + expanded_bytes + SCRATCH_RESERVE
    require(args.remote_free_bytes >= required, "insufficient_remote_space")
    return required


def prepare_inputs(args, api, session):
    state = session.state
    if "plan" not in state:
        require(args.media and args.runtime and args.connection and args.end_at and args.width and args.height,
                "new_session_requires_media_runtime_connection_dimensions_and_end")
        require(SETUP_SECONDS + FINALIZATION_SECONDS < args.wall_seconds <= 86400, "invalid_wall_budget")
        require(epoch(args.end_at) > time.time() + 60, "end_deadline_expired_or_too_close")
        lock = read_json(HERE / "runtime.lock.json")
        verify_file(args.runtime, lock["archive"], 200 << 20)
        media_size, media_hash = digest(args.media, MAX_MEDIA_BYTES)
        ca = None
        if args.ca:
            size, value = digest(args.ca, 1 << 20)
            ca = {"bytes": size, "sha256": value}
        config = {"schema_version": 1, "media": {"bytes": media_size, "sha256": media_hash,
                  "width": args.width, "height": args.height}, "ca": ca, "end_at": args.end_at,
                  "startup_seconds": 30, "stall_seconds": 30, "progress_seconds": 10}
        from workload import validate_config
        validate_config(config)
        # Discover before uploads. No connection mutation or image installation.
        profile = discover(api, args.connection, args.wall_seconds)
        config_path = session.directory / "config.json"
        config_raw = json.dumps(config, sort_keys=True) + "\n"
        if config_path.exists():
            require(config_path.read_text() == config_raw, "existing_configuration_changed")
        else:
            with config_path.open("x", encoding="utf-8") as target:
                target.write(config_raw)
        bundle = session.directory / "bundle.tar.gz"
        if bundle.exists():
            inspected = local_command([args.relay_binary, "bundle", "inspect", "--file", bundle])
            entries = inspected["manifest"]["files"]
            require({e["path"]: e["sha256"] for e in entries} == {
                name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
                for name in ("workload.py", "runtime.lock.json")}, "existing_bundle_changed")
        else:
            local_command([args.relay_binary, "bundle", "create", "--root", HERE, "--include", "workload.py",
                           "--include", "runtime.lock.json", "--output", bundle])
        paths = {"bundle": bundle, "runtime": args.runtime, "media": args.media, "config": config_path}
        if args.ca:
            paths["ca"] = args.ca
        files = {}
        for label, path in paths.items():
            size, value = digest(path, 100 << 20 if label == "bundle" else 2 << 30)
            files[label] = {"path": str(path.resolve()), "bytes": size, "sha256": value}
        require(sum(v["bytes"] for k, v in files.items() if k != "bundle") <= 4 << 30, "aggregate_input_limit")
        staged = sum(v["bytes"] for v in files.values())
        expanded = sum(v["bytes"] for v in lock["members"])
        required = space_preflight(args, staged, expanded)
        state.update(plan={"profile": profile, "connection_id": args.connection, "end_at": args.end_at,
                           "wall_seconds": args.wall_seconds, "staged_bytes": staged, "runtime_expanded_bytes": expanded,
                           "remote_space_required": required}, files=files)
        session.save()
    plan = state["plan"]
    require(not args.end_at or args.end_at == plan["end_at"], "original_end_deadline_cannot_change")
    require(epoch(plan["end_at"]) > time.time(), "end_deadline_expired")
    space_preflight(args, plan["staged_bytes"], plan["runtime_expanded_bytes"])
    for label, item in state["files"].items():
        verify_file(Path(item["path"]), item, 2 << 30)
        if "object" not in item:
            # An uncertain immutable upload may leave an unreferenced object. It
            # cannot start compute; explicit rerun may upload the same digest.
            item["upload_intent"] = True
            session.save()
            receipt = api.request("POST", api.base + "/objects", upload=(Path(item["path"]), item), expected=(201,))
            require(receipt["workspace_id"] == api.workspace and receipt["bytes"] == item["bytes"] and
                    receipt["sha256"] == item["sha256"], "uploaded_object_mismatch")
            item["object"] = receipt
            session.save()
    files = state["files"]
    targets = {"runtime": "runtime.tar.xz", "media": "media.mp4", "config": "config.json", "ca": "ca.pem"}
    spec = {"api_version": "compute-connector/v1alpha1", "name": "finite-rtmps", "profile": plan["profile"],
            "execution_control": "prepare_then_start", "bundle": {"object_id": files["bundle"]["object"]["object_id"]},
            "execution": {"kind": "python", "command": ["python", "workload.py"]},
            "inputs": [{"name": name, "source": {"kind": "object", "object_id": files[name]["object"]["object_id"]},
                        "target": target} for name, target in targets.items() if name in files],
            "outputs": [{"path": "summary.json", "required": False}], "resources": {"accelerator": "cpu"},
            "network": {"remote_internet": "required"},
            "workload_secrets": [{"name": "ingest", "exposure": "file_and_local_process_arguments"}],
            "timeouts": {"remote_wall_seconds": plan["wall_seconds"], "setup_seconds": SETUP_SECONDS,
                         "finalization_grace_seconds": FINALIZATION_SECONDS, "execution_deadline": plan["end_at"]}}
    job = session.mutate(api, "admit", api.base + "/jobs", spec)
    if "job_id" not in state:
        state.update(job_id=job["job_id"], attempt_id=job["attempt_id"])
        state["attempts"].append(job["attempt_id"])
        session.save()


def prepare_current(args, api, session):
    base, attempt = session.target(api)
    plan = session.state["plan"]
    space_preflight(args, plan["staged_bytes"], plan["runtime_expanded_bytes"])
    require(epoch(plan["end_at"]) > time.time() + 60, "end_deadline_expired_or_too_close")
    label = "prepare:" + attempt
    body = session.intent(label).get("body") or {
        "attempt_id": attempt, "max_preparation_seconds": 300,
        "ready_until": timestamp(min(time.time() + 900, epoch(plan["end_at"]))),
        "max_staged_bytes": plan["staged_bytes"], "authorize_private_staging": True}
    session.mutate(api, label, base + "/prepare", body)
    end = time.monotonic() + 330
    while time.monotonic() < end:
        ready = api.request("GET", base + "/readiness")
        session.state["readiness"] = ready
        session.save()
        require(ready["attempt_id"] == attempt, "original_attempt_changed")
        if ready["state"] == "ready":
            return ready
        require(ready["state"] in {"queued", "preparing"}, "readiness_requires_attention")
        time.sleep(2)
    raise Failure("preparation_observation_timeout_use_observe")


def start(args, api, session):
    base, attempt = session.target(api)
    start_label, supply_label = "start:" + attempt, "supply:" + attempt
    original = session.intent(start_label)
    if original:
        # Explicit rerun replays the original grant, without resending secret bytes.
        return session.mutate(api, start_label, base + "/start", original["body"])
    ready = api.request("GET", base + "/readiness")
    require(ready["attempt_id"] == attempt and ready["state"] == "ready", "original_attempt_not_ready")
    plan = session.state["plan"]
    supply = session.intent(supply_label)
    metadata = supply.get("body") or {"attempt_id": attempt, "readiness_revision": ready["revision"],
               "expires_at": timestamp(min(epoch(plan["end_at"]), time.time() + plan["wall_seconds"]))}
    require(epoch(metadata["expires_at"]) > time.time() + SETUP_SECONDS + FINALIZATION_SECONDS, "end_deadline_expired_or_too_close")
    secret_status = ready.get("secrets", {}).get("status")
    require(secret_status in {"missing", "available", "material_lost"}, "secret_authority_unusable")
    if not supply.get("receipt") or secret_status != "available":
        # This read is deliberately after readiness and all non-secret checks.
        require(args.endpoint_stdin or sys.stdin.isatty(), "noninteractive_endpoint_requires_stdin_flag")
        value = sys.stdin.buffer.read(8193) if args.endpoint_stdin else getpass.getpass("RTMPS endpoint (hidden): ").encode()
        value = value.rstrip(b"\r\n")
        require(0 < len(value) <= 8192 and b"\r" not in value and b"\n" not in value, "invalid_endpoint_input")
        receipt = session.mutate(api, supply_label, base + "/workload-secrets",
                                 {**metadata, "values": {"ingest": base64.b64encode(value).decode()}}, resupply=True)
        value = None
    else:
        receipt = supply["receipt"]
    body = {"attempt_id": attempt, "readiness_revision": ready["revision"], "max_remote_wall_seconds": plan["wall_seconds"],
            "execution_deadline": metadata["expires_at"], "authorize_compute": True,
            "secret_authorization_id": receipt["authorization_id"]}
    return session.mutate(api, start_label, base + "/start", body)


def observe(args, api, session):
    base, attempt = session.target(api)
    status = api.request("GET", base)
    ready = api.request("GET", base + "/readiness")
    records, started = [], time.monotonic()
    cursor = session.state.get("log_cursors", {}).get(attempt, "")
    availability = "unavailable"
    try:
        for _ in range(args.pages):
            query = {"attempt_id": attempt, "limit": 100}
            if cursor:
                query["cursor"] = cursor
            page = api.request("GET", base + "/logs?" + urllib.parse.urlencode(query))
            require(page["attempt_id"] == attempt, "log_attempt_mismatch")
            availability = page["availability"]
            if availability == "unavailable":
                break
            for line in page["lines"]:
                try:
                    record = json.loads(line)
                    if isinstance(record, dict) and record.get("schema_version") == 1 and record.get("event") in {"progress", "stopped"}:
                        allowed = {"schema_version", "event", "observed_at", "elapsed_ms", "output_time_us", "frames",
                                   "last_advance_elapsed_ms", "reason", "process_terminated"}
                        records.append({k: v for k, v in record.items() if k in allowed})
                except (ValueError, TypeError):
                    pass
            previous, cursor = cursor, page["next_cursor"]
            session.state.setdefault("log_cursors", {})[attempt] = cursor
            session.save()
            if not page["lines"] or cursor == previous:
                break
    except Failure as exc:
        availability = str(exc)
    return {"observed_at": utc(), "job": status, "readiness": ready, "log_availability": availability,
            "log_read_seconds": round(time.monotonic() - started, 3), "records": records,
            "receiver_health": "not_observed", "note": "Old or unavailable logs do not establish inactivity or receiver failure."}


def settle(api, receipt, timeout=150):
    route = receipt["links"]["self"]
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        current = api.request("GET", route)
        if current["status"] in {"succeeded", "failed", "manual_required"}:
            require(current["status"] == "succeeded", "operation_requires_attention")
            return current
        time.sleep(2)
    raise Failure("operation_observation_timeout_use_observe")


def finish(args, api, session):
    base, attempt = session.target(api)
    job = api.request("GET", base)
    require(job["active_attempt_id"] == attempt and job["state"]["remote_activity"] in {"inactive", "not_started"}, "inactivity_not_confirmed")
    downloads = session.state["downloads"].setdefault(attempt, {})
    if job["state"]["remote_activity"] == "inactive":
        require(job["state"]["result"] == "available", "results_not_available_preserve_original")
        query = urllib.parse.urlencode({"attempt_id": attempt, "limit": 100})
        page = api.request("GET", base + "/artifacts?" + query)
        require(not page.get("next_cursor"), "unexpected_artifact_pagination")
        directory = session.directory / attempt
        directory.mkdir(mode=0o700, exist_ok=True)
        for item in page["artifacts"]:
            artifact_id = identifier(item["artifact_id"])
            if artifact_id in downloads:
                continue
            output = directory / artifact_id
            require(not output.exists(), "download_uncertain_preserve_file")
            # The native public client verifies the mandatory final HTTP trailer.
            # Python's http.client does not expose it, so a hash-only reader is insufficient.
            receipt = local_command([args.relay_binary, "artifact", "download", "--url", args.url,
                "--workspace", args.workspace, "--token-file", args.token_file, "--job", session.state["job_id"],
                "--attempt", attempt, "--id", artifact_id, "--output", output])
            require(receipt.get("delivery") == "verified-new-file" and receipt.get("workspace_id") == api.workspace and
                    receipt.get("job_id") == session.state["job_id"] and receipt.get("attempt_id") == attempt and
                    receipt.get("artifact") == item, "artifact_delivery_not_verified")
            verify_file(output, item, 8 << 20, allow_empty=True)
            downloads[artifact_id] = {"metadata": item, "receipt": receipt, "local_file": str(output)}
            session.save()
    receipt = session.mutate(api, "cleanup:" + attempt, base + "/cleanup", {"attempt_id": attempt})
    result = settle(api, receipt)
    session.state.setdefault("cleanup", {})[attempt] = result
    session.save()
    return result


def continuation(args, api, session):
    base, attempt = session.target(api)
    plan = session.state["plan"]
    require(epoch(plan["end_at"]) > time.time() + 60, "original_end_deadline_expired")
    space_preflight(args, plan["staged_bytes"], plan["runtime_expanded_bytes"])
    label = "retry:" + attempt
    original = session.intent(label)
    if not original:
        job = api.request("GET", base)
        require(job["active_attempt_id"] == attempt and job["state"]["remote_activity"] == "inactive", "inactivity_not_confirmed")
        require(session.state.get("cleanup", {}).get(attempt, {}).get("status") == "succeeded", "finish_original_attempt_first")
        require(job["state"]["execution"] in {"failed", "cancelled", "timed_out"}, "attempt_not_retryable")
    receipt = session.mutate(api, label, base + "/retry", {"attempt_id": attempt, "reason": "Explicit finite media continuation"})
    require(receipt.get("new_attempt_id") and receipt["new_attempt_id"] != attempt, "new_attempt_not_confirmed")
    session.state["attempt_id"] = receipt["new_attempt_id"]
    session.state["attempts"].append(receipt["new_attempt_id"])
    session.save()
    return prepare_current(args, api, session)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--session", type=Path, required=True)
    parser.add_argument("--relay-binary", default="compute-relay")
    commands = parser.add_subparsers(dest="action", required=True)
    prepare = commands.add_parser("prepare")
    for name in ("media", "runtime", "ca"):
        prepare.add_argument("--" + name, type=Path)
    for name in ("connection", "end-at"):
        prepare.add_argument("--" + name)
    for name in ("width", "height"):
        prepare.add_argument("--" + name, type=int)
    prepare.add_argument("--wall-seconds", type=int, default=28800)
    resume = commands.add_parser("continue")
    for command_parser in (prepare, resume):
        command_parser.add_argument("--remote-free-bytes", type=int, required=True)
        command_parser.add_argument("--space-observed-at", required=True)
    commands.add_parser("start").add_argument("--endpoint-stdin", action="store_true")
    commands.add_parser("observe").add_argument("--pages", type=int, default=1, choices=range(1, 101))
    commands.add_parser("cancel")
    commands.add_parser("finish")
    args = parser.parse_args()
    try:
        require(os.name == "posix", "example_controller_requires_posix_file_locking")
        import fcntl
        session = Session(args.session)
        with (session.directory / "session.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Reload after locking so concurrent readers cannot overwrite state.
            session = Session(args.session)
            api = API(args.url, args.workspace, private_token(args.token_file))
            if args.action == "prepare":
                prepare_inputs(args, api, session)
                result = prepare_current(args, api, session)
            elif args.action == "start":
                result = start(args, api, session)
            elif args.action == "observe":
                result = observe(args, api, session)
            elif args.action == "finish":
                result = finish(args, api, session)
            elif args.action == "continue":
                result = continuation(args, api, session)
            else:
                base, attempt = session.target(api)
                result = session.mutate(api, "cancel:" + attempt, base + "/cancel", {"attempt_id": attempt})
            print(json.dumps(result, sort_keys=True))
        return 0
    except Failure as exc:
        print(str(exc), file=sys.stderr)
    except Exception:
        print("controller_failed; preserve the session and inspect the original attempt", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
