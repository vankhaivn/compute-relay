"""Only synthetic, repository-owned fixtures execute; never an admitted API workload."""

import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from relay_runner import contract, main, process

SCRIPT = Path(main.__file__).parents[1] / "run.py"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bundle(entries):
    """Match M2-07 regular USTAR framing: manifest first, sorted files, exactly two end blocks."""
    manifest = {"bundle_version": "compute-relay/bundle/v1", "files": [
        {"path": name, "bytes": len(data), "sha256": sha(data), "executable": False}
        for name, data in sorted(entries.items())]}
    stream = io.BytesIO()
    for name, data in [(".compute-relay/bundle.json", json.dumps(manifest).encode())] + [("code/" + n, d) for n, d in sorted(entries.items())]:
        h = tarfile.TarInfo(name)
        h.size, h.mode, h.mtime = len(data), 0o644, 0
        stream.write(h.tobuf(tarfile.USTAR_FORMAT))
        stream.write(data)
        stream.write(bytes((-len(data)) % 512))
    stream.write(bytes(1024))
    return gzip.compress(stream.getvalue(), mtime=0)


SUCCESS = b'''import json, os, pathlib, sys
root = pathlib.Path(os.environ["CC_OUTPUT_DIR"])
assert "KAGGLE_API_TOKEN" not in os.environ
assert "HTTP_PROXY" not in os.environ
assert "AWS_SECRET_ACCESS_KEY" not in os.environ
assert os.environ["CC_ATTEMPT_ID"] == "att_fixture"
assert pathlib.Path(os.environ["CC_INPUT_DIR"], "data.txt").read_text() == "input fixture"
assert sys.argv[1] == "$CC_INPUT_DIR"
(root / "answer.json").write_text(json.dumps({"answer": 42}))
print("fixture finished")
'''


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.staging = self.base / "staging"
        self.staging.mkdir()
        self.root = self.base / "attempt"
        self.m = self.make(SUCCESS)

    def make(self, payload, extra=None):
        raw = bundle({"main.py": payload, **(extra or {})})
        (self.staging / "bundle.tar.gz").write_bytes(raw)
        data = b"input fixture"
        (self.staging / "data").write_bytes(data)
        inputs = [{"name": "input", "path": "data", "target": "data.txt", "bytes": len(data), "sha256": sha(data)}]
        return {"manifest_version": contract.VERSION, "job_id": "job_fixture", "attempt_id": "att_fixture",
                "attempt_nonce": "fixture-nonce-0123456789", "bundle": {"path": "bundle.tar.gz", "bytes": len(raw), "sha256": sha(raw)},
                "inputs": inputs, "input_manifest_sha256": contract.input_digest(inputs),
                "execution": {"kind": "python", "command": ["python", "main.py", "$CC_INPUT_DIR"]},
                "outputs": [{"path": "answer.json", "required": True}], "resources": {"accelerator": "cpu"},
                "network": {"remote_internet": "disabled"}, "timeouts": {"remote_wall_seconds": 10, "setup_seconds": 5, "finalization_grace_seconds": 2}}

    def invoke(self, m=None, root=None):
        result = main.execute(m or self.m, self.staging, root or self.root, "disabled")
        export = os.environ.get("CR_RUNNER_RESULTS_DIR")
        if export:
            destination = Path(export)
            destination.mkdir(parents=True, exist_ok=True)
            data = json.dumps(result, sort_keys=True).encode()
            (destination / (sha(data) + ".json")).write_bytes(data)
        return result


class ContractTests(Fixture):
    def test_strict_contract_and_budgets(self):
        mutations = [lambda m: m.update(secret="canary"), lambda m: m.update(job_id="../escape"),
                     lambda m: m["timeouts"].update(setup_seconds=10), lambda m: m["timeouts"].update(setup_seconds=True),
                     lambda m: m["execution"].update(command="python main.py"),
                     lambda m: m["execution"].update(environment={"CC_JOB_ID": "wrong"}),
                     lambda m: m["execution"].update(environment={"KAGGLE_API_TOKEN": "canary"}),
                     lambda m: m["execution"].update(environment={"BASH_ENV": "evil"}),
                     lambda m: m["execution"].update(environment={"PYTHONPATH": "evil"}),
                     lambda m: m["execution"].update(command=["/bin/sh"]),
                     lambda m: m["bundle"].update(path="../private"),
                     lambda m: m["inputs"][0].update(target=".compute-relay/secret"),
                     lambda m: m["inputs"].append(dict(m["inputs"][0])),
                     lambda m: m["outputs"].append({"path": "answer.json/child", "required": True}),
                     lambda m: m["resources"].update(minimum_gpu_count=1),
                     lambda m: m.update(limits={"stdout_bytes": 100 << 20})]
        for mutate in mutations:
            m = copy.deepcopy(self.m)
            mutate(m)
            with self.subTest(mutation=mutate), self.assertRaises((contract.Failure, ValueError)):
                contract.load(json.dumps(m).encode())
        for raw in [b'null', b'{}', b'{"job_id":"one","job_id":"two"}', b'{"nan":NaN}', b'[]', b'{} {}']:
            with self.assertRaises(contract.Failure):
                contract.load(raw)

    def test_validate_never_executes(self):
        p = self.base / "manifest.json"
        p.write_text(json.dumps(self.m))
        with patch.object(main, "execute", side_effect=AssertionError("must not execute")):
            self.assertEqual(main.main(["--manifest", str(p), "--validate-only"]), 0)
        self.assertFalse(self.root.exists())

    def test_input_identity_excludes_provider_paths(self):
        inputs = copy.deepcopy(self.m["inputs"])
        inputs[0]["path"] = "another/physical/path"
        self.assertEqual(contract.input_digest(inputs), self.m["input_manifest_sha256"])
        inputs[0]["target"] = "changed.txt"
        self.assertNotEqual(contract.input_digest(inputs), self.m["input_manifest_sha256"])

    def test_pinned_requirements_reject_options_and_gpu_replacement(self):
        for text in ("torch==1.0", "nvidia-cublas-cu12==1.0", "pkg>=1.0", "-r other.txt", "https://example.org/a.whl", "--index-url https://example.org", "x==1.0\n--extra-index-url x"):
            (self.staging / "requirements.txt").write_text(text)
            with self.assertRaises(contract.Failure):
                main.pinned_requirements(self.staging, "requirements.txt")
        (self.staging / "requirements.txt").write_text("# complete pins\nsmall==1.2.3\n")
        self.assertEqual(main.pinned_requirements(self.staging, "requirements.txt"), "small==1.2.3\n")


@unittest.skipUnless(sys.platform == "linux", "Remote execution contract targets Linux only")
class ExecutionTests(Fixture):
    def test_success_roundtrip_and_no_ambient_credentials(self):
        with patch.dict(os.environ, {"KAGGLE_API_TOKEN": "synthetic-secret", "HTTP_PROXY": "private-proxy", "AWS_SECRET_ACCESS_KEY": "synthetic-aws"}):
            r = self.invoke()
        self.assertEqual(r["phase"], "completed", r)
        self.assertEqual(r["exit_code"], 0)
        self.assertEqual(r["artifacts"][0]["sha256"], sha((self.root / "outputs/answer.json").read_bytes()))
        saved = json.loads((self.root / "control/execution-result.json").read_text())
        self.assertEqual(r, saved)
        env = (self.root / "control/environment.json").read_text()
        self.assertNotIn("synthetic-secret", env)
        self.assertEqual([p["phase"] for p in json.loads(env)["processes"]], ["resource_check", "payload"])
        with self.assertRaises(FileExistsError):
            self.invoke()  # No accidental re-execution in the same directory.

    def test_shell_setup_and_exit_code(self):
        m = self.make(b"unused", {"work.sh": b'printf setup-ok >&2\nexit 17\n'})
        m["execution"] = {"kind": "shell", "command": ["sh", "work.sh"], "dependencies": {"shell_setup": [["sh", "-c", "printf prepared > setup.txt"]]}}
        r = self.invoke(m)
        self.assertEqual((r["phase"], r["exit_code"], r["error"]["code"]), ("failed", 17, "COMMAND_FAILED"))
        self.assertEqual((self.root / "code/setup.txt").read_text(), "prepared")

    def test_setup_failure_preserved_and_no_payload(self):
        m = self.make(SUCCESS)
        m["execution"] = {"kind": "shell", "command": ["sh", "-c", "touch payload-ran"], "dependencies": {"shell_setup": [["sh", "-c", "exit 23"]]}}
        r = self.invoke(m)
        self.assertEqual(r["phase"], "setup_failed")
        self.assertEqual(r["error"]["code"], "DEPENDENCY_SETUP_FAILED")
        self.assertIsNone(r["exit_code"])
        self.assertFalse((self.root / "code/payload-ran").exists())
        env = json.loads((self.root / "control/environment.json").read_text())
        self.assertEqual(env["processes"][-1]["exit_code"], 23)

    def test_bad_input_and_bundle_never_launch(self):
        for key in ("bundle", "input", "input_manifest"):
            m = copy.deepcopy(self.m)
            if key == "bundle":
                m["bundle"]["sha256"] = "a" * 64
            elif key == "input":
                m["inputs"][0]["sha256"] = "a" * 64
                m["input_manifest_sha256"] = contract.input_digest(m["inputs"])
            else:
                m["input_manifest_sha256"] = "a" * 64
            with patch.object(main, "run", side_effect=AssertionError("unsafe launch")):
                r = self.invoke(m, self.base / key)
            self.assertEqual(r["phase"], "failed")
            self.assertEqual(r["error"]["code"], "INPUT_DIGEST_MISMATCH")

    def test_network_mismatch_never_launches(self):
        self.m["network"]["remote_internet"] = "required"
        with patch.object(main, "run", side_effect=AssertionError("unsafe launch")):
            r = self.invoke()
        self.assertEqual(r["error"]["code"], "UNSUPPORTED_CAPABILITY")

    def test_gpu_failure_has_no_cpu_fallback(self):
        self.m["resources"] = {"accelerator": "gpu", "minimum_gpu_count": 1}
        with patch.object(main, "run", return_value=process.Outcome(1, False, False, True)) as calls:
            r = self.invoke()
        self.assertEqual(r["phase"], "resource_check_failed")
        self.assertEqual(r["error"]["code"], "RESOURCE_REQUIREMENT_UNSATISFIED")
        self.assertFalse(r["resource_check"]["gpu_verified"])
        self.assertEqual(calls.call_count, 1)
        self.assertFalse((self.root / "outputs/answer.json").exists())

    def test_missing_required_and_optional_outputs(self):
        self.m["outputs"].append({"path": "missing.txt", "required": True})
        r = self.invoke()
        self.assertEqual((r["phase"], r["exit_code"]), ("failed", 0))
        self.assertEqual(r["error"]["code"], "ARTIFACT_MISSING")
        self.m["outputs"][-1]["required"] = False
        r = self.invoke(root=self.base / "optional")
        self.assertEqual(r["phase"], "completed")

    def test_output_links_and_quota_fail(self):
        for variant in ("symlink", "hardlink", "overflow", "directory"):
            payload = {"symlink": b'import os; os.symlink("/etc/passwd", os.environ["CC_OUTPUT_DIR"]+"/answer.json")',
                       "hardlink": b'import os; os.link(__file__, os.environ["CC_OUTPUT_DIR"]+"/answer.json")',
                       "overflow": b'import os,pathlib; pathlib.Path(os.environ["CC_OUTPUT_DIR"],"answer.json").write_bytes(b"x"*100)',
                       "directory": b'import os,pathlib; pathlib.Path(os.environ["CC_OUTPUT_DIR"],"answer.json").mkdir()'}[variant]
            m = self.make(payload)
            m["limits"] = {"artifact_bytes": 32}
            r = self.invoke(m, self.base / variant)
            self.assertEqual((r["phase"], r["exit_code"], r["error"]["stage"]), ("failed", 0, "results"))

    def test_bounded_logs_and_pattern_redaction(self):
        payload = b'import os,sys; print("cr1_"+"A"*43); sys.stdout.write("x"*200000); sys.stderr.write("y"*200000)'
        m = self.make(payload)
        m["limits"] = {"stdout_bytes": 256, "stderr_bytes": 256}
        m["outputs"][0]["required"] = False
        r = self.invoke(m)
        self.assertEqual(r["phase"], "completed")
        for key in ("stdout", "stderr"):
            raw = (self.root / ("control/" + key + ".log")).read_bytes()
            self.assertLessEqual(len(raw), 256)
            self.assertIn(process.MARKER, raw)
            self.assertNotIn(b"cr1_", raw)

    def test_cli_timeout_and_child_group_cleanup(self):
        payload = b'''import subprocess, sys, pathlib, time, signal
p = subprocess.Popen([sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)"])
pathlib.Path("child.pid").write_text(str(p.pid))
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(30)
'''
        m = self.make(payload)
        m["timeouts"] = {"remote_wall_seconds": 4, "setup_seconds": 1, "finalization_grace_seconds": 1}
        start = time.monotonic()
        probe = 'import sys,json; open(sys.argv[1],"x").write(json.dumps({"packages":[]}))'
        with patch.object(main, "PROBE", probe):
            r = self.invoke(m)
        self.assertEqual(r["phase"], "timed_out", r)
        self.assertTrue(r["timed_out"])
        self.assertLess(time.monotonic() - start, 5)
        pid = int((self.root / "code/child.pid").read_text())
        status = Path(f"/proc/{pid}/stat")
        self.assertTrue(not status.exists() or status.read_text().split()[2] == "Z", "child still executing")

    def test_explicit_cli_and_sigterm(self):
        m = self.make(b'import pathlib,time; pathlib.Path("started").touch(); time.sleep(30)')
        path = self.base / "manifest.json"
        path.write_text(json.dumps(m))
        p = subprocess.Popen([sys.executable, str(SCRIPT), "--execute", "--manifest", str(path),
                              "--staging-root", str(self.staging), "--work-root", str(self.root), "--network-mode", "disabled"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(lambda: p.kill() if p.poll() is None else None)
        deadline = time.monotonic() + 5
        while not (self.root / "code/started").exists() and p.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        p.send_signal(signal.SIGTERM)
        out, err = p.communicate(timeout=3)
        self.assertEqual(p.returncode, 1, (out, err))
        r = json.loads((self.root / "control/execution-result.json").read_text())
        self.assertEqual(r["phase"], "cancelled")
        self.assertFalse(r["timed_out"])

    def test_setup_timeout_prevents_payload(self):
        m = self.make(b"unused")
        m["execution"] = {"kind": "shell", "command": ["sh", "-c", "touch payload-ran"],
                          "dependencies": {"shell_setup": [["sh", "-c", "sleep 30"]]}}
        m["timeouts"] = {"remote_wall_seconds": 5, "setup_seconds": 2, "finalization_grace_seconds": 1}
        r = self.invoke(m)
        self.assertEqual((r["phase"], r["timed_out"]), ("timed_out", True))
        self.assertIsNone(r["exit_code"])
        self.assertFalse((self.root / "code/payload-ran").exists())

    def test_completed_leader_cannot_leave_running_child(self):
        m = self.make(b'import subprocess,sys; subprocess.Popen([sys.executable,"-c","import time,pathlib; time.sleep(2); pathlib.Path(\\"escaped\\").touch()"])')
        m["outputs"][0]["required"] = False
        started = time.monotonic()
        r = self.invoke(m)
        self.assertEqual(r["phase"], "completed", r)
        time.sleep(2.1)
        self.assertFalse((self.root / "code/escaped").exists())
        self.assertLess(time.monotonic() - started, 6)

    def test_requirements_never_install_when_network_disabled(self):
        m = self.make(SUCCESS, {"requirements.txt": b"small==1.2.3"})
        m["execution"]["dependencies"] = {"python_requirements": "requirements.txt"}
        r = self.invoke(m)
        self.assertEqual(r["phase"], "setup_failed")
        self.assertEqual(r["error"]["code"], "UNSUPPORTED_CAPABILITY")
        env = json.loads((self.root / "control/environment.json").read_text())
        self.assertEqual([p["phase"] for p in env["processes"]], ["resource_check"])

    def test_dependency_plan_is_venv_only_and_no_ambient_pip_config(self):
        m = self.make(SUCCESS, {"requirements.txt": b"small==1.2.3"})
        m["network"]["remote_internet"] = "required"
        m["execution"]["dependencies"] = {"python_requirements": "requirements.txt"}
        seen = []
        def fake_run(args, cwd, env, deadline, grace, stdout, stderr, cancelled):
            seen.append((args, dict(env)))
            if main.PROBE in args:
                Path(args[-2]).write_text(json.dumps({"python": "fixture", "packages": []}))
            if "main.py" in args:
                (self.root / "outputs/answer.json").write_text("{}")
            return process.Outcome(0, False, False, True)
        with patch.object(main, "run", side_effect=fake_run):
            r = main.execute(m, self.staging, self.root, "required")
        self.assertEqual(r["phase"], "completed")
        install, env = next(x for x in seen if "pip" in x[0])
        self.assertIn("scratch/venv/bin/python", install[0])
        self.assertIn("--no-deps", install)
        self.assertIn("--only-binary=:all:", install)
        self.assertEqual(env["PIP_CONFIG_FILE"], os.devnull)
        self.assertNotIn("--upgrade", install)
        # This is command-construction evidence only; no packages were downloaded.

    def test_resource_count_memory_and_success_with_synthetic_probe(self):
        for count, memory, want in [(0, [], False), (1, [4], False), (2, [16,16], True)]:
            root = self.base / ("gpu" + str(count))
            m = copy.deepcopy(self.m)
            m["resources"] = {"accelerator": "gpu", "minimum_gpu_count": 2, "minimum_gpu_memory_bytes": 8}
            def fake_run(args, cwd, env, deadline, grace, stdout, stderr, cancelled):
                if main.PROBE in args:
                    Path(args[-2]).write_text(json.dumps({"gpu": {"count": count, "memory_bytes": memory, "names": ["synthetic"], "cuda": "fixture", "framework": "fixture"}}))
                else:
                    (root / "outputs/answer.json").write_text("{}")
                return process.Outcome(0, False, False, True)
            with patch.object(main, "run", side_effect=fake_run):
                r = self.invoke(m, root)
            self.assertEqual(r["resource_check"]["gpu_verified"], want)
            self.assertEqual(r["phase"] == "completed", want)
            # Deliberately synthetic policy coverage, NOT evidence of actual GPU access.


    def test_artifact_directory_and_zero_byte_files(self):
        m = self.make(b'import os,pathlib; p=pathlib.Path(os.environ["CC_OUTPUT_DIR"],"dir");p.mkdir();(p/"empty").touch();(p/"x").write_bytes(b"xyz")')
        m["outputs"] = [{"path": "dir", "kind": "directory", "required": True}]
        r = self.invoke(m)
        self.assertEqual(r["phase"], "completed")
        self.assertEqual([(x["path"], x["bytes"]) for x in r["artifacts"]], [("dir/empty", 0), ("dir/x", 3)])


@unittest.skipUnless(sys.platform == "linux", "Remote extraction targets Linux")
class ArchiveTests(Fixture):
    def test_bad_archives_fail_before_any_command(self):
        good = (self.staging / "bundle.tar.gz").read_bytes()
        plain = gzip.decompress(good)
        cases = [good + good, good + b"junk", good[:-4], gzip.compress(plain + b"extra"), gzip.compress(b"x" * 4096)]
        for index, bad in enumerate(cases):
            (self.staging / "bundle.tar.gz").write_bytes(bad)
            m = copy.deepcopy(self.m)
            m["bundle"].update(bytes=len(bad), sha256=sha(bad))
            with patch.object(main, "run", side_effect=AssertionError("unsafe archive executed")):
                r = self.invoke(m, self.base / f"bad{index}")
            self.assertNotEqual(r["phase"], "completed")

    def test_archive_path_and_header_type_corpus(self):
        for name in ("../escape", "/absolute", "A/../b", "a\\b", "CON", "x/../../control/result"):
            bad = bundle({name: b"x"})
            (self.staging / "bundle.tar.gz").write_bytes(bad)
            m = copy.deepcopy(self.m)
            m["bundle"].update(bytes=len(bad), sha256=sha(bad))
            with patch.object(main, "run", side_effect=AssertionError("unsafe archive executed")):
                r = self.invoke(m, self.base / sha(name.encode())[:8])
            self.assertNotEqual(r["phase"], "completed")
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE, tarfile.XHDTYPE, tarfile.GNUTYPE_SPARSE):
            h = tarfile.TarInfo(".compute-relay/bundle.json")
            h.type, h.mode = kind, 0o644
            bad = gzip.compress(h.tobuf(tarfile.USTAR_FORMAT) + bytes(1024))
            (self.staging / "bundle.tar.gz").write_bytes(bad)
            m = copy.deepcopy(self.m)
            m["bundle"].update(bytes=len(bad), sha256=sha(bad))
            with patch.object(main, "run", side_effect=AssertionError("unsafe header executed")):
                r = self.invoke(m, self.base / ("type" + kind.decode()))
            self.assertNotEqual(r["phase"], "completed")

    def test_staging_symlink_and_hardlink_rejected(self):
        source = self.staging / "data"
        source.unlink()
        source.symlink_to("/etc/passwd")
        with patch.object(main, "run", side_effect=AssertionError("link executed")):
            r = self.invoke()
        self.assertEqual(r["phase"], "failed")


if __name__ == "__main__":
    unittest.main()
