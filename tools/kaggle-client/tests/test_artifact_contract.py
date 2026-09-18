"""Offline artifact selection; no SDK or provider credentials."""
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("artifact_contract", ROOT / "internal/provider/kaggle/artifact_contract.py")
contract = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(contract)


def fixture():
    identity = dict(job_id="job", attempt_id="attempt", attempt_nonce="nonce",
                    bundle_sha256="a" * 64, input_manifest_sha256="b" * 64)
    outputs = [dict(path="answer.txt", kind="file", required=True, max_bytes=10)]
    manifest = dict(identity, phase="completed", artifacts=[dict(path="answer.txt", bytes=3,
                    sha256=hashlib.sha256(b"yes").hexdigest(), media_type="text/plain")])
    listing = {contract.PREFIX + contract.MANIFEST, contract.PREFIX + "outputs/answer.txt",
               contract.PREFIX + "scratch/private.bin", contract.PREFIX + "code/main.py"}
    return identity, outputs, manifest, listing


class ArtifactContractTests(unittest.TestCase):
    def test_only_manifest_declared_outputs_are_selected(self):
        identity, outputs, manifest, listing = fixture()
        raw = json.dumps(manifest).encode()
        result = contract.select_manifest(raw, identity, outputs, listing)
        self.assertEqual([x["path"] for x in result], [contract.MANIFEST, "outputs/answer.txt"])
        self.assertEqual(result[0]["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result[0]["bytes"], len(raw))
        self.assertNotIn("scratch", json.dumps(result))

    def test_identity_required_files_and_bounds_fail_closed(self):
        for fault in ("identity", "digest", "size", "missing", "required", "undeclared", "total", "count"):
            with self.subTest(fault=fault):
                identity, outputs, manifest, listing = fixture()
                kw = {}
                if fault == "identity":
                    manifest["attempt_nonce"] = "foreign"
                if fault == "digest":
                    manifest["artifacts"][0]["sha256"] = "invalid"
                if fault == "size":
                    manifest["artifacts"][0]["bytes"] = True
                if fault == "missing":
                    listing.remove(contract.PREFIX + "outputs/answer.txt")
                if fault == "required":
                    manifest["artifacts"] = []
                if fault == "undeclared":
                    manifest["artifacts"][0]["path"] = "other.txt"
                if fault == "total":
                    kw["max_bytes"] = 1
                if fault == "count":
                    kw["max_files"] = 1
                with self.assertRaises(ValueError):
                    contract.select_manifest(json.dumps(manifest).encode(), identity, outputs, listing, **kw)
        identity, outputs, manifest, listing = fixture()
        manifest.update(phase="failed", artifacts=[])
        self.assertEqual(len(contract.select_manifest(json.dumps(manifest).encode(), identity, outputs, listing)), 1)

    def test_directory_selection_and_case_prefix_collisions(self):
        identity, outputs, manifest, listing = fixture()
        outputs[0].update(path="results", kind="directory")
        manifest["artifacts"][0]["path"] = "results/a.txt"
        listing.add(contract.PREFIX + "outputs/results/a.txt")
        selected = contract.select_manifest(json.dumps(manifest).encode(), identity, outputs, listing)
        self.assertEqual(selected[1]["path"], "outputs/results/a.txt")
        for paths in (["A", "a"], ["foo", "foo/bar"], ["foo/a", "FOO"], ["a", "a"]):
            with self.assertRaises(ValueError):
                contract.collision_free(paths)
        contract.collision_free(["a", "ab/c", "xyz/z"])

    def test_paths_urls_and_json_are_not_normalized_into_authority(self):
        for path in ("", "/abs", "../a", "a/../b", "a//b", "a\\b", "C:/x", "a%2fb", "a\n", "CON.txt", "a.", "a ", "nul/x", "a" * 513):
            self.assertFalse(contract.safe_path(path), path)
        for url in ("http://storage.googleapis.com/a", "https://storage.googleapis.com.evil/a",
                    "https://user@storage.googleapis.com/a", "https://127.0.0.1/a",
                    "https://storage.googleapis.com/a#secret", "https://storage.googleapis.com:444/a",
                    "https://storage.googleapis.com/a\n", "https://storage.googleapis.com/\\x",
                    "http://www.kaggleusercontent.com/kf/a", "https://www.kaggleusercontent.com.evil/kf/a",
                    "https://user@www.kaggleusercontent.com/kf/a", "https://www.kaggleusercontent.com:444/kf/a",
                    "https://www.kaggleusercontent.com/kf/a#secret", "https://www.kaggleusercontent.com/kf/a\n",
                    "https://www.kaggleusercontent.com/\\x"):
            with self.assertRaises(ValueError):
                contract.signed_url(url)
        for url in ("https://storage.googleapis.com/bucket/file?signature=x",
                    "https://storage.googleapis.com:443/bucket/file?signature=x",
                    "https://www.kaggleusercontent.com/kf/output/file?signature=x",
                    "https://www.kaggleusercontent.com:443/kf/output/file?signature=x"):
            self.assertEqual(contract.signed_url(url), url)
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'[]', b'{"x":"\xff"}'):
            with self.assertRaises((ValueError, UnicodeError)):
                contract.strict_json(raw)

    def test_output_declarations_are_closed_and_disjoint(self):
        _, outputs, _, _ = fixture()
        contract.declarations_valid(outputs)
        for extra in (dict(outputs[0]), dict(outputs[0], path="answer.txt/nested")):
            with self.assertRaises(ValueError):
                contract.declarations_valid(outputs + [extra])
        for key, value in (("required", 1), ("kind", "symlink"), ("max_bytes", -1), ("secret", "x")):
            with self.assertRaises(ValueError):
                contract.declarations_valid([dict(outputs[0], **{key: value})])
