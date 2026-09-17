"""Real pinned SDK and isolated in-memory HTTP fixtures. Never contact a provider."""
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

import requests

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("relay_staging", ROOT / "internal/provider/kaggle/staging.py")
staging = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(staging)


def fixture(inputs=2):
    payloads = {"relay-stage.bin": b'{"synthetic":"identity"}', "code.bin": b"synthetic-code"}
    for i in range(inputs):
        payloads[f"input-{i:03d}.bin"] = b"input" + str(i).encode()
    files = [dict(name=n, bytes=len(b), sha256=hashlib.sha256(b).hexdigest()) for n, b in payloads.items()]
    r = dict(protocol=1, owner="fixture_user", slug="crs-" + "a"*40,
             license="copyright-authors", marker_sha256=files[0]["sha256"], files=files)
    staging.validate_request(r)
    return r, payloads


def response(data=b"{}", status=200, headers=None):
    r = requests.Response()
    r.status_code = status
    r.headers = {"Content-Type": "application/json", **(headers or {})}
    r.raw = io.BytesIO(data)
    return r


def typed_response(value):
    return response(json.dumps(type(value).to_dict(value)).encode())


class StagingProtocolTests(unittest.TestCase):
    def test_request_and_destination_are_bounded_and_closed(self):
        r, _ = fixture()
        for mode in ("field", "license", "path", "owner", "digest", "size", "bool-size", "duplicate", "too-many"):
            with self.subTest(mode=mode):
                bad = copy.deepcopy(r)
                if mode == "field": bad["command"] = "not-allowed"
                if mode == "license": bad["license"] = "CC0-1.0"
                if mode == "path": bad["files"][1]["name"] = "../code.bin"
                if mode == "owner": bad["owner"] = "foreign/account"
                if mode == "digest": bad["files"][0]["sha256"] = "0"*64
                if mode == "size": bad["files"][1]["bytes"] = 1 << 40
                if mode == "bool-size": bad["files"][1]["bytes"] = False
                if mode == "duplicate": bad["files"][1] = bad["files"][0]
                if mode == "too-many": bad["files"] = bad["files"] * 100
                with self.assertRaises((ValueError, TypeError)):
                    staging.validate_request(bad)
        for url in ("http://storage.googleapis.com/a", "https://evil.invalid/a", "https://storage.googleapis.com.evil.invalid/a",
                    "https://127.0.0.1/a", "https://user:secret@storage.googleapis.com/a", "https://storage.googleapis.com:444/a",
                    "https://storage.googleapis.com/a#fragment", "https://storage.googleapis.com/\\evil", "https://storage.googleapis.com/a\n"):
            with self.assertRaises(ValueError): staging.signed_url(url)
        self.assertEqual(staging.signed_url("https://storage.googleapis.com/a?signature=fixture"), "https://storage.googleapis.com/a?signature=fixture")
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":"\xff"}', b'[]'):
            with self.assertRaises((ValueError, UnicodeError)): staging.strict_json(raw)

    def test_pins_framing_and_exceptions_cannot_leak_or_start_work(self):
        r, _ = fixture()
        good = b"SYNTHETIC_TOKEN\n" + json.dumps(r).encode() + b"\n"
        for mode in ("pins", "empty", "oversized", "duplicate", "exception", "identity"):
            raw = good
            if mode == "empty": raw = b"\n{}\n"
            if mode == "oversized": raw = b"X"*8193 + b"\n{}\n"
            if mode == "duplicate": raw = b"SYNTHETIC_TOKEN\n" + b'{"protocol":1,"protocol":1}\n'
            source = io.BytesIO(raw)
            if mode == "pins": source = mock.Mock(readline=mock.Mock(side_effect=AssertionError("secret read before pins")))
            out = io.StringIO()
            failure = staging.IdentityError("SYNTHETIC_TOKEN") if mode == "identity" else RuntimeError("SYNTHETIC_TOKEN")
            with mock.patch.object(staging.sys, "argv", ["helper", "create", "10"]), \
                 mock.patch.object(staging.sys, "stdin", SimpleNamespace(buffer=source)), \
                 mock.patch.object(staging, "pins_ready", return_value=mode != "pins"), \
                 mock.patch.object(staging, "stage", side_effect=failure) as call, contextlib.redirect_stdout(out):
                self.assertEqual(staging.main(), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report, staging.empty_result("invalid" if mode == "identity" else "unknown"))
            self.assertNotIn("SYNTHETIC_TOKEN", out.getvalue())
            self.assertEqual(call.call_count, 1 if mode in ("exception", "identity") else 0)

    def test_stream_eof_hash_and_encoding_limits(self):
        data = b"abc"
        f = dict(bytes=3, sha256=hashlib.sha256(data).hexdigest())
        body = staging.UploadBody(io.BytesIO(data), f)
        self.assertEqual(body.read(1), b"a")
        with self.assertRaises(staging.IdentityError): body.verify()
        self.assertEqual(body.read(), b"bc")
        self.assertEqual(body.read(), b"")
        body.verify()
        with self.assertRaises(ValueError): list(staging.chunks(response(b"abcd"), 3))
        with self.assertRaises(ValueError): list(staging.chunks(response(b"", headers={"Content-Encoding": "gzip"}), 3))
        with self.assertRaises(ValueError): list(staging.chunks(response(b"", headers={"Content-Length": "999"}), 3))
        late = response()
        def broken(*a, **kw):
            yield data
            raise OSError("late response error")
        late.iter_content = broken
        with self.assertRaises(OSError): list(staging.chunks(late, 3))

    def test_standalone_watchdog_does_not_depend_on_go_parent(self):
        code = (ROOT / "internal/provider/kaggle/staging.py").read_text(encoding="utf-8")
        script = "import sys,time\nns={'__name__':'fixture'}\nexec(" + repr(code) + ",ns)\n"
        script += "sys.argv=['helper','observe','1']\nns['main']=lambda:time.sleep(60)\nns['bounded_main']()\n"
        result = subprocess.run([sys.executable,"-I","-c",script], capture_output=True, input=b"", timeout=5, check=False)
        self.assertEqual((result.returncode,result.stdout,result.stderr),(3,b"",b""))


class Backend:
    def __init__(self, test, r, payloads):
        self.test, self.r, self.payloads = test, r, payloads
        self.uploaded = {}
        self.exists = False
        self.ready = False
        self.mode = "normal"
        self.calls = []
        self.creates = self.starts = self.puts = self.gets = self.meta_reads = 0
        self.sdk = __import__("kagglesdk.datasets.types.dataset_api_service", fromlist=["unused"])
        self.enums = __import__("kagglesdk.datasets.types.dataset_enums", fromlist=["unused"])

    def exchange(self, adapter, req, **kw):
        self.calls.append((req.method, req.url))
        self.test.assertEqual(kw["proxies"], {})
        self.test.assertTrue(kw["verify"])
        self.test.assertTrue(kw["stream"])
        self.test.assertEqual(adapter.max_retries.total, 0)
        if req.url.startswith("https://storage.googleapis.com/"):
            self.test.assertNotIn("Authorization", req.headers)
            self.test.assertNotIn("Cookie", req.headers)
            name = req.url.split("?",1)[0].rsplit("/",1)[1]
            if req.method == "PUT":
                self.puts += 1
                self.test.assertEqual(req.headers["Content-Type"], "application/octet-stream")
                self.test.assertNotIn("Transfer-Encoding", req.headers)
                chunks = []
                if self.mode != "false-upload-ack":
                    if hasattr(req.body, "read"):
                        while True:
                            b = req.body.read(2)
                            if not b: break
                            chunks.append(b)
                    elif req.body:
                        chunks.append(req.body)
                data = b"".join(chunks)
                self.uploaded[name] = data
                if self.mode != "false-upload-ack": self.test.assertEqual(len(data), int(req.headers["Content-Length"]))
                if self.mode == "lost-upload": raise requests.ConnectionError("SYNTHETIC_TOKEN")
                result = response(b"", headers={"Content-Type":"application/octet-stream"})
                if self.mode == "late-upload":
                    def late(*a, **kw):
                        yield b""
                        raise OSError("late upload error")
                    result.iter_content = late
                return result
            self.test.assertEqual(req.method, "GET")
            self.gets += 1
            data = self.uploaded.get(name, self.payloads[name])
            if self.mode == "corrupt": data = bytes([data[0]^1])+data[1:]
            if self.mode == "short-download": data = data[:-1]
            if self.mode == "overflow": data += b"X"
            if self.mode == "cloud-redirect": return response(status=302, headers={"Location":"https://evil.invalid/"})
            result = response(data, headers={"Content-Type":"application/octet-stream"})
            if self.mode == "late-download":
                def late(*a, **kw):
                    yield data
                    raise OSError("late download error")
                result.iter_content = late
            return result
        self.test.assertTrue(req.url.startswith(staging.API))
        self.test.assertEqual(req.method, "POST")
        self.test.assertEqual(req.headers["Authorization"], "Bearer SYNTHETIC_TOKEN")
        body = json.loads(req.body)
        name = req.url[len(staging.API):]
        if name == staging.INTROSPECT:
            self.test.assertEqual(body["token"], "SYNTHETIC_TOKEN")
            return response(json.dumps(dict(active=self.mode != "inactive", username="other_user" if self.mode=="account" else self.r["owner"])).encode())
        if name == staging.START:
            self.starts += 1
            self.test.assertEqual(body["contentType"], "application/octet-stream")
            f = next(f for f in self.r["files"] if f["name"] == body["name"])
            self.test.assertEqual(body["contentLength"], f["bytes"])
            url = "https://storage.googleapis.com/upload/" + f["name"] + "?signature=fixture"
            if self.mode == "upload-host": url = "https://evil.invalid/upload"
            return response(json.dumps(dict(token="upload-"+f["name"], createUrl=url)).encode())
        self.test.assertTrue(name.startswith(staging.DATASET))
        operation = name[len(staging.DATASET):]
        self.test.assertEqual(body["ownerSlug"],self.r["owner"])
        if operation == "CreateDataset":
            self.creates += 1
            self.test.assertEqual(self.creates, 1)
            self.test.assertIs(body["isPrivate"], True)
            self.test.assertEqual(body["slug"], self.r["slug"])
            self.test.assertEqual(body["licenseName"],"copyright-authors")
            self.test.assertEqual(len(body["files"]),len(self.r["files"]))
            self.test.assertEqual(set(self.uploaded),set(self.payloads))
            self.exists = True
            if self.mode == "lost-create": raise requests.ConnectionError("SYNTHETIC_TOKEN after creation")
            return response(b'{"status":"ok"}')
        self.test.assertEqual(body["datasetSlug"],self.r["slug"])
        if operation == "GetDataset":
            self.meta_reads += 1
            if self.mode == "missing-forbidden" and not self.exists:
                return response(json.dumps({"error": {
                    "code": 403,
                    "message": "Permission 'datasets.get' was denied",
                    "status": "PERMISSION_DENIED",
                }}).encode(), status=403)
            if self.mode == "permission-other":
                return response(json.dumps({"error": {
                    "code": 403,
                    "message": "Permission 'datasets.list' was denied",
                    "status": "PERMISSION_DENIED",
                }}).encode(), status=403)
            if self.mode == "forbidden": return response(status=403)
            if self.mode == "server-error": return response(status=503)
            if self.mode == "fake-404": return response(b'{"code":404,"message":"SYNTHETIC_TOKEN"}')
            if not self.exists: return response(status=404)
            obj = self.sdk.ApiDataset()
            obj.id = 123
            obj.ref = self.r["owner"]+"/"+self.r["slug"]
            obj.current_version_number = 2 if self.mode == "version" else 1
            obj.description = "wrong-marker" if self.mode=="marker" else staging.description(self.r)
            obj.license_name = self.r["license"]
            obj.is_private = self.mode != "public"
            if self.mode=="swap-during-download" and self.gets: obj.id = 124
            return typed_response(obj)
        if operation == "GetDatasetStatus":
            obj = self.sdk.ApiGetDatasetStatusResponse()
            obj.status = self.enums.DatabundleVersionStatus.READY if self.ready else self.enums.DatabundleVersionStatus.BLOBS_RECEIVED
            if self.mode == "failed": obj.status = self.enums.DatabundleVersionStatus.FAILED
            return typed_response(obj)
        if operation == "ListDatasetFiles":
            self.test.assertEqual(body["datasetVersionNumber"],1)
            self.test.assertEqual(body["pageSize"],20)
            start = int(body.get("pageToken", "0"))
            selected = self.r["files"][start:start+2]
            obj = self.sdk.ApiListDatasetFilesResponse()
            if self.mode == "missing": selected=[]
            entries=[]
            for f in selected:
                v=self.sdk.ApiDatasetFile()
                v.name="../escape" if self.mode=="path" else f["name"]
                v.total_bytes=f["bytes"]+1 if self.mode=="size" else f["bytes"]
                entries.append(v)
            obj.dataset_files=entries
            if self.mode=="cursor":
                obj.next_page_token="0"
            elif start+2<len(self.r["files"]) and self.mode!="missing":
                obj.next_page_token=str(start+2)
            return typed_response(obj)
        if operation == "DownloadDataset":
            self.test.assertEqual(body["datasetVersionNumber"],1)
            self.test.assertIs(body["raw"],True)
            self.test.assertIn(body["fileName"],self.payloads)
            url="https://storage.googleapis.com/download/"+body["fileName"]+"?signature=fixture"
            if self.mode=="download-host": url="https://evil.invalid/download"
            # A poisoned redirect body must never be read by either SDK or guard.
            r=response(b"X"*2000000,302,{"Location":url})
            r.iter_content=mock.Mock(side_effect=AssertionError("redirect body consumed"))
            return r
        raise AssertionError("unapproved provider operation " + operation)

    def run(self, mode, data=None):
        if data is None: data=b"".join(self.payloads.values()) + staging.UPLOAD_COMPLETE
        with mock.patch.object(requests.adapters.HTTPAdapter,"send",lambda adapter,req,**kw:self.exchange(adapter,req,**kw)), \
             mock.patch.dict(os.environ,{"KAGGLE_API_TOKEN":"AMBIENT_CANARY","HTTPS_PROXY":"http://do-not-use.invalid"}), \
             mock.patch("kagglesdk.kaggle_http_client.get_access_token_from_env",side_effect=AssertionError("ambient credential")), \
             mock.patch("kagglesdk.kaggle_http_client._get_apikey_creds",side_effect=AssertionError("legacy credential")):
            return staging.stage(mode,"SYNTHETIC_TOKEN",self.r,io.BytesIO(data))


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "requires pinned SDK; pure protocol tests still run")
class StagingPinnedSDKTests(unittest.TestCase):
    def test_private_creation_pending_then_paginated_byte_verification(self):
        self.assertTrue(staging.pins_ready())
        r,payloads=fixture(22)
        backend=Backend(self,r,payloads)
        result=backend.run("create")
        self.assertEqual(result["status"],"pending")
        self.assertTrue(result["private"])
        self.assertEqual(backend.creates,1)
        self.assertEqual(backend.starts,len(payloads))
        self.assertEqual(backend.gets,0)
        backend.ready=True
        result=backend.run("observe",b"")
        self.assertEqual(result["status"],"ready")
        self.assertEqual(result["verified_files"],len(payloads))
        self.assertEqual(result["verified_bytes"],sum(map(len,payloads.values())))
        self.assertEqual(backend.gets,len(payloads))
        self.assertEqual(backend.creates,1)
        self.assertEqual(backend.starts,len(payloads))
        self.assertGreater(sum(url.endswith("/ListDatasetFiles") for _,url in backend.calls),1)
        self.assertNotIn("SYNTHETIC_TOKEN",json.dumps(result))

    def test_missing_dataset_permission_denied_allows_one_private_creation(self):
        r,payloads=fixture()
        missing=Backend(self,r,payloads)
        missing.mode="missing-forbidden"
        self.assertEqual(missing.run("observe",b""),staging.empty_result("not_found"))
        self.assertEqual((missing.starts,missing.puts,missing.creates),(0,0,0))

        backend=Backend(self,r,payloads)
        backend.mode="missing-forbidden"
        result=backend.run("create")
        self.assertEqual(result["status"],"pending")
        self.assertTrue(result["private"])
        self.assertEqual((backend.meta_reads,backend.starts,backend.puts,backend.creates),
                         (2,len(payloads),len(payloads),1))

    def test_lost_creation_acknowledgement_recovers_without_second_mutation(self):
        r,payloads=fixture()
        backend=Backend(self,r,payloads)
        backend.mode="lost-create"
        with self.assertRaises(requests.ConnectionError): backend.run("create")
        self.assertTrue(backend.exists)
        self.assertEqual(backend.creates,1)
        calls=(backend.starts,backend.puts,backend.creates)
        backend.mode="normal";backend.ready=True
        result=backend.run("observe",b"")
        self.assertEqual(result["status"],"ready")
        self.assertEqual(calls,(backend.starts,backend.puts,backend.creates))
        # An already-matching resource is only verified, never updated/versioned.
        backend.run("create")
        self.assertEqual(calls,(backend.starts,backend.puts,backend.creates))

    def test_unknown_read_or_account_cannot_authorize_creation(self):
        for mode in ("account","inactive","forbidden","permission-other","server-error","fake-404"):
            with self.subTest(mode=mode):
                r,p=fixture();b=Backend(self,r,p);b.mode=mode
                with self.assertRaises((staging.IdentityError,requests.HTTPError)): b.run("create")
                self.assertEqual((b.starts,b.puts,b.creates),(0,0,0))
        r,p=fixture();b=Backend(self,r,p)
        self.assertEqual(b.run("observe",b""),staging.empty_result("not_found"))
        self.assertEqual((b.starts,b.puts,b.creates),(0,0,0))

    def test_partial_upload_false_receipt_and_late_error_never_create_dataset(self):
        for mode in ("lost-upload","late-upload","false-upload-ack","upload-host","truncated","changed","trailing"):
            with self.subTest(mode=mode):
                r,p=fixture();b=Backend(self,r,p);b.mode=mode
                data=b"".join(p.values())
                if mode=="truncated": data=data[:-1]
                if mode=="changed": data=b"X"+data[1:]
                data += staging.UPLOAD_COMPLETE
                if mode=="trailing": data+=b"X"
                with self.assertRaises((ValueError,OSError,staging.IdentityError,requests.RequestException)):
                    b.run("create",data)
                self.assertEqual(b.creates,0)
                calls=(b.starts,b.puts,b.creates)
                b.mode="normal"
                self.assertEqual(b.run("observe",b"")["status"],"not_found")
                self.assertEqual(calls,(b.starts,b.puts,b.creates))

    def test_public_changed_or_unverified_resources_never_become_ready(self):
        for mode in ("public","version","marker","failed","missing","path","size","cursor","corrupt","short-download","overflow","late-download","download-host","cloud-redirect","swap-during-download"):
            with self.subTest(mode=mode):
                r,p=fixture();b=Backend(self,r,p);b.mode=mode;b.exists=True;b.ready=True
                try: result=b.run("observe",b"")
                except (ValueError,OSError,staging.IdentityError,requests.RequestException): result=staging.empty_result("invalid")
                self.assertNotEqual(result["status"],"ready")
                if mode=="public": self.assertFalse(result["private"])
                self.assertEqual((b.starts,b.puts,b.creates),(0,0,0))
