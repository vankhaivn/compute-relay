"""BUG-RUN-20260917-01-01: real pinned SDK, synthetic HTTP and quota data only."""
import importlib.util
import json
import unittest

import test_execution as execution_fixtures
import test_monitor_sdk as monitor_fixtures
from test_monitor_protocol import monitor
from test_preflight import response


def omitted_quota():
    # No account, token or raw operator response is retained in this fixture.
    return {"gpuQuota": {"totalTimeAllowed": "108000s", "timeUsed": "0s",
                         "timeReserved": "0s", "minimumTimeAllowed": "108000s",
                         "hasEverRun": True}}


@unittest.skipUnless(importlib.util.find_spec("kagglesdk"), "real SDK runs in locked client CI")
class QuotaDefaultSDKTests(unittest.TestCase):
    def monitor_fixture(self, raw):
        fixture = monitor_fixtures.MonitorPinnedSDKTests()
        fixture.setUp()
        fixture.quota = raw
        return fixture

    def execution_fixture(self, raw):
        fixture = execution_fixtures.ExecutionPinnedSDKTests()
        fixture.setUp()
        fixture.r.update(gpu=True, machine_shape="NvidiaTeslaT4")
        original = fixture.exchange

        def exchange(adapter, request, **kwargs):
            result = original(adapter, request, **kwargs)
            if request.url == execution_fixtures.bridge.ROOT + execution_fixtures.bridge.OPERATIONS["quota"]:
                result.close()
                return response(json.dumps(raw).encode("utf-8"))
            return result

        fixture.exchange = exchange
        return fixture

    def test_omitted_false_matches_pinned_sdk_and_preserves_complete_durations(self):
        from kagglesdk.kernels.types.kernels_api_service import ApiAcceleratorQuota
        defaults = ApiAcceleratorQuota()
        self.assertIs(defaults.is_pay_to_scale_enabled, False)
        self.assertIsNone(defaults.total_time_allowed)
        self.assertIsNone(defaults.time_used)
        self.assertIsNone(defaults.time_reserved)
        fixture = self.monitor_fixture(omitted_quota())
        result = fixture.invoke("quota")
        self.assertEqual(result, dict(monitor.empty("known", "none"),
                                      limit_ns="108000000000000", used_ns="0", reserved_ns="0"))
        self.assertEqual(len(fixture.calls), 2)  # active account then quota, no mutation
        fixture.quota["gpuQuota"]["isPayToScaleEnabled"] = False
        self.assertEqual(fixture.invoke("quota"), result)

    def test_omission_cannot_default_missing_or_invalid_duration_messages(self):
        for field in ("totalTimeAllowed", "timeUsed", "timeReserved"):
            for mode in ("missing", "null", "negative", "malformed"):
                with self.subTest(field=field, mode=mode):
                    raw = omitted_quota()
                    if mode == "missing":
                        del raw["gpuQuota"][field]
                    else:
                        raw["gpuQuota"][field] = {"null": None, "negative": "-1s", "malformed": "bad"}[mode]
                    result = self.monitor_fixture(raw).invoke("quota")
                    self.assertNotEqual(result["status"], "known")
                    self.assertEqual(result["limit_ns"] + result["used_ns"] + result["reserved_ns"], "")
        for raw in ({}, {"gpuQuota": None}, {"gpuQuota": {}}):
            self.assertNotEqual(self.monitor_fixture(raw).invoke("quota")["status"], "known")

    def test_omitted_false_passes_the_second_gate_without_duplicate_save(self):
        for explicit in (False, True):
            with self.subTest(explicit_false=explicit):
                raw = omitted_quota()
                if explicit:
                    raw["gpuQuota"]["isPayToScaleEnabled"] = False
                fixture = self.execution_fixture(raw)
                self.assertEqual(fixture.invoke("submit")["status"], "found")
                self.assertEqual(fixture.saves, 1)
                self.assertEqual(fixture.invoke("reconcile")["status"], "found")
                self.assertEqual(fixture.invoke("observe")["status"], "found")
                self.assertEqual(fixture.saves, 1)
                saves = [body for op, body in fixture.calls if op == execution_fixtures.bridge.OPERATIONS["save"]]
                self.assertEqual(len(saves), 1)
                self.assertIs(saves[0]["isPrivate"], True)
                self.assertIs(saves[0]["enableGpu"], True)
                self.assertEqual(saves[0]["machineShape"], "NvidiaTeslaT4")

    def test_paid_null_and_false_like_values_never_authorize_either_gate(self):
        for flag in (True, None, "false", "true", "", 0, 1, 0.0, [], {}):
            with self.subTest(flag=flag):
                raw = omitted_quota()
                raw["gpuQuota"]["isPayToScaleEnabled"] = flag
                result = self.monitor_fixture(raw).invoke("quota")
                self.assertNotEqual(result["status"], "known")
                self.assertEqual(result["limit_ns"] + result["used_ns"] + result["reserved_ns"], "")
                fixture = self.execution_fixture(raw)
                self.assertEqual(fixture.invoke("submit")["status"], "rejected")
                self.assertEqual(fixture.saves, 0)
        for raw in ({}, {"gpuQuota": None}, {"gpuQuota": []}):
            fixture = self.execution_fixture(raw)
            self.assertEqual(fixture.invoke("submit")["status"], "rejected")
            self.assertEqual(fixture.saves, 0)

    def test_account_mismatch_and_later_paid_setting_still_stop_submission(self):
        fixture = self.monitor_fixture(omitted_quota())
        fixture.wrong_account = True
        self.assertEqual(fixture.invoke("quota")["status"], "unavailable")
        self.assertEqual(len(fixture.calls), 1)
        submit = self.execution_fixture(omitted_quota())
        submit.wrong_account = True
        self.assertEqual(submit.invoke("submit")["status"], "rejected")
        self.assertEqual(submit.saves, 0)
        self.assertEqual(len(submit.calls), 1)
        # An earlier good observation does not skip the fresh pre-save paid check.
        self.assertEqual(self.monitor_fixture(omitted_quota()).invoke("quota")["status"], "known")
        changed = omitted_quota()
        changed["gpuQuota"]["isPayToScaleEnabled"] = True
        submit = self.execution_fixture(changed)
        self.assertEqual(submit.invoke("submit")["status"], "rejected")
        self.assertEqual(submit.saves, 0)

    def test_lost_save_after_defaulted_flag_keeps_original_reconciliation(self):
        fixture = self.execution_fixture(omitted_quota())
        fixture.save_fault = "lost"
        self.assertEqual(fixture.invoke("submit")["status"], "unknown")
        self.assertEqual(fixture.saves, 1)
        fixture.save_fault = ""
        self.assertEqual(fixture.invoke("reconcile")["status"], "found")
        self.assertEqual(fixture.saves, 1)
