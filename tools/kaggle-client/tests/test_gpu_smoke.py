"""Arithmetic wiring with synthetic tensor objects; no torch import or GPU allocation."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("smoke_calculation", ROOT / "examples/jobs/gpu-smoke/calculation.py")
calculation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(calculation)


class Number:
    def __init__(self, value):
        self.value = value
    def item(self):
        return self.value


class Tensor:
    def __init__(self, value, cuda=True):
        self.value, self.is_cuda = value, cuda
    def __mul__(self, other):
        return Tensor(self.value * other, self.is_cuda)
    def __eq__(self, other):
        return Number(self.value == other)
    def sum(self):
        return Number(self.value * 64 * 64)


class GPUCalculationTests(unittest.TestCase):
    def framework(self, available=True, cuda=True, wrong=False):
        calls = []
        def ones(shape, *, device, dtype):
            self.assertEqual((shape, device, dtype), ((64, 64), "cuda:0", "float32"))
            calls.append("allocate")
            return Tensor(1, cuda)
        def multiply(left, right):
            calls.append("matmul")
            return Tensor(left.value * right.value * 64 + int(wrong), cuda)
        def synchronize(index):
            self.assertEqual(index, 0)
            calls.append("synchronize")
        return SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: available, synchronize=synchronize),
                               float32="float32", ones=ones, mm=multiply, all=lambda value: value), calls

    def test_challenge_selects_exact_bounded_calculation(self):
        for prefix in ("00", "7f", "ff"):
            challenge = {"schema": 1, "challenge": prefix + "a" * 62}
            framework, calls = self.framework()
            result = calculation.calculate(framework, challenge)
            scale = int(prefix, 16) % 7 + 1
            self.assertEqual(result["result_sum"], 64 ** 3 * scale)
            self.assertTrue(result["cuda_computation"])
            self.assertEqual(calls, ["allocate", "allocate", "matmul", "synchronize"])

    def test_missing_gpu_wrong_result_and_cpu_tensor_never_pass(self):
        challenge = {"schema": 1, "challenge": "a" * 64}
        for options in ({"available": False}, {"cuda": False}, {"wrong": True}):
            framework, calls = self.framework(**options)
            with self.assertRaises(RuntimeError):
                calculation.calculate(framework, challenge)
            if options.get("available") is False:
                self.assertEqual(calls, [])

    def test_input_is_bounded_closed_and_duplicate_safe(self):
        good = json.dumps({"schema": 1, "challenge": "a" * 64}).encode()
        self.assertEqual(calculation.parse_input(good)["challenge"], "a" * 64)
        for raw in (b"", b"[]", b"x" * 1025, good + b" {}",
                    good.replace(b'"schema": 1', b'"schema": true'),
                    good.replace(b'"schema": 1', b'"schema": 2,"schema": 1'),
                    good.replace(b'"challenge"', b'"unknown"'),
                    good.replace(b"a" * 64, b"A" * 64)):
            with self.assertRaises((ValueError, UnicodeError)):
                calculation.parse_input(raw)
