"""Small checked CUDA matrix multiplication. The injected module is torch in production."""
import json
import re


def parse_input(raw):
    if not raw or len(raw) > 1024:
        raise ValueError("invalid smoke input length")
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate smoke input field")
            result[key] = value
        return result
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    if type(value) is not dict or set(value) != {"schema", "challenge"}:
        raise ValueError("invalid smoke input")
    if type(value["schema"]) is not int or value["schema"] != 1 or type(value["challenge"]) is not str or not re.fullmatch(r"[a-f0-9]{64}", value["challenge"]):
        raise ValueError("invalid smoke challenge")
    return value


def calculate(torch, challenge):
    if not torch.cuda.is_available():
        raise RuntimeError("GPU required; CPU fallback is forbidden")
    n = 64
    scale = int(challenge["challenge"][:2], 16) % 7 + 1
    left = torch.ones((n, n), device="cuda:0", dtype=torch.float32) * scale
    right = torch.ones((n, n), device="cuda:0", dtype=torch.float32)
    product = torch.mm(left, right)
    if not left.is_cuda or not right.is_cuda or not product.is_cuda:
        raise RuntimeError("calculation did not use CUDA tensors")
    torch.cuda.synchronize(0)
    all_correct = bool(torch.all(product == n * scale).item())
    total = float(product.sum().item())
    expected = n * n * n * scale
    if not all_correct or total != expected:
        raise RuntimeError("GPU matrix result failed exact arithmetic validation")
    return dict(device="cuda:0", matrix_size=n, scale=scale, elements=n*n,
                result_sum=total, expected_sum=expected, all_correct=True, cuda_computation=True)
