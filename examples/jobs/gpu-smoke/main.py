"""Explicit remote GPU smoke workload; never a local provider fallback."""
import hashlib
import json
import os
from pathlib import Path
import platform

from calculation import calculate, parse_input


def main():
    # torch must already exist in the managed remote image. Never install it,
    # enable the network or substitute CPU to make the acceptance test green.
    import torch
    source = Path(os.environ["CC_INPUT_DIR"]) / "challenge.json"
    with source.open("rb") as stream:
        raw = stream.read(1025)
    challenge = parse_input(raw)
    result = calculate(torch, challenge)
    result.update(schema=1, job_id=os.environ["CC_JOB_ID"],
                  attempt_id=os.environ["CC_ATTEMPT_ID"],
                  challenge=challenge["challenge"], input_sha256=hashlib.sha256(raw).hexdigest())
    hardware = dict(schema=1, torch_version=str(torch.__version__),
                    cuda_version=str(torch.version.cuda), python_version=platform.python_version(),
                    device_name=torch.cuda.get_device_name(0), device="cuda:0")
    root = Path(os.environ["CC_OUTPUT_DIR"])
    for name, value in (("result.json", result), ("hardware.json", hardware)):
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > 4096:
            raise RuntimeError("GPU smoke evidence exceeds its bound")
        with (root / name).open("x", encoding="utf-8") as stream:
            stream.write(encoded + "\n")
    print("GPU smoke calculation verified; inspect the retained result artifacts")


if __name__ == "__main__":
    main()
