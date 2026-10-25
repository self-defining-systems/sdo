"""Measure validator startup locally, without a cluster or network.

Supply a baseline controller (the old init-container compiler source), a slim
baseline validator, and the candidate validator. Images are never built, pruned,
or changed by this script. All writes use a private temporary directory.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path


def run(args: list[str]) -> tuple[float, str]:
    started = time.monotonic()
    result = subprocess.run(args, capture_output=True, text=True, timeout=240, check=False)
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args}\n{result.stdout}\n{result.stderr}")
    return time.monotonic() - started, result.stdout + result.stderr


def image_size(image: str) -> int:
    _, output = run(["docker", "image", "inspect", "--format", "{{.Size}}", image])
    return int(output.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-controller", required=True)
    parser.add_argument("--baseline-validator", required=True)
    parser.add_argument("--candidate-validator", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for image in (args.baseline_controller, args.baseline_validator, args.candidate_validator):
        image_size(image)  # Fail before Docker can try pulling a missing image.
    fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/sdo/playbook_program_app"
    with tempfile.TemporaryDirectory(prefix="sdo-validator-measure-") as temporary:
        root = Path(temporary)
        os.chmod(root, 0o755)
        for name in ("seed", "before", "after"):
            (root / name).mkdir(mode=0o777)
            os.chmod(root / name, 0o777)
        base = [
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--cpus",
            "2",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
        ]
        copy_seconds, _ = run(
            [
                *base,
                "--memory",
                "256m",
                "--volume",
                f"{root / 'seed'}:/sdo-toolchain",
                args.baseline_controller,
                "/bin/sh",
                "-ec",
                "cp -R /usr/local/go /sdo-toolchain/go; cp -R /go /sdo-toolchain/cache",
            ]
        )
        results: dict[str, object] = {"init_copy_seconds": copy_seconds}
        for phase, image, mounts in (
            (
                "before",
                args.baseline_validator,
                [
                    "--volume",
                    f"{root / 'seed/go'}:/usr/local/go:ro",
                    "--volume",
                    f"{root / 'seed/cache'}:/go:ro",
                ],
            ),
            ("after", args.candidate_validator, []),
        ):
            _, schema = run([*base, "--workdir", "/opt/sdo/sdo/operator", image, "sdo-operator", "schema-identity"])
            # Querying the schema from source is deliberately independent of the
            # caller; the validator still enforces its own compiled schema pin.
            seconds, output = run(
                [
                    *base,
                    "--memory",
                    "3g",
                    *mounts,
                    "--volume",
                    f"{root / phase}:/tmp",
                    "--volume",
                    f"{fixture}:/fixture:ro",
                    image,
                    "sdo-operator",
                    "validate",
                    "--app",
                    "/fixture",
                    "--expect-schema",
                    schema.strip(),
                ]
            )
            results[f"{phase}_validation_seconds"] = seconds
            results[f"{phase}_validation_output"] = output
        before, after = image_size(args.baseline_validator), image_size(args.candidate_validator)
        results.update(
            {
                "baseline_controller": args.baseline_controller,
                "baseline_validator": args.baseline_validator,
                "candidate_validator": args.candidate_validator,
                "baseline_image_bytes": before,
                "candidate_image_bytes": after,
                "image_delta_bytes": after - before,
                "before_startup_seconds": copy_seconds + float(results["before_validation_seconds"]),
                "after_startup_seconds": results["after_validation_seconds"],
                "notes": (
                    "Wall times include docker launch; 2 CPU, offline, nonroot, read-only image, "
                    "private disk scratch. No image pull or cluster schedule time."
                ),
            }
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: v for k, v in results.items() if not k.endswith("_output")}, indent=2))


if __name__ == "__main__":
    main()
