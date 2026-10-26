"""Opt-in host lease and metadata-only Secret fence for delegated fastloop runs.

Invoke as ``python -m ...lane_guard --cluster NAME --kubeconfig PATH --lease ID
-- MODULE [ARGS...]``. The same fence follows worker and dry-run subprocesses.
No application, traffic, grading or cleanup behavior is changed by this wrapper.
"""

from __future__ import annotations

import argparse
import os
import re
import runpy
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LaneGuard:
    cluster: str
    kubeconfig: Path
    lease: str
    coord: str = "coord"

    def __post_init__(self) -> None:
        if not self.cluster or not self.lease:
            raise ValueError("lane guard requires cluster and lease identities")

    def command(self, args: list[str]) -> list[str]:
        values = list(args)
        executable = Path(values[0]).name if values else ""
        if executable not in {"kubectl", "docker", "kind"}:
            return values
        prefix = self.cluster.rsplit("-", 1)[0] + "-"
        for value in values:
            if value.startswith(prefix) and not re.fullmatch(
                re.escape(self.cluster) + r"(?:-control-plane|-worker\d*|-registry)?", value
            ):
                raise ValueError("lane guard rejects foreign cluster target")
        if executable != "kubectl":
            return values
        for index, value in enumerate(values):
            if value in {"--context", "--server"} or value.startswith(("--context=", "--server=")):
                raise ValueError("lane guard rejects alternate API target")
            if value == "--kubeconfig" and (index + 1 == len(values) or values[index + 1] != str(self.kubeconfig)):
                raise ValueError("lane guard rejects foreign kubeconfig")
            if value.startswith("--kubeconfig=") and value.split("=", 1)[1] != str(self.kubeconfig):
                raise ValueError("lane guard rejects foreign kubeconfig")
            if re.search(r"/secrets(?:/|\?|$)", value):
                raise ValueError("lane guard rejects Secret API paths")
        resources = {part.lower().split("/", 1)[0] for value in values for part in value.split(",")}
        if not resources & {"secret", "secrets"} or not resources & {"get", "list", "watch"}:
            return values
        if "get" not in values or any(value in {"--watch", "-w"} or value.startswith("--watch=") for value in values):
            raise ValueError("lane guard rejects Secret list/watch")
        outputs = []
        for index, value in enumerate(values):
            if value in {"-o", "--output"}:
                outputs.append(values[index + 1] if index + 1 < len(values) else "")
            elif value.startswith(("--output=", "-o=")):
                outputs.append(value.split("=", 1)[1])
            elif value.startswith("-o") and value != "-o":
                outputs.append(value[2:])
        allowed = {"name", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name"}
        if len(outputs) > 1 or any(value not in allowed for value in outputs):
            raise ValueError("lane guard permits only Secret namespace/name metadata")
        return values if outputs else [*values, "-o", "name"]

    def install(self) -> None:
        """Fence each subprocess and each long-lived worker request."""
        original = subprocess.Popen
        guard = self
        os.environ["KUBECONFIG"] = str(self.kubeconfig)

        def check() -> None:
            with original(
                [guard.coord, "check", guard.cluster, "--lease", guard.lease],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as process:
                _, error = process.communicate()
                if process.returncode:
                    raise RuntimeError("lane lease fence rejected operation: " + error.decode())

        class FencedPopen(original):
            def __init__(self, args: Any, *positional: Any, **kwargs: Any) -> None:
                check()
                if isinstance(args, (list, tuple)):
                    values = guard.command([str(value) for value in args])
                    for module in ("benchmarks.sregym.fastloop.worker_bridge", "benchmarks.sregym.fastloop.dry_run"):
                        if "-m" in values and values[values.index("-m") + 1] == module:
                            position = values.index("-m")
                            values[position + 1 : position + 2] = [
                                "benchmarks.sregym.fastloop.lane_guard",
                                "--cluster",
                                guard.cluster,
                                "--kubeconfig",
                                str(guard.kubeconfig),
                                "--lease",
                                guard.lease,
                                "--coord",
                                guard.coord,
                                "--",
                                module,
                            ]
                            break
                    args = values
                super().__init__(args, *positional, **kwargs)

        subprocess.Popen = FencedPopen
        from benchmarks.sregym.fastloop.worker_client import SregymWorker

        request = SregymWorker.request

        def fenced_request(self: SregymWorker, op: str, *, timeout: float = 1800, **args: Any) -> dict[str, Any]:
            check()
            return request(self, op, timeout=timeout, **args)

        SregymWorker.request = fenced_request


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--lease", required=True)
    parser.add_argument("--coord", default="coord")
    parser.add_argument("module_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    module_args = args.module_args[1:] if args.module_args[:1] == ["--"] else args.module_args
    if not module_args:
        parser.error("a module is required after --")
    LaneGuard(args.cluster, args.kubeconfig, args.lease, args.coord).install()
    sys.argv = module_args
    runpy.run_module(module_args[0], run_name="__main__")


if __name__ == "__main__":
    main()
