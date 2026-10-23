"""Fastloop worker extensions for a mounted kind lane and baseline oracle checks."""

from __future__ import annotations

import importlib
import os
import sys
from typing import Any


def main() -> int:
    # The external worker and its dependencies run in SREGym's own environment.
    worker_infra: Any = importlib.import_module("sregym.worker_infra")
    worker = importlib.import_module("sregym.fastloop.worker")
    handle = worker.FastloopWorker.handle

    def prepare_astronomy() -> None:
        from pathlib import Path

        from benchmarks.sregym.adapter import prepare_oracle_observability

        assert worker.__file__ is not None
        prepare_oracle_observability(
            kubeconfig=Path(os.environ["KUBECONFIG"]),
            sregym_dir=Path(worker.__file__).resolve().parents[2],
            cluster=os.environ["SREGYM_KIND_CLUSTER_NAME"],
        )

    def extended_handle(self: Any, request: dict[str, Any]) -> dict[str, Any]:
        astronomy = (
            request.get("op") == "deploy"
            and self._problem_factory(str(request["problem_id"])).namespace == "astronomy-shop"
        )
        if astronomy:
            prepare_astronomy()
        if request.get("op") == "baseline_oracle":
            problem = self._problem_factory(str(request["problem_id"]))
            return self._grade(problem)
        reply = handle(self, request)
        if astronomy:
            from pathlib import Path

            from benchmarks.sregym.adapter import route_astronomy_traces

            route_astronomy_traces(Path(os.environ["KUBECONFIG"]))
        return reply

    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
    worker.FastloopWorker.handle = extended_handle
    return worker.main()


if __name__ == "__main__":
    sys.exit(main())
