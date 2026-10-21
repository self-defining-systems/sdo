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

    def extended_handle(self: Any, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("op") == "baseline_oracle":
            problem = self._problem_factory(str(request["problem_id"]))
            return self._grade(problem)
        return handle(self, request)

    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
    worker.FastloopWorker.handle = extended_handle
    return worker.main()


if __name__ == "__main__":
    sys.exit(main())
