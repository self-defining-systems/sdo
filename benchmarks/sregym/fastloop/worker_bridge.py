"""Fastloop worker extensions for a mounted kind lane and baseline oracle checks."""

from __future__ import annotations

import os
import sys
from typing import Any

from sregym import worker_infra
from sregym.fastloop.worker import FastloopWorker, main

_handle = FastloopWorker.handle


def _extended_handle(self: FastloopWorker, request: dict[str, Any]) -> dict[str, Any]:
    if request.get("op") == "baseline_oracle":
        problem = self._problem_factory(str(request["problem_id"]))
        return self._grade(problem)
    return _handle(self, request)


if __name__ == "__main__":
    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
    FastloopWorker.handle = _extended_handle
    sys.exit(main())
