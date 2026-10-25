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
            verdict = self._grade(problem)
            if request.get("verify_traffic"):
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic

                return verify_baseline_traffic(problem, verdict)
            return verdict
        reply = handle(self, request)
        if astronomy:
            from pathlib import Path

            from benchmarks.sregym.adapter import route_astronomy_traces

            route_astronomy_traces(Path(os.environ["KUBECONFIG"]))
        return reply

    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
    from pathlib import Path

    from benchmarks.sregym.fastloop.toy_images import load_images

    def deliver(cluster: str, images: list[str]) -> dict[str, dict[str, str]]:
        return load_images(cluster, images, record_path=Path(os.environ["FASTLOOP_DELIVERY_RECORD"]))

    def deliver_platform(cluster: str, images: list[str], _platform_name: str) -> dict[str, dict[str, str]]:
        return deliver(cluster, images)

    worker_infra.ensure_kind_images = deliver
    worker_infra.ensure_kind_platform_images = deliver_platform
    source_deploy: Any = importlib.import_module("sregym.service.source_deploy")

    def deliver_source(*, cluster_name: str, image_ref: str) -> None:
        deliver(cluster_name, [image_ref])

    source_deploy._kind_load_image = deliver_source
    worker.FastloopWorker.handle = extended_handle
    return worker.main()


if __name__ == "__main__":
    sys.exit(main())
