"""Fastloop worker extensions for a mounted kind lane and baseline oracle checks."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any


def main() -> int:
    # The external worker and its dependencies run in SREGym's own environment.
    worker_infra: Any = importlib.import_module("sregym.worker_infra")
    worker = importlib.import_module("sregym.fastloop.worker")
    handle = worker.FastloopWorker.handle
    from benchmarks.sregym.fastloop.clean_start import install_probe_isolation

    install_probe_isolation(importlib.import_module("sregym.conductor.problems.composition.grading"))

    def prepare_astronomy() -> None:
        from benchmarks.sregym.adapter import prepare_oracle_observability

        assert worker.__file__ is not None
        prepare_oracle_observability(
            kubeconfig=Path(os.environ["KUBECONFIG"]),
            sregym_dir=Path(worker.__file__).resolve().parents[2],
            cluster=os.environ["SREGYM_KIND_CLUSTER_NAME"],
        )

    def extended_handle(self: Any, request: dict[str, Any]) -> dict[str, Any]:
        from benchmarks.sregym.fastloop.clean_start import (
            assert_clean_baseline,
            prepare_harness_traffic,
            remove_injected_objects,
        )

        # All problem instances, including those made only for grading, must read
        # the same harness traffic namespace. Existing oracle manager references
        # are preserved by changing the manager in place.
        if hasattr(self, "_problem_factory") and not getattr(self, "_harness_factory_ready", False):
            factory = self._problem_factory

            def isolated_factory(problem_id: str) -> Any:
                problem = factory(problem_id)
                if hasattr(problem, "app"):
                    prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"]))
                return problem

            self._problem_factory = isolated_factory
            self._harness_factory_ready = True
        op = request.get("op")
        if op in {"deploy", "clean_start"}:
            remove_injected_objects(Path(os.environ["KUBECONFIG"]))
        if op == "clean_start":
            problem = self._problem_factory(str(request["problem_id"]))
            if prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"])):
                problem.app.start_workload()
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            return {"success": True}
        astronomy = (
            request.get("op") == "deploy"
            and self._problem_factory(str(request["problem_id"])).namespace == "astronomy-shop"
        )
        if astronomy:
            prepare_astronomy()
        if request.get("op") == "refresh_baseline_clients":
            from benchmarks.sregym.fastloop.seed_recovery import refresh_baseline_clients

            problem = self._problem_factory(str(request["problem_id"]))
            return refresh_baseline_clients(problem, dependencies_only=request.get("dependencies_only", False))
        if request.get("op") == "baseline_oracle":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            verdict = self._grade(problem)
            if request.get("verify_traffic"):
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic

                return verify_baseline_traffic(problem, verdict)
            return verdict
        reply = handle(self, request)
        if op == "recover" and request.get("fault") is None and reply.get("success") is True:
            remove_injected_objects(Path(os.environ["KUBECONFIG"]))
        if op == "deploy" and reply.get("deployed") is False:
            problem = self._problem_factory(str(request["problem_id"]))
            if prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"])):
                problem.app.start_workload()
        if op == "deploy":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
        if astronomy:
            from benchmarks.sregym.adapter import route_astronomy_traces

            route_astronomy_traces(Path(os.environ["KUBECONFIG"]))
        return reply

    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
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
