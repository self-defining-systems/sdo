"""Fastloop worker extensions for a mounted kind lane and baseline oracle checks."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.idempotent_helm import make_deploy_idempotent

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

#: Worker ops that restart or roll out workloads; each runs under the unschedulable-surge guard.
SURGE_GUARDED_OPS = frozenset({"recover", "refresh"})


def attach_existing_cluster(worker_infra: Any) -> dict[str, str]:
    """Attach using the fenced kubeconfig; never invoke a cluster factory."""
    cluster = os.environ["SREGYM_KIND_CLUSTER_NAME"]
    kubeconfig = str(Path(os.environ["KUBECONFIG"]).resolve())
    reusable, reason = worker_infra.existing_cluster_is_reusable(cluster, kubeconfig)
    if not reusable:
        raise ValueError(f"eval: refusing cluster recreation in attach-only mode: {reason}")
    worker_infra.preflight_cluster_requirements(cluster, kubeconfig)
    return {"cluster": cluster, "kubeconfig": kubeconfig}


def handle_with_surge_guard(
    handle: Callable[[Any, dict[str, Any]], dict[str, Any]],
    worker: Any,
    request: dict[str, Any],
    *,
    namespaces: Sequence[str],
    kubeconfig: Path,
    cluster_factory: Callable[[str, Path], Any] | None = None,
) -> dict[str, Any]:
    """Run one worker op; a restart that cannot surge on a 1-worker cluster is recreated, not waited on.

    Every harness path that restarts a workload (a fault's recover_fault, its client refresh, the
    fault driver's rollouts) goes through the worker, so this is the one place that owns the
    unschedulable-surge deadlock for all of them.
    """
    if request.get("op") not in SURGE_GUARDED_OPS or not namespaces:
        return handle(worker, request)
    from benchmarks.sregym.fastloop.surge_recovery import KubectlRolloutCluster, SurgeWatch, guard_blocked_surges

    factory = cluster_factory or KubectlRolloutCluster
    watches = [SurgeWatch(factory(namespace, kubeconfig), namespace) for namespace in dict.fromkeys(namespaces)]
    with guard_blocked_surges(watches):
        return handle(worker, request)


def _make_deploy_idempotent() -> None:
    make_deploy_idempotent(importlib.import_module("sregym.service.helm").Helm)


def _incident_namespaces(worker_module: Any, worker: Any) -> list[str]:
    namespaces = []
    for problem in getattr(worker, "_incidents", ()):
        try:
            namespaces.append(worker_module._app_namespace(problem))
        except ValueError:
            continue
    return namespaces


def main() -> int:
    # The external worker and its dependencies run in SREGym's own environment.
    worker_infra: Any = importlib.import_module("sregym.worker_infra")
    worker = importlib.import_module("sregym.fastloop.worker")
    handle = worker.FastloopWorker.handle
    from benchmarks.sregym.fastloop.clean_start import install_probe_isolation
    from benchmarks.sregym.fastloop.hotel_helpers import admit_install, configure_profile, prepare_install

    install_probe_isolation(importlib.import_module("sregym.conductor.problems.composition.grading"))
    _make_deploy_idempotent()

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
                    configure_profile(problem.app)
                    prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"]))
                return problem

            self._problem_factory = isolated_factory
            self._harness_factory_ready = True
        from benchmarks.sregym.fastloop.oracle_baseline import OracleBaselines, install_oracle_baselines

        if not hasattr(self, "_rehearse_oracles") and os.environ.get("FASTLOOP_ORACLE_BASELINES"):
            assert worker.__file__ is not None

            def confirm_healthy(problem: Any) -> bool:
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic

                if not self._health_probe(problem.namespace).get("healthy"):
                    return False
                return (
                    verify_baseline_traffic(problem, {"kind": "health-check", "success": True, "details": {}})[
                        "success"
                    ]
                    is True
                )

            self._rehearse_oracles = install_oracle_baselines(
                self,
                OracleBaselines(
                    Path(os.environ["FASTLOOP_ORACLE_BASELINES"]),
                    os.environ["FASTLOOP_ORACLE_IDENTITY"]
                    + ":"
                    + subprocess.check_output(
                        ["git", "-C", str(Path(worker.__file__).resolve().parents[2]), "rev-parse", "HEAD"],
                        text=True,
                        timeout=10,
                    ).strip(),
                ),
                confirm_healthy=confirm_healthy,
            )
        op = request.get("op")
        if op == "rehearse_oracles":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            return self._rehearse_oracles(str(request["problem_id"]))
        if op == "attach_existing":
            return attach_existing_cluster(worker_infra)
        if op in {"deploy", "clean_start"}:
            remove_injected_objects(Path(os.environ["KUBECONFIG"]))
        if op == "clean_start":
            problem = self._problem_factory(str(request["problem_id"]))
            if prepare_harness_traffic(
                problem.app, Path(os.environ["KUBECONFIG"]), suspended=problem.namespace == "social-network"
            ):
                problem.app.start_workload()
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            return {"success": True}
        astronomy = (
            request.get("op") == "deploy"
            and self._problem_factory(str(request["problem_id"])).namespace == "astronomy-shop"
        )
        if op == "deploy":
            problem = self._problem_factory(str(request["problem_id"]))
            if problem.namespace == "hotel-reservation":
                prepare_install(Path(os.environ["KUBECONFIG"]), problem.namespace, Path(request["baseline_path"]))
        if op == "inject":
            problem = self._problem_factory(str(request["problem_id"]))
            admit_install(Path(os.environ["KUBECONFIG"]), problem.namespace)
        if astronomy:
            prepare_astronomy()
        if request.get("op") == "refresh_baseline_clients":
            from benchmarks.sregym.fastloop.seed_recovery import refresh_baseline_clients

            problem = self._problem_factory(str(request["problem_id"]))
            return refresh_baseline_clients(problem, dependencies_only=request.get("dependencies_only", False))
        if request.get("op") == "baseline_oracle":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            problem_id = str(request["problem_id"])
            # Rehearsal classifies only once. Recovery still takes a fresh grade:
            # cached healthy observations cannot establish a restored baseline.
            if hasattr(self, "_rehearse_oracles"):
                verdict = self._rehearse_oracles(problem_id, fresh=True)
            else:
                verdict = self._grade(problem)
            if request.get("verify_traffic"):
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic

                return verify_baseline_traffic(problem, verdict)
            return verdict
        reply = handle_with_surge_guard(
            handle,
            self,
            request,
            namespaces=_incident_namespaces(worker, self),
            kubeconfig=Path(os.environ["KUBECONFIG"]),
        )
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
