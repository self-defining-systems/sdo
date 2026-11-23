"""Fastloop worker extensions for a mounted kind lane and baseline oracle checks."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.lanes.eks import Ecr, EksLane, assert_lane_cluster, publish_app_image
from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.app_images import install_build_lock, install_shared_builds
from benchmarks.sregym.fastloop.host_limits import HostLimits, install_node_limits
from benchmarks.sregym.fastloop.idempotent_helm import make_deploy_idempotent
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

#: Worker ops that restart or roll out workloads; each runs under the unschedulable-surge guard.
SURGE_GUARDED_OPS = frozenset({"recover", "refresh"})


def eks_lane_from_env() -> EksLane | None:
    """The EKS lane this worker drives (``FASTLOOP_EKS_LANE``), or None on a kind lane."""
    raw = os.environ.get("FASTLOOP_EKS_LANE", "").strip()
    return EksLane.model_validate_json(raw) if raw else None


def attach_existing_cluster(worker_infra: Any) -> dict[str, str]:
    """Attach using the fenced kubeconfig; never invoke a cluster factory."""
    cluster = os.environ["SREGYM_KIND_CLUSTER_NAME"]
    kubeconfig = str(Path(os.environ["KUBECONFIG"]).resolve())
    lane = eks_lane_from_env()
    if lane is not None:
        # No kind registry or node images to check: the fence is the API server and the cluster's identity.
        assert_lane_cluster(kubeconfig, lane)
        return {"cluster": cluster, "kubeconfig": kubeconfig}
    reusable, reason = worker_infra.existing_cluster_is_reusable(cluster, kubeconfig)
    if not reusable:
        raise ValueError(f"eval: refusing cluster recreation in attach-only mode: {reason}")
    worker_infra.preflight_cluster_requirements(cluster, kubeconfig)
    return {"cluster": cluster, "kubeconfig": kubeconfig}


def install_eks_delivery(worker_infra: Any, source_deploy: Any, ecr: Ecr, *, record_path: Path | None = None) -> None:
    """An EKS lane's app images: SREGym builds from source on this host, and the image is published under the name
    its manifests declare by a push to the lane's node mirror. Nothing is loaded into nodes, so the kind loaders fail
    loudly if anything still calls them. Every EKS lane of this host shares the local build cache, so the kind lanes'
    shared host registry is not used.

    Every push is recorded in ``record_path`` (one JSON line: declared name, built image, digest). The declared name is
    one mutable tag shared by every lane of the application on the host cluster, so a row's report can show which
    bytes it deployed and whether another lane of the host pushed different ones under the same name."""

    def no_kind(*_args: object, **_kwargs: object) -> Any:
        raise RuntimeError("an EKS lane cannot load images into kind nodes")

    worker_infra.ensure_kind_images = no_kind
    worker_infra.ensure_kind_platform_images = no_kind
    worker_infra.tag_image_in_kind_nodes = no_kind
    worker_infra.kind_nodes_hold_image = no_kind

    def published_by_declaration(*, cluster_name: str, image_ref: str) -> None:
        del cluster_name, image_ref  # the declared name is what nodes pull; see declare()

    def declare(*, cluster_name: str, built_ref: str, declared_tag: str) -> None:
        del cluster_name
        repository = built_ref.rpartition(":")[0]
        digest = publish_app_image(ecr, built_ref, f"{repository}:{declared_tag}")
        print(f"app image {repository}:{declared_tag} published to the node mirror as {digest}", file=sys.stderr)
        if record_path is not None:
            record_path.parent.mkdir(parents=True, exist_ok=True)
            with record_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({"declared": f"{repository}:{declared_tag}", "built": built_ref, "digest": digest})
                    + "\n"
                )

    source_deploy._kind_load_image = published_by_declaration
    source_deploy._declare_image = declare


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
    eks_lane = eks_lane_from_env()
    if eks_lane is None:
        install_node_limits(worker_infra)
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
                    from benchmarks.sregym.fastloop.source_deployment import configure_source_deployment

                    configure_profile(problem.app)
                    configure_source_deployment(problem.app)
                    prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"]))
                return problem

            self._problem_factory = isolated_factory
            self._harness_factory_ready = True
        from benchmarks.sregym.fastloop.oracle_baseline import OracleBaselines, install_oracle_baselines

        if not hasattr(self, "_rehearse_oracles") and os.environ.get("FASTLOOP_ORACLE_BASELINES"):
            assert worker.__file__ is not None

            from benchmarks.sregym.fastloop.workload_readiness import sustained_pod_health

            def confirm_healthy(problem: Any) -> dict[str, Any]:
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic
                from benchmarks.sregym.fastloop.workload_readiness import await_workload_identity

                await_workload_identity(Path(os.environ["KUBECONFIG"]), problem.namespace)
                health = self._health_probe(problem.namespace)
                return verify_baseline_traffic(
                    problem,
                    {
                        "kind": "health-check",
                        "success": health.get("healthy") is True,
                        "details": health,
                    },
                )

            self._rehearse_oracles = install_oracle_baselines(
                self,
                OracleBaselines(
                    Path(os.environ["FASTLOOP_ORACLE_BASELINES"]),
                    os.environ["FASTLOOP_ORACLE_IDENTITY"]
                    + ":"
                    + subprocess.check_output(
                        host_git_command(Path(worker.__file__).resolve().parents[2], "rev-parse", "HEAD"),
                        text=True,
                        timeout=wait_for(WaitKind.PROBE),
                    ).strip(),
                ),
                confirm_healthy=confirm_healthy,
                pod_health_gate=lambda problem: sustained_pod_health(Path(os.environ["KUBECONFIG"]), problem.namespace),
            )
        op = request.get("op")
        if op == "rehearse_oracles":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
            return self._rehearse_oracles(str(request["problem_id"]), fresh=request.get("fresh") is True)
        if op == "sustained_health":
            from benchmarks.sregym.fastloop.workload_readiness import sustained_pod_health

            problems = sustained_pod_health(Path(os.environ["KUBECONFIG"]), str(request["namespace"]))
            return {"success": not problems, "problems": problems}
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
                verdict = self._rehearse_oracles(problem_id, fresh=True, sustain=request.get("sustain") is not False)
            else:
                verdict = self._grade(problem)
            if request.get("verify_traffic"):
                from benchmarks.sregym.fastloop.seed_recovery import verify_baseline_traffic

                return verify_baseline_traffic(problem, verdict)
            return verdict
        from benchmarks.sregym.fastloop.source_deployment import APP_REVISION_ENV

        pinned = request.get("revision") if op == "deploy" else None
        if pinned:
            os.environ[APP_REVISION_ENV] = str(pinned)
        try:
            reply = handle_with_surge_guard(
                handle,
                self,
                request,
                namespaces=_incident_namespaces(worker, self),
                kubeconfig=Path(os.environ["KUBECONFIG"]),
            )
        finally:
            os.environ.pop(APP_REVISION_ENV, None)
        if op == "recover" and request.get("fault") is None and reply.get("success") is True:
            remove_injected_objects(Path(os.environ["KUBECONFIG"]))
        if op == "deploy" and reply.get("deployed") is False:
            problem = self._problem_factory(str(request["problem_id"]))
            if prepare_harness_traffic(problem.app, Path(os.environ["KUBECONFIG"])):
                problem.app.start_workload()
        if op == "deploy":
            problem = self._problem_factory(str(request["problem_id"]))
            assert_clean_baseline(Path(os.environ["KUBECONFIG"]), namespace=problem.namespace)
        return reply

    if "FASTLOOP_KIND_CONFIG" in os.environ:
        worker_infra.worker_kind_config_path = lambda: os.environ["FASTLOOP_KIND_CONFIG"]
    from benchmarks.sregym.fastloop.toy_images import load_images

    def deliver(cluster: str, images: list[str]) -> dict[str, dict[str, str]]:
        return load_images(cluster, images, record_path=Path(os.environ["FASTLOOP_DELIVERY_RECORD"]))

    def deliver_platform(cluster: str, images: list[str], _platform_name: str) -> dict[str, dict[str, str]]:
        return deliver(cluster, images)

    source_deploy: Any = importlib.import_module("sregym.service.source_deploy")
    if eks_lane is not None:
        delivery = os.environ.get("FASTLOOP_DELIVERY_RECORD")
        install_eks_delivery(
            worker_infra,
            source_deploy,
            Ecr(eks_lane),
            record_path=Path(delivery).parent / "published_app_images.jsonl" if delivery else None,
        )
        # No shared registry on EKS (nodes pull from ECR), but one build per identity on this host still holds.
        install_build_lock(source_deploy, lock_dir=HostLimits.from_env().directory)
    else:
        worker_infra.ensure_kind_images = deliver
        worker_infra.ensure_kind_platform_images = deliver_platform

        def deliver_source(*, cluster_name: str, image_ref: str) -> None:
            deliver(cluster_name, [image_ref])

        source_deploy._kind_load_image = deliver_source
        record = os.environ.get("FASTLOOP_DELIVERY_RECORD")
        install_shared_builds(
            source_deploy,
            record_path=Path(record).parent / "app_images.jsonl" if record else None,
            lock_dir=HostLimits.from_env().directory,
        )
    worker.FastloopWorker.handle = extended_handle
    return worker.main()


if __name__ == "__main__":
    sys.exit(main())
