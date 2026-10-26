"""Always-run pre-injection evidence over narrow benchmark I/O boundaries."""

from __future__ import annotations

import ipaddress
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.sregym.adapter import (
    Deadline,
    assert_host_disk_headroom,
    assert_lane_resources,
    assert_stable_workloads,
    controller_proof,
    kubectl,
    qualified_scenarios,
    setup_lineage,
    wait_for_proof,
)
from benchmarks.sregym.fastloop.clean_start import assert_clean_baseline

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.worker_client import SregymWorker


def workload_identity(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    listing = json.loads(
        kubectl(
            ["get", "deployments,statefulsets,daemonsets,pods", "-o", "json"],
            namespace=namespace,
            kubeconfig=kubeconfig,
        ).stdout
    )["items"]
    workloads = {}
    pods = [row for row in listing if row["kind"] == "Pod" and not row["metadata"].get("deletionTimestamp")]
    for row in listing:
        if row["kind"] == "Pod":
            continue
        metadata, spec = row["metadata"], row["spec"]
        selector = spec.get("selector", {}).get("matchLabels", {})
        selected = [
            pod
            for pod in pods
            if selector and all(pod["metadata"].get("labels", {}).get(key) == val for key, val in selector.items())
        ]
        status = row.get("status", {})
        desired = spec.get("replicas", 1)
        if row["kind"] == "DaemonSet":
            desired = status.get("desiredNumberScheduled", 0)
            complete = (
                status.get("updatedNumberScheduled") == desired
                and status.get("currentNumberScheduled") == desired
                and status.get("numberReady") == desired
            )
        else:
            complete = (
                status.get("updatedReplicas") == desired
                and status.get("replicas") == desired
                and status.get("readyReplicas") == desired
            )
            if row["kind"] == "StatefulSet":
                complete = complete and status.get("currentRevision") == status.get("updateRevision")
        if (
            not complete
            or status.get("observedGeneration", -1) < metadata.get("generation", 0)
            or len(selected) != desired
        ):
            raise ValueError(f"restore invariant: rollout not complete: {row['kind']}/{metadata['name']}")
        if desired == 0:
            raise ValueError(f"restore invariant: installed workload scaled to zero: {row['kind']}/{metadata['name']}")
        if not selected or any(
            not any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in pod.get("status", {}).get("conditions", [])
            )
            for pod in selected
        ):
            raise ValueError(f"restore invariant: workload not Ready: {row['kind']}/{metadata['name']}")
        workloads[f"{row['kind']}/{metadata['name']}"] = {
            "generation": metadata.get("generation"),
            "pods": sorted(
                (
                    pod["metadata"]["uid"],
                    tuple(
                        status.get("restartCount", 0) for status in pod.get("status", {}).get("containerStatuses", [])
                    ),
                )
                for pod in selected
            ),
        }
    if not workloads:
        raise ValueError("restore invariant: no installed application workloads")
    return workloads


class LaneAdmission:
    def __init__(
        self,
        environment: FastloopEnvironment,
        worker: SregymWorker,
        ops: ClusterOps,
        directory: Path,
        *,
        timeout_seconds: float = 300,
        synchronize: Callable[[], None] | None = None,
        progress: Callable[..., None] | None = None,
        before_proof: Callable[[str, set[str]], None] | None = None,
    ) -> None:
        self.environment, self.worker, self.ops = environment, worker, ops
        self.directory = directory
        self.timeout_seconds = timeout_seconds
        self.synchronize, self.progress = synchronize, progress
        self.before_proof = before_proof
        self.required_detectors = {"workload-health", "service-endpoints", "service-reachability", "traffic-health"}
        self.required_scenarios: set[str] = set()
        self._identity: dict[str, Any] | None = None
        self._sequence = 0

    def _policy(self) -> None:
        root = self.environment.workspace / ".sdo/diagnostics"
        self.required_detectors.update(path.parent.name for path in (root / "detectors/health").glob("*/manifest.yaml"))
        self.required_scenarios = set()
        for path in (root / "traffic/workloads").glob("*.yaml"):
            document = yaml.safe_load(path.read_text())
            if document.get("purpose") == "health-probe":
                self.required_scenarios.update(row["id"] for row in document.get("scenarios", []))
        if not self.required_scenarios:
            raise ValueError("eval: accepted health policy declares no synthetic traffic scenarios")

    def _windows(self, control: str) -> dict[str, Any]:
        env = self.environment
        pod = json.loads(
            kubectl(["get", "pod/sdo-prober", "-o", "json"], namespace=control, kubeconfig=env.kubeconfig).stdout
        )
        address = ipaddress.ip_address(pod["status"]["podIP"])
        host = f"[{address}]" if address.version == 6 else str(address)
        code = f"import urllib.request; print(urllib.request.urlopen('http://{host}:8080/v1/windows',timeout=5).read().decode())"
        return json.loads(
            kubectl(
                ["exec", "job/sdo-controller-run", "-c", "controller", "--", "python3", "-c", code],
                namespace=control,
                kubeconfig=env.kubeconfig,
            ).stdout
        )

    def check_resources(self, stage: str) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            evidence = assert_lane_resources(self.environment.kubeconfig, app_namespaces={self.environment.namespace})
            evidence["host_disk"] = assert_host_disk_headroom()
        except ValueError as exc:
            if self.progress:
                self.progress("lane_resources", passed=False, reason=str(exc))
            raise
        destination = self.directory / "lane-resources.json"
        destination.write_text(json.dumps(evidence, indent=2) + "\n")
        if self.progress:
            self.progress("lane_resources", evidence=str(destination))

    def prove(self, control: str, *, fresh: bool = False) -> dict[str, Any]:
        from benchmarks.sregym.fastloop.rbac import verify_rbac

        self.check_resources("entry")
        self._policy()
        if self.before_proof is not None:
            self.before_proof(control, set(self.required_detectors))
        deadline = Deadline(time.monotonic() + self.timeout_seconds, time.monotonic)
        env = self.environment
        self.directory.mkdir(parents=True, exist_ok=True)
        rbac = verify_rbac(env.kubeconfig, env.namespace)
        (self.directory / "rbac.json").write_text(json.dumps(rbac, indent=2) + "\n")
        boundary = datetime.now(timezone.utc).isoformat()
        if self.progress:
            self.progress("rbac_equality", evidence=str(self.directory / "rbac.json"))
        self._identity = workload_identity(env.kubeconfig, env.namespace)
        self._sequence += 1
        path = self.directory / f"stabilization-{self._sequence:03d}.jsonl"

        def sample() -> dict[str, Any]:
            assert_clean_baseline(env.kubeconfig, namespace=env.namespace)
            if self.progress:
                self.progress("probe_placement", namespace="sregym")
            assert_stable_workloads(self._identity or {}, workload_identity(env.kubeconfig, env.namespace))
            proof = controller_proof(self.ops.runtime_state(control), self.required_detectors, boundary)
            try:
                windows = self._windows(control)
                proof["qualified_scenarios"] = qualified_scenarios(windows, self.required_scenarios)
            except (RuntimeError, ValueError, KeyError) as exc:
                proof.update(qualified_scenarios=False, prober_error=str(exc))
            proof["passed"] = proof["passed"] and proof["qualified_scenarios"]
            if proof["passed"]:
                traffic = self.worker.request(
                    "baseline_oracle",
                    problem_id=env.deploy_problem,
                    verify_traffic=True,
                    timeout=deadline.remaining(),
                )
                proof["independent_traffic"] = traffic
                proof["passed"] = traffic.get("success") is True
                assert_stable_workloads(self._identity or {}, workload_identity(env.kubeconfig, env.namespace))
            proof["required_scenarios"] = sorted(self.required_scenarios)
            return proof

        def record(proof: dict[str, Any]) -> None:
            with path.open("a") as output:
                output.write(json.dumps(proof) + "\n")

        def restore() -> None:
            from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster
            from benchmarks.sregym.fastloop.reset import RestoreChecks, restore_application

            self.check_resources("restore")
            checks = RestoreChecks(env)
            checks.prepare(fresh=False)
            restore_application(env, KubectlApplicationCluster(env.kubeconfig), self.worker, checks=checks)
            self._identity = workload_identity(env.kubeconfig, env.namespace)

        proof = wait_for_proof(
            sample, restore, record, timeout_seconds=self.timeout_seconds, deadline=deadline, monotonic=time.monotonic
        )
        if self.progress:
            self.progress("stabilization", evidence=str(path))
        if fresh:
            if self.synchronize is None:
                raise ValueError("eval: fresh-memory proof requires synchronization from the accepted repository")
            self.synchronize()
            lineage = setup_lineage(env.workspace, env.seed_commit or "HEAD")
            destination = self.directory / "setup-lineage.json"
            destination.write_text(json.dumps(lineage, indent=2) + "\n")
            if self.progress:
                self.progress("setup_memory", evidence=str(destination))
        self.check_resources("injection")
        deadline.remaining()
        return proof


class EvalAdmissionOps:
    """Prove health while ACTIVE, before either ordinary or simultaneous injection."""

    def __init__(self, inner: ClusterOps, admission: LaneAdmission, *, fresh: bool = False) -> None:
        self._inner, self.admission, self._fresh = inner, admission, fresh

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        started = time.monotonic()
        self._inner.set_maintenance(control_namespace, paused=False, generation=generation + "-proof")
        self.admission.prove(control_namespace, fresh=self._fresh)
        self._fresh = False
        seconds = time.monotonic() - started
        result = dict(self._inner.inject_after_resume(control_namespace, generation, inject))
        result["traffic_stabilization"] = seconds
        return result
