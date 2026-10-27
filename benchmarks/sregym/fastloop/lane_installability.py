"""Offline injector admission against recorded lane topology, before row setup.

These are benchmark injection prerequisites, never production repair capabilities.
Missing recorded evidence is not permission to spend a lane discovering it live.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.fastloop.lane_guard import FaultNotInstallableError, LaneGuard, assert_faults_installable

if TYPE_CHECKING:
    from collections.abc import Mapping

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.faults.description import ProblemDescription


class LaneTopology(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    worker_nodes: int | None = Field(default=None, ge=0)
    control_plane_nodes: int = Field(default=1, ge=0)
    features: frozenset[str] = frozenset()

    @classmethod
    def of(cls, environment: FastloopEnvironment) -> LaneTopology:
        """Old lanes retain their recorded kind settings; never infer live topology."""
        if environment.lane_topology is not None:
            return environment.lane_topology
        workers = environment.worker_env.get("SREGYM_KIND_WORKER_NODES")
        features = set()
        if environment.worker_env.get("SREGYM_KIND_REQUIRE_NETWORK_POLICY") == "1":
            features.add("network-policy")
        return cls(worker_nodes=int(workers) if workers is not None else None, features=frozenset(features))


@dataclass(frozen=True)
class LanePlanResult:
    status: Literal["runnable", "not_runnable_on_lane"]
    reasons: tuple[str, ...] = ()


# One owner for prerequisites, shared by single rows and sweep planners. These
# include faults outside the tracked seed plan so composed rows use the same rule.
_MIN_WORKERS = {
    "internal_traffic_policy_local_astronomy_shop": 2,
    "node_conntrack_exhaustion_hotel_reservation": 2,
    "calico_route_reflector_label_drift": 2,
    "kubelet_eviction_threshold_misconfig": 1,
}
_FEATURES = {
    "network_policy": "network-policy",
    "service_dns_resolution_failure_netpol": "network-policy",
    "hpa_missing_effective_cpu_request": "metrics-server",
    "calico_route_reflector_label_drift": "calico-bgp",
}
_CLASS_KINDS = {
    "NodeConntrackExhaustionHotelReservation": "node_conntrack_exhaustion_hotel_reservation",
    "CalicoRouteReflectorLabelDriftHotelReservation": "calico_route_reflector_label_drift",
    "KubeletEvictionThresholdMisconfig": "kubelet_eviction_threshold_misconfig",
}


def lane_plan(
    descriptions: Mapping[str, ProblemDescription],
    topology: LaneTopology,
    sregym_dir: Path,
    *,
    guard: LaneGuard | None = None,
) -> dict[str, LanePlanResult]:
    """Return attributable decisions for every row; callers can filter before setup."""
    guard = guard or LaneGuard("offline-lane", Path("/offline-kubeconfig"), "plan")
    results = {}
    for problem_id, description in descriptions.items():
        reasons = []
        for fault in description.faults:
            kind = _CLASS_KINDS.get(fault.class_name, fault.kind)
            minimum = _MIN_WORKERS.get(kind)
            if minimum is not None and (topology.worker_nodes is None or topology.worker_nodes < minimum):
                count = "two" if minimum == 2 else "one"
                reasons.append(
                    f"{fault.class_name or kind}: requires at least {count} worker nodes; "
                    f"recorded={topology.worker_nodes}"
                )
            feature = _FEATURES.get(kind)
            if feature and feature not in topology.features:
                reasons.append(f"{fault.class_name or kind}: lane has no recorded {feature} support")
            if kind == "calico_route_reflector_label_drift" and topology.control_plane_nodes != 1:
                reasons.append(f"{kind}: requires exactly one control-plane node")
            if kind.startswith("secret_rotation") or fault.class_name.startswith("SecretRotation"):
                reasons.append(f"{fault.class_name or kind}: injector Secret read is blocked by the lane guard")
        try:
            assert_faults_installable({problem_id: description}, sregym_dir, guard)
        except FaultNotInstallableError as exc:
            reasons.append(str(exc))
        results[problem_id] = LanePlanResult("not_runnable_on_lane" if reasons else "runnable", tuple(reasons))
    return results


def require_runnable(
    descriptions: Mapping[str, ProblemDescription],
    environment: FastloopEnvironment,
    *,
    report_path: Path | None = None,
) -> dict[str, LanePlanResult]:
    results = lane_plan(
        descriptions,
        LaneTopology.of(environment),
        environment.sregym_dir,
        guard=LaneGuard(environment.cluster, environment.kubeconfig, "plan"),
    )
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({key: asdict(item) for key, item in results.items()}, indent=2) + "\n")
    rejected = {key: item.reasons for key, item in results.items() if item.status != "runnable"}
    if rejected:
        raise FaultNotInstallableError(
            "not_runnable_on_lane: " + "; ".join(f"{key}: {', '.join(reasons)}" for key, reasons in rejected.items())
        )
    return results
