"""Versioned benchmark-only fault metadata. Never pass this to production agents."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, model_validator

from benchmarks.sregym.faults.probe_catalog import CLASS_KINDS

PROBE_KINDS = frozenset(CLASS_KINDS.values())
CapabilityMode = Literal["strict", "permissive"]


class FaultCapability(BaseModel):
    """Benchmark evidence only: review status is independent of mitigation success."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    problem_id: str
    declared_index: int
    class_name: str
    fault_key: str
    required_capabilities: tuple[str, ...]
    missing_capabilities: tuple[str, ...]
    status: Literal["supported", "unreviewed", "unsupported"]


class SuiteCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: CapabilityMode = "strict"
    faults: tuple[FaultCapability, ...] = ()


@dataclass(frozen=True)
class FaultSpec:
    kind: str
    component: str
    policy_name: str | None = None
    pod_label_key: str = "io.kompose.service"
    declared_index: int = 0
    class_name: str = ""
    fault_key: str = ""
    parameters: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    targets: tuple[str, ...] = ()
    tracking_supported: bool = True
    tracking_reason: str | None = None
    # These name both the repair authority and the controller snapshot needed.
    required_capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.kind or not self.component:
            raise ValueError("fault kind and component must be non-empty")
        if self.targets and (
            any(not isinstance(target, str) or not target for target in self.targets)
            or len(set(self.targets)) != len(self.targets)
            or self.component != ",".join(self.targets)
        ):
            raise ValueError("targets must be distinct non-empty names matching component")
        if self.declared_index < 0:
            raise ValueError("declared_index must be non-negative")
        if self.tracking_supported and self.kind not in PROBE_KINDS:
            raise ValueError(f"no probe for fault kind {self.kind!r}")
        if not self.tracking_supported and not self.tracking_reason:
            raise ValueError("unsupported tracking needs a reason")

    @property
    def target_components(self) -> tuple[str, ...]:
        return self.targets or (self.component,)

    @property
    def name(self) -> str:
        return f"{self.kind}:{self.component}"


class ProblemDescription(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["sdo.sregym-fault-description/v1"] = "sdo.sregym-fault-description/v1"
    problem_id: str
    composite: bool = False
    faults: tuple[FaultSpec, ...]
    # Declared indices in actual injection order, distinct from enumeration order.
    injection_order: tuple[int, ...]

    @model_validator(mode="after")
    def check_indices(self) -> ProblemDescription:
        indices = [fault.declared_index for fault in self.faults]
        if not self.problem_id or not indices or len(set(indices)) != len(indices):
            raise ValueError("a problem needs an ID and distinct fault indices")
        if len(self.injection_order) != len(indices) or set(self.injection_order) != set(indices):
            raise ValueError("injection_order must enumerate every declared fault exactly once")
        return self

    @property
    def tracking_supported(self) -> bool:
        return all(fault.tracking_supported for fault in self.faults) and len({f.name for f in self.faults}) == len(
            self.faults
        )

    def ordered_faults(self) -> tuple[FaultSpec, ...]:
        faults = {fault.declared_index: fault for fault in self.faults}
        return tuple(faults[index] for index in self.injection_order)


# Matches the installer Role and informer snapshot at the S3 boundary. Expanding this
# set requires a production permission AND snapshot change, with authorizer tests.
SDO_CAPABILITIES = frozenset(
    {
        "application-workloads",
        "application-services",
        "application-configmaps",
        "application-networkpolicies",
        "application-pvcs",
        "application-pod-exec",
    }
)


def validate_suite(
    catalog: Mapping[str, ProblemDescription],
    *,
    require_tracking: bool = False,
    capability_mode: CapabilityMode = "strict",
) -> SuiteCapabilities:
    if capability_mode not in ("strict", "permissive"):
        raise ValueError("capability_mode must be strict or permissive")
    errors: list[str] = []
    evidence: list[FaultCapability] = []
    for problem_id, description in catalog.items():
        for fault in description.ordered_faults():
            missing = set(fault.required_capabilities) - SDO_CAPABILITIES
            evidence.append(
                FaultCapability(
                    problem_id=problem_id,
                    declared_index=fault.declared_index,
                    class_name=fault.class_name,
                    fault_key=fault.fault_key,
                    required_capabilities=fault.required_capabilities,
                    missing_capabilities=tuple(sorted(missing)),
                    status="unreviewed"
                    if any(item.startswith("unreviewed:") for item in missing)
                    else "unsupported"
                    if missing
                    else "supported",
                )
            )
            if missing and capability_mode == "strict":
                errors.append(
                    f"{problem_id}/{fault.class_name}: unsupported repair/snapshot scope {', '.join(sorted(missing))}"
                )
        if require_tracking and (
            not description.tracking_supported or any(f.kind not in PROBE_KINDS for f in description.faults)
        ):
            unsupported = [
                fault.class_name or fault.kind
                for fault in description.faults
                if not fault.tracking_supported or fault.kind not in PROBE_KINDS
            ]
            errors.append(
                f"{problem_id}: per-fault tracking unsupported for "
                f"{', '.join(unsupported) or 'duplicate probe identities'}"
            )
    if errors:
        raise ValueError("; ".join(errors))
    return SuiteCapabilities(mode=capability_mode, faults=tuple(evidence))


def describe_sregym_problems(
    problem_ids: tuple[str, ...], *, sregym_dir: Path, app_source_dir: Path | None = None
) -> dict[str, ProblemDescription]:
    """Resolve probe targets from SREGym's registered problem instances without cluster calls."""

    if not problem_ids:
        return {}
    root = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, (str(root), os.environ.get("PYTHONPATH", "")))),
    }
    if app_source_dir is not None:
        env["SREGYM_APP_SOURCE_DIR"] = str(app_source_dir)
    completed = subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(sregym_dir),
            "python",
            "-m",
            "benchmarks.sregym.faults.worker",
        ],
        input=json.dumps(problem_ids),
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"SREGym fault description failed: {(completed.stderr or completed.stdout)[-2000:]}")
    described = json.loads(completed.stdout)
    if set(described) != set(problem_ids):
        raise ValueError("SREGym returned incomplete fault descriptions")
    return {problem_id: ProblemDescription.model_validate(item) for problem_id, item in described.items()}
