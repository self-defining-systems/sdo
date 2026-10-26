"""Versioned benchmark-only fault metadata. Never pass this to production agents."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict, model_validator

from benchmarks.sregym.faults.probe_catalog import CLASS_KINDS
from benchmarks.sregym.faults.sregym_subprocess import SregymSubprocessError, run_sregym_json, sregym_env

PROBE_KINDS = frozenset(CLASS_KINDS.values())
CapabilityMode = Literal["strict", "permissive"]


class FaultDescriptionError(SregymSubprocessError, ValueError):
    """SREGym could not describe the selected problems."""


class CompositeConflictError(ValueError):
    """A composite whose faults cannot all be installed; SREGym would burn its settle timeouts finding out."""

    reason = "composite_fault_conflict"

    def __init__(self, conflicts: dict[str, tuple[str, ...]]) -> None:
        self.conflicts = conflicts
        super().__init__(
            f"{self.reason}: "
            + "; ".join(f"{problem_id}: {', '.join(items)}" for problem_id, items in sorted(conflicts.items()))
        )


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


# A fault of the first kind leaves its component's pods unsettled for good, so SREGym's settle wait for a later
# fault of the second kind on the same component times out (300 s, retried once) and the injection is invalid.
UNSETTLING_FAULT_BLOCKS: dict[str, frozenset[str]] = {
    "rolling_update_misconfigured": frozenset({"missing_service"}),
}


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

    def conflicts(self) -> tuple[str, ...]:
        """Faults that cannot be installed after an earlier fault of this same composite."""

        ordered = self.ordered_faults()
        return tuple(
            f"{later.name} cannot follow {earlier.name}"
            for position, earlier in enumerate(ordered)
            for later in ordered[position + 1 :]
            if later.kind in UNSETTLING_FAULT_BLOCKS.get(earlier.kind, frozenset())
            and set(earlier.target_components) & set(later.target_components)
        )


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


def check_composites(catalog: Mapping[str, ProblemDescription]) -> None:
    """Fail before deploy, with a typed reason, on composites SREGym cannot install."""

    conflicts = {problem_id: found for problem_id, item in catalog.items() if (found := item.conflicts())}
    if conflicts:
        raise CompositeConflictError(conflicts)


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
    env = sregym_env(
        os.environ, PYTHONPATH=os.pathsep.join(filter(None, (str(root), os.environ.get("PYTHONPATH", ""))))
    )
    if app_source_dir is not None:
        env["SREGYM_APP_SOURCE_DIR"] = str(app_source_dir)
    described = run_sregym_json(
        "fault description",
        ["uv", "run", "--directory", str(sregym_dir), "python", "-m", "benchmarks.sregym.faults.worker"],
        input_text=json.dumps(problem_ids),
        env=env,
        timeout=120,
        error=FaultDescriptionError,
    )
    if set(described) != set(problem_ids):
        raise ValueError("SREGym returned incomplete fault descriptions")
    return {problem_id: ProblemDescription.model_validate(item) for problem_id, item in described.items()}
