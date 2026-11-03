"""Independent, conservative footprint restoration grades; never an agent input."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.adapter.kubernetes_content import declared_manifest

if TYPE_CHECKING:
    from benchmarks.sregym.protocol.injection import FaultComponent


# Only kinds covered by KubectlApplicationCluster.list_grading_objects may prove absence.
GRADING_KINDS = frozenset(
    {
        "Deployment",
        "DaemonSet",
        "StatefulSet",
        "Service",
        "ConfigMap",
        "Job",
        "CronJob",
        "Pod",
        "NetworkPolicy",
        "PersistentVolumeClaim",
        "ServiceAccount",
        "Role",
        "RoleBinding",
        "MutatingWebhookConfiguration",
        "ValidatingWebhookConfiguration",
        "Namespace",
        "ResourceQuota",
        "LimitRange",
    }
)


class FootprintGrade(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: str
    name: str
    field: str | None
    status: Literal["reverted", "not_reverted", "unknown"]
    baseline: Any = None
    live: Any = None


class FaultReversion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    fault: int
    status: Literal["reverted", "not_reverted", "unknown"]
    footprints: list[FootprintGrade] = Field(default_factory=list)


def _field(value: Any, parts: list[str]) -> Any:
    if not parts:
        return value
    if isinstance(value, list):
        # Preserve list identity as well as the selected value (ports/containers).
        return sorted([(str(item.get("name", item.get("port", ""))), _field(item, parts)) for item in value], key=str)
    if not isinstance(value, dict):
        return None
    return _field(value.get(parts[0]), parts[1:])


def _project(obj: dict[str, Any] | None, field: str | None, class_name: str) -> Any:
    if field and (field.split(".")[0] not in {"spec", "data", "binaryData", "webhooks", "metadata"} or "[" in field):
        raise ValueError("unsupported footprint field")
    if obj is None:
        return {"exists": False}
    obj = declared_manifest(obj)
    if field:
        return {"exists": True, "value": _field(obj, field.split("."))}
    paths = {
        "AssignNonExistentNode": ["spec.template.spec.nodeSelector"],
        "LivenessProbeMisconfiguration": ["spec.template.spec.containers.livenessProbe"],
    }
    selected = paths.get(class_name) if obj.get("kind") in {"Deployment", "StatefulSet", "DaemonSet"} else None
    if selected:
        return {"exists": True, **{path: _field(obj, path.split(".")) for path in selected}}
    # Whole-object footprints compare declared content, never API allocations.
    return {
        "exists": True,
        "labels": obj.get("metadata", {}).get("labels", {}),
        **{key: value for key, value in obj.items() if key not in {"metadata", "status", "apiVersion", "kind"}},
    }


def _find(objects: list[dict[str, Any]], kind: str, name: str, namespace: str | None) -> dict[str, Any] | None:
    matches = [
        obj
        for obj in objects
        if obj.get("kind") == kind
        and obj.get("metadata", {}).get("name") == name
        and (not namespace or obj.get("metadata", {}).get("namespace") == namespace)
    ]
    if len(matches) > 1:
        raise ValueError("ambiguous footprint namespace")
    return matches[0] if matches else None


def grade_faults(
    components: list[FaultComponent],
    baseline: list[dict[str, Any]] | None,
    live: list[dict[str, Any]] | None,
) -> list[FaultReversion]:
    """None means unavailable; an empty successful inventory proves absence.

    A mismatch proves non-restoration even if another footprint is unknown.
    This grades exact field restoration, not alternative valid mitigations.
    """
    grades = []
    for component in components:
        footprints = []
        for footprint in component.identity.get("footprints", []):
            kind, name, field = footprint["kind"], footprint["name"], footprint.get("field")
            before = after = None
            status: Literal["reverted", "not_reverted", "unknown"] = "unknown"
            if baseline is not None and live is not None and kind in GRADING_KINDS:
                try:
                    before = _project(
                        _find(baseline, kind, name, footprint.get("namespace")),
                        field,
                        component.identity.get("class", ""),
                    )
                    after = _project(
                        _find(live, kind, name, footprint.get("namespace")), field, component.identity.get("class", "")
                    )
                    if (
                        field
                        and before.get("exists")
                        and after.get("exists")
                        and (before.get("value") is None and after.get("value") is None)
                    ):
                        raise ValueError("field not observed in either inventory")
                    status = "reverted" if before == after else "not_reverted"
                except ValueError:
                    pass
            footprints.append(
                FootprintGrade(kind=kind, name=name, field=field, status=status, baseline=before, live=after)
            )
        statuses = [item.status for item in footprints]
        result: Literal["reverted", "not_reverted", "unknown"] = (
            "not_reverted"
            if "not_reverted" in statuses
            else "reverted"
            if statuses and all(value == "reverted" for value in statuses)
            else "unknown"
        )
        grades.append(FaultReversion(fault=component.fault, status=result, footprints=footprints))
    return grades
