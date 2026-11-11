"""Which SDO incidents belong to a fuzzops run.

An incident belongs to a run when SDO detected it inside the run's window, or when it was already open at the
inject (detected earlier, not completed before it) and its findings, confirmed root causes or repair actions name the
faulted component by resource reference. The second case is an incident SDO opened shortly before the inject and then
took over the injected fault.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

# Workload kinds whose Pods and ReplicaSets carry the workload name as a prefix.
_OWNED_KINDS = frozenset({"Pod", "ReplicaSet", "ControllerRevision"})
_KIND_REF = re.compile(
    r"(?<![\w.-])(?:deploy|deployment|sts|statefulset|ds|daemonset|svc|service|pod|job|cronjob|configmap|cm|secret"
    r"|pvc|rollout)/([a-z0-9][a-z0-9.-]*)"
)
_LINK_END = re.compile(r"^[a-z0-9][a-z0-9.-]*")


def fault_targets(fault: str, component: str = "") -> frozenset[str]:
    """Resource names a fuzzops fault instance (``template@kind:ref``) and its root-cause component point at.

    Workload refs are ``deploy/x``, ``sts/x:port``, ``deploy/x/container`` or ``...object:configmap/y``; a link is
    ``a->b:port`` and names both ends.
    """
    names: set[str] = set()
    target = fault.partition("@")[2]
    for text in (target, component):
        names.update(_KIND_REF.findall(text))
        if "->" in text:
            for end in text.removeprefix("link:").split("->"):
                match = _LINK_END.match(end.strip())
                if match:
                    names.add(match.group(0))
    return frozenset(names)


def _get(obj: dict[str, Any], name: str) -> Any:
    if name in obj:
        return obj[name]
    camel = re.sub(r"_([a-z])", lambda m: m.group(1).upper(), name)
    return obj.get(camel)


def _refs_of(items: Iterable[dict[str, Any]], *fields: str) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for item in items:
        for field in fields:
            value = _get(item, field)
            if isinstance(value, dict):
                refs.append(value)
            elif isinstance(value, list):
                refs += [v for v in value if isinstance(v, dict)]
    return refs


def _names(ref: dict[str, Any], targets: frozenset[str]) -> bool:
    name = str(ref.get("name") or "")
    if name in targets:
        return True
    return ref.get("kind") in _OWNED_KINDS and any(name.startswith(t + "-") for t in targets)


def concerns(doc: dict[str, Any], targets: frozenset[str], inject: datetime | None = None) -> bool:
    """Whether an outcome document (or controller state's request/result) names a target by resource reference.

    Findings and confirmed causes count while the incident is open at the inject; a repair action counts only when it
    started at or after the inject.
    """
    if not targets:
        return False
    findings = _get(doc, "findings") or []
    causes = _get(doc, "confirmed_root_causes") or []
    refs = _refs_of(findings, "primary_resource", "related_resources") + _refs_of(causes, "resources")
    actions = [
        a
        for a in _get(doc, "repair_actions") or []
        if inject is None or (_stamp(_get(a, "started_at")) or inject) >= inject
    ]
    refs += _refs_of(actions, "resources")
    return any(_names(r, targets) for r in refs)


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def belongs(
    doc: dict[str, Any],
    start: datetime,
    end: datetime,
    inject: datetime | None,
    targets: frozenset[str],
) -> bool:
    """Detected inside [start, end], or open at ``inject`` and concerning the faulted component."""
    stamps = _get(doc, "timestamps") or {}
    detected = _stamp(stamps.get("detected_at"))
    if detected is None:
        return False
    if start <= detected <= end:
        return True
    if inject is None or detected >= inject:
        return False
    completed = _stamp(stamps.get("completed_at"))
    if completed is not None and completed < inject:
        return False
    return concerns(doc, targets, inject)
