"""Kubernetes declared content, excluding API-assigned identity and allocation fields."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

# Paths shared by capture, comparison, and restore. A wildcard selects list items.
ALLOCATED_FIELDS = (
    ("*", ("status",)),
    *(
        ("*", ("metadata", name))
        for name in ("uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "selfLink")
    ),
    *(
        ("Service", ("spec", name))
        for name in ("clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy", "healthCheckNodePort")
    ),
    ("Service", ("spec", "ports", "*", "nodePort")),
    ("PersistentVolumeClaim", ("spec", "volumeName")),
    ("Pod", ("spec", "nodeName")),
    ("Job", ("spec", "selector")),
    *(
        ("Job", (*prefix, label))
        for prefix in (("metadata", "labels"), ("spec", "template", "metadata", "labels"))
        for label in ("controller-uid", "batch.kubernetes.io/controller-uid")
    ),
)


def allocated_paths(value: dict[str, Any]) -> list[tuple[str, ...]]:
    headless = value["kind"] == "Service" and value.get("spec", {}).get("clusterIP") == "None"
    return [
        path
        for kind, path in ALLOCATED_FIELDS
        if kind in ("*", value["kind"]) and not (headless and path in (("spec", "clusterIP"), ("spec", "clusterIPs")))
    ]


def replace_allocated(target: Any, live: Any, path: tuple[str, ...]) -> None:
    key, *remaining = path
    if key == "*":
        if not isinstance(target, list):
            return
        current = live if isinstance(live, list) else []
        for index, item in enumerate(target):
            # Service ports retain allocations by name when the desired list moves.
            matches = [
                v
                for v in current
                if isinstance(v, dict) and isinstance(item, dict) and item.get("name") and v.get("name") == item["name"]
            ]
            prior = matches[0] if matches else current[index] if index < len(current) else None
            replace_allocated(item, prior, tuple(remaining))
        return
    if not isinstance(target, dict):
        return
    current = live if isinstance(live, dict) else {}
    if not remaining:
        if key in current:
            target[key] = deepcopy(current[key])
        else:
            target.pop(key, None)
        return
    if key not in target and key in current and isinstance(current[key], dict):
        target[key] = {}
    if key in target:
        replace_allocated(target[key], current.get(key), tuple(remaining))


def declared_manifest(value: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(value)
    for path in allocated_paths(result):
        replace_allocated(result, None, path)
    return result
