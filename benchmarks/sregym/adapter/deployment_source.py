"""Benchmark-side builders for the production ``DeploymentSource`` (ProtoJSON), the one launcher edge format.

The controller takes ``--deployment-source <json>``; launchers never pass manifest path lists. Raw is the default
renderer; SREGym ``hotelReservation`` is a kustomize root (``kubernetes/kustomization.yaml``).
"""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import Any

_CONFIG_TYPE = "type.googleapis.com/sdodev.contracts.v1alpha1."
RAW = "raw"
KUSTOMIZE = "kustomize"


def _checked_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError(f"invalid deployment source path {path!r}")
    clean = PurePosixPath(path)
    if str(clean) != path or clean.is_absolute() or ".." in clean.parts:
        raise ValueError(f"invalid deployment source path {path!r}")
    return path


def raw_source(paths: tuple[str, ...] | list[str]) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the raw renderer over repository-relative manifest files/directories."""
    if not paths:
        raise ValueError("raw deployment source requires at least one path")
    for path in paths:
        if _checked_path(path) == ".":
            raise ValueError(f"invalid deployment source path {path!r}")
    return {"renderer_id": RAW, "config": {"@type": _CONFIG_TYPE + "RawRenderConfig", "paths": list(paths)}}


def kustomize_source(path: str) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the kustomize renderer over one kustomization directory."""
    return {
        "renderer_id": KUSTOMIZE,
        "config": {"@type": _CONFIG_TYPE + "KustomizeRenderConfig", "path": _checked_path(path)},
    }


def helm_source(chart_path: str, value_files: tuple[str, ...] = ()) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the helm renderer over one chart directory of the source tree."""
    return {
        "renderer_id": "helm",
        "config": {
            "@type": _CONFIG_TYPE + "HelmRenderConfig",
            "chart_path": _checked_path(chart_path),
            "value_files": list(value_files),
        },
    }


def source_from_config(value: Any) -> dict[str, Any]:
    """Build a source from a TOML table: raw with ``paths``, or kustomize with ``path``."""
    if not isinstance(value, dict):
        raise ValueError("deployment_source must be a table with a renderer")
    renderer = value.get("renderer")
    known = {RAW: {"renderer", "paths"}, KUSTOMIZE: {"renderer", "path"}}
    if renderer not in known:
        raise ValueError(f"deployment_source.renderer must be one of {sorted(known)}, got {renderer!r}")
    unknown = sorted(set(value) - known[renderer])
    if unknown:
        raise ValueError(f"deployment_source for renderer {renderer!r} has unknown keys {unknown}")
    if renderer == RAW:
        paths = value.get("paths")
        if not isinstance(paths, list) or not paths or any(not isinstance(p, str) or not p.strip() for p in paths):
            raise ValueError("deployment_source.paths must be a non-empty list of non-empty strings")
        return raw_source(paths)
    path = value.get("path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("deployment_source.path must be a non-empty string")
    return kustomize_source(path)


def source_flag_value(source: dict[str, Any]) -> str:
    """The compact JSON the controller's ``--deployment-source`` flag takes."""
    return json.dumps(source, separators=(",", ":"), sort_keys=True)
