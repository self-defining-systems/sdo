"""Benchmark-side builders for the production deployment override (ProtoJSON ``DeploymentPlan``).

SDO derives the deployment from the repository's files. Where the files cannot say it (several roots, values
files, image builds the Dockerfile rule does not find), the harness commits the optional override
``.sdo/deployment/plan.yaml`` into the repository as a human, like an engineer (``commit_override``); no launcher
passes a source or plan flag. SREGym ``hotelReservation`` is a kustomize root (``kubernetes/kustomization.yaml``).
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for
from benchmarks.sregym.repository_git import host_git_command

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


def buildkit_image(image: str, context_path: str, dockerfile_path: str, source_id: str = "buildkit") -> dict[str, Any]:
    """ProtoJSON ``ImageSource``: ``image`` as the reference the deployment uses, built by SDO from the source tree."""
    if not image:
        raise ValueError("an image source needs the image reference")
    return {
        "image": image,
        "source_id": source_id,
        "config": {
            "@type": _CONFIG_TYPE + "BuildKitImageConfig",
            "context_path": _checked_path(context_path),
            "dockerfile_path": _checked_path(dockerfile_path),
        },
    }


def prebuilt_image(image: str, source_id: str = "prebuilt") -> dict[str, Any]:
    """ProtoJSON ``ImageSource``: ``image`` already exists in a registry; SDO pins its tag to a digest at deploy."""
    if not image:
        raise ValueError("an image source needs the image reference")
    return {"image": image, "source_id": source_id, "config": {"@type": _CONFIG_TYPE + "PrebuiltImageConfig"}}


def deployment_plan(
    source: dict[str, Any] | None = None,
    *,
    images: tuple[dict[str, Any], ...] | list[dict[str, Any]] = (),
) -> dict[str, Any]:
    """ProtoJSON ``DeploymentPlan`` override: the source (and image declarations) the files cannot say."""
    plan: dict[str, Any] = {}
    if source is not None:
        plan["source"] = source
    if images:
        plan["images"] = list(images)
    return plan


OVERRIDE_PATH = ".sdo/deployment/plan.yaml"
_OVERRIDE_MESSAGE = (
    "sdo: record the deployment override\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing"
)


def override_text(plan: dict[str, Any]) -> str:
    """The stored form of the override: compact JSON (also YAML) with a trailing newline."""
    return json.dumps(plan, separators=(",", ":"), sort_keys=True) + "\n"


def commit_override(repository: Path, plan: dict[str, Any]) -> bool:
    """Commit ``plan`` as the repository's override in a human commit; return whether a commit was made.

    A repository whose override already says exactly this is left alone, so a repeated install commits nothing.
    """
    path = repository / OVERRIDE_PATH
    text = override_text(plan)
    if path.is_file() and path.read_text(encoding="utf-8") == text:
        status = subprocess.run(
            host_git_command(repository, "status", "--porcelain", "--", OVERRIDE_PATH),
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.COMMAND),
        ).stdout
        if not status.strip():
            return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    subprocess.run(
        host_git_command(repository, "add", "--force", "--", OVERRIDE_PATH),
        check=True,
        timeout=wait_for(WaitKind.COMMAND),
    )
    subprocess.run(
        host_git_command(repository, "commit", "-q", "-m", _OVERRIDE_MESSAGE, "--", OVERRIDE_PATH),
        check=True,
        timeout=wait_for(WaitKind.COMMAND),
    )
    return True


@dataclass(frozen=True)
class DeploymentPolicyConfig:
    """The install-time deployment policy a launcher hands the Go installer (config ``deployment.policy``).

    The default is no policy: no registry allowlist and foreign or cluster-scoped objects are notes, today's behavior.
    Production owns the semantics and the validation (``deploymentpolicy``); this only names the lane's choices.
    """

    allowed_registries: tuple[str, ...] = ()
    scope_blocking: bool = False
    granted_namespaces: tuple[str, ...] = ()
    granted_cluster_kinds: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Content rules (host charset, grants needing scope_blocking) belong to
        # Go's deploymentpolicy.Validate, which the installer runs on the resolved config.
        if type(self.scope_blocking) is not bool:
            raise TypeError("scope_blocking must be a boolean")

    def settings(self) -> dict[str, object]:
        """The ``deployment.policy`` config object; empty when there is no policy."""
        values: dict[str, object] = {
            "allowed_registries": list(self.allowed_registries),
            "scope_blocking": self.scope_blocking,
            "granted_namespaces": list(self.granted_namespaces),
            "granted_cluster_kinds": list(self.granted_cluster_kinds),
        }
        return {key: value for key, value in values.items() if value}
