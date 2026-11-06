"""The accepted source as the one input to every eval restore decision.

SDO renders accepted source straight from Git (``rendered.Source``): the
accepted commit's tree rendered by the deployment source's renderer, each
object defaulted to the application namespace. Restore reads the same bytes
the same way, through the production ``render-source`` command. It never
reads the rendered-object snapshot an old seed may carry under ``.sdo``: that
snapshot stops matching the source as soon as the source changes.
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from functools import cached_property
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Final

from benchmarks.sregym.adapter import WaitKind, execute_operator, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT_MANIFEST_PATHS = ("kubernetes",)
# Applications whose ``kubernetes/`` holds more than the deployed manifest set.
APPLICATION_MANIFEST_PATHS = {"opentelemetry-demo-astronomy-shop": ("kubernetes/astronomy-shop.yaml",)}
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")
_CONFIG_TYPE: Final = "type.googleapis.com/sdodev.contracts.v1alpha1."


def raw_deployment_source(paths: tuple[str, ...]) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the raw renderer over manifest paths."""
    return {"renderer_id": "raw", "config": {"@type": _CONFIG_TYPE + "RawRenderConfig", "paths": list(paths)}}


def kustomize_deployment_source(path: str) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the kustomize renderer over one kustomization directory."""
    return {"renderer_id": "kustomize", "config": {"@type": _CONFIG_TYPE + "KustomizeRenderConfig", "path": path}}


def helm_deployment_source(chart_path: str, value_files: tuple[str, ...] = ()) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the helm renderer over one chart directory of the source tree."""
    return {
        "renderer_id": "helm",
        "config": {
            "@type": _CONFIG_TYPE + "HelmRenderConfig",
            "chart_path": chart_path,
            "value_files": list(value_files),
        },
    }


def manifest_paths_for(application: str) -> tuple[str, ...]:
    """Manifest selection for a problem application name (never a lane-qualified one)."""
    return APPLICATION_MANIFEST_PATHS.get(application, DEFAULT_MANIFEST_PATHS)


def manifest_paths_for_environment(environment: Any) -> tuple[str, ...]:
    """The one environment-to-manifests mapping: keyed by the problem application, never ``environment.application``.

    Shared attachment renames ``environment.application`` to a lane-qualified
    name; that name selects nothing. An environment without a recorded problem
    application cannot say which manifests it deploys, so it raises.
    """
    application = environment.problem_application
    if not application:
        raise ValueError(
            f"environment {environment.application!r} has no problem application; "
            "it cannot select its accepted manifests"
        )
    return manifest_paths_for(application)


@dataclass(frozen=True)
class AcceptedSource:
    """Rendered manifests of the accepted commit checked out at ``workspace``."""

    workspace: Path
    namespace: str
    manifest_paths: tuple[str, ...] = DEFAULT_MANIFEST_PATHS
    # ProtoJSON DeploymentSource; ``None`` means the raw renderer over ``manifest_paths``.
    renderer_source: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("accepted source requires the application namespace")
        if not self.manifest_paths:
            raise ValueError("accepted source requires at least one manifest path")
        for path in self.manifest_paths:
            clean = PurePosixPath(path)
            if not path or str(clean) != path or clean.is_absolute() or ".." in clean.parts or path == ".":
                raise ValueError(f"invalid accepted manifest path {path!r}")

    @property
    def deployment_source(self) -> dict[str, Any]:
        """The DeploymentSource the production renderer reads this source with."""
        if self.renderer_source is not None:
            return self.renderer_source
        return raw_deployment_source(self.manifest_paths)

    @classmethod
    def of(cls, environment: Any) -> AcceptedSource:
        return cls(environment.workspace, environment.namespace, manifest_paths_for_environment(environment))

    def _git(self, *args: str) -> str:
        result = subprocess.run(
            host_git_command(self.workspace, *args),
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.PROBE),
        )
        return result.stdout

    @cached_property
    def revision(self) -> str:
        value = self._git("rev-parse", "HEAD").strip()
        if not _REVISION.match(value):
            raise ValueError(f"accepted workspace has no commit: {self.workspace}")
        return value

    @cached_property
    def manifests(self) -> tuple[dict[str, Any], ...]:
        """Use the production renderer for plain manifests and generators alike."""
        try:
            rendered = json.loads(
                execute_operator(
                    "render-source",
                    [],
                    timeout=wait_for(WaitKind.COMMAND),
                    input_text=json.dumps(
                        {
                            "repository": str(self.workspace.resolve()),
                            "revision": self.revision,
                            "namespace": self.namespace,
                            "deployment_source": self.deployment_source,
                        }
                    ),
                )
            )
        except RuntimeError as error:
            raise ValueError(f"accepted source render failed at {self.revision}: {error}") from error
        if not isinstance(rendered, list) or any(
            not isinstance(obj, dict) or obj.get("kind") == "Secret" for obj in rendered
        ):
            raise ValueError("accepted source renderer returned invalid non-secret inventory")
        return tuple(rendered)
