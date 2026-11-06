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
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, execute_operator, kustomize_source, raw_source, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT_MANIFEST_PATHS = ("kubernetes",)
# Applications whose ``kubernetes/`` holds more than the deployed manifest set.
APPLICATION_MANIFEST_PATHS = {"opentelemetry-demo-astronomy-shop": ("kubernetes/astronomy-shop.yaml",)}
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")

# SREGym applications whose ``kubernetes/`` is a kustomize root (S3-10d stopgap: raw rejects kustomize roots).
KUSTOMIZE_APPLICATIONS = {"hotel-reservation": "kubernetes"}


def deployment_source_for(application: str) -> dict[str, Any]:
    """DeploymentSource for a problem application name: kustomize for hotel, raw over its manifest paths otherwise."""
    name = application.lower().replace("_", "-").replace(" ", "-")
    if name in KUSTOMIZE_APPLICATIONS:
        return kustomize_source(KUSTOMIZE_APPLICATIONS[name])
    return raw_source(manifest_paths_for(application))


def manifest_paths_for(application: str) -> tuple[str, ...]:
    """Manifest selection for a problem application name (never a lane-qualified one)."""
    return APPLICATION_MANIFEST_PATHS.get(application, DEFAULT_MANIFEST_PATHS)


def deployment_source_for_environment(environment: Any) -> dict[str, Any]:
    """Source keyed like ``manifest_paths_for_environment`` (the problem application, never a lane-qualified name)."""
    manifest_paths_for_environment(environment)  # raises without a problem application
    return deployment_source_for(environment.problem_application)


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
class RenderedWorkloads:
    """What a deployment plan needs from a render: ``Kind/namespace/name`` IDs and the images the workloads run.

    Go owns which kinds are workloads (the pod-template table behind ``render-source``); nothing here lists kinds.
    """

    workload_ids: tuple[str, ...]
    images: tuple[str, ...]
    #: Release-independent forms Go proposes for a plan that must keep matching later renders: workload identities
    #: (a generated Job name suffix replaced by ``*``) and image repositories (no tag, no digest).
    identities: tuple[str, ...] = ()
    repositories: tuple[str, ...] = ()

    @classmethod
    def from_render(cls, value: Any) -> RenderedWorkloads:
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("workloads"), list)
            or not isinstance(value.get("images"), list)
        ):
            raise ValueError("render-source returned an invalid workload summary")
        return cls(
            tuple(str(workload["id"]) for workload in value["workloads"]),
            tuple(str(image) for image in value["images"]),
            tuple(str(identity) for identity in value.get("identities") or ()),
            tuple(str(repository) for repository in value.get("repositories") or ()),
        )


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
        return raw_source(self.manifest_paths)

    @classmethod
    def of(cls, environment: Any) -> AcceptedSource:
        return cls(
            environment.workspace,
            environment.namespace,
            manifest_paths_for_environment(environment),
            deployment_source_for_environment(environment),
        )

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
    def workloads(self) -> RenderedWorkloads:
        """The plan's view of the render: workload IDs and images, derived by Go from its pod-template table."""
        return RenderedWorkloads.from_render(self._render("workloads"))

    @cached_property
    def manifests(self) -> tuple[dict[str, Any], ...]:
        """Use the production renderer for plain manifests and generators alike."""
        rendered = self._render("objects")
        if not isinstance(rendered, list) or any(
            not isinstance(obj, dict) or obj.get("kind") == "Secret" for obj in rendered
        ):
            raise ValueError("accepted source renderer returned invalid non-secret inventory")
        return tuple(rendered)

    def _render(self, output: str) -> Any:
        request: dict[str, Any] = {
            "repository": str(self.workspace.resolve()),
            "revision": self.revision,
            "namespace": self.namespace,
            "deployment_source": self.deployment_source,
        }
        if output != "objects":
            request["output"] = output
        try:
            return json.loads(
                execute_operator(
                    "render-source",
                    [],
                    timeout=wait_for(WaitKind.COMMAND),
                    input_text=json.dumps(request),
                )
            )
        except RuntimeError as error:
            raise ValueError(f"accepted source render failed at {self.revision}: {error}") from error
