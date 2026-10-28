"""The accepted source as the one input to every eval restore decision.

SDO renders accepted source straight from Git (``rendered.Source``): the
selected manifest files at the accepted commit, each object defaulted to the
application namespace. Restore reads the same bytes the same way. It never
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

from benchmarks.sregym.adapter import WaitKind, execute_operator, wait_for

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT_MANIFEST_PATHS = ("kubernetes",)
# Applications whose ``kubernetes/`` holds more than the deployed manifest set.
APPLICATION_MANIFEST_PATHS = {"opentelemetry-demo-astronomy-shop": ("kubernetes/astronomy-shop.yaml",)}
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")


def manifest_paths_for(application: str) -> tuple[str, ...]:
    return APPLICATION_MANIFEST_PATHS.get(application, DEFAULT_MANIFEST_PATHS)


@dataclass(frozen=True)
class AcceptedSource:
    """Rendered manifests of the accepted commit checked out at ``workspace``."""

    workspace: Path
    namespace: str
    manifest_paths: tuple[str, ...] = DEFAULT_MANIFEST_PATHS

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("accepted source requires the application namespace")
        if not self.manifest_paths:
            raise ValueError("accepted source requires at least one manifest path")
        for path in self.manifest_paths:
            clean = PurePosixPath(path)
            if not path or str(clean) != path or clean.is_absolute() or ".." in clean.parts or path == ".":
                raise ValueError(f"invalid accepted manifest path {path!r}")

    @classmethod
    def of(cls, environment: Any) -> AcceptedSource:
        return cls(environment.workspace, environment.namespace, manifest_paths_for(environment.application))

    def _git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self.workspace), *args],
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
                            "manifest_paths": self.manifest_paths,
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
