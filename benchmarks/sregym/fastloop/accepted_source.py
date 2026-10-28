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

import yaml

from benchmarks.sregym.adapter import WaitKind, execute_operator, wait_for

if TYPE_CHECKING:
    from pathlib import Path

# Mirrors ClusterScopedKind in sdo/operator/rendered/scope.go (parity is tested).
CLUSTER_SCOPED_KINDS = frozenset(
    {
        "Namespace",
        "Node",
        "PersistentVolume",
        "ClusterRole",
        "ClusterRoleBinding",
        "CustomResourceDefinition",
        "APIService",
        "StorageClass",
        "VolumeAttachment",
        "CSIDriver",
        "CSINode",
        "RuntimeClass",
        "PriorityClass",
        "MutatingWebhookConfiguration",
        "ValidatingWebhookConfiguration",
        "ValidatingAdmissionPolicy",
        "ValidatingAdmissionPolicyBinding",
        "ClusterTrustBundle",
        "CertificateSigningRequest",
        "IngressClass",
        "PodSecurityPolicy",
        "IPAddress",
        "ServiceCIDR",
    }
)

DEFAULT_MANIFEST_PATHS = ("kubernetes",)
# Applications whose ``kubernetes/`` holds more than the deployed manifest set.
APPLICATION_MANIFEST_PATHS = {"opentelemetry-demo-astronomy-shop": ("kubernetes/astronomy-shop.yaml",)}
_MANIFEST_SUFFIXES = (".yaml", ".yml", ".json")
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")


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
        return cls(environment.workspace, environment.namespace, manifest_paths_for_environment(environment))

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
        """Rendered non-Secret objects, namespace-defaulted; duplicates are an error."""
        tree = self._git("ls-tree", "-r", "--name-only", self.revision).split("\n")
        files = sorted(
            {
                candidate
                for selection in self.manifest_paths
                for candidate in tree
                if (candidate == selection or candidate.startswith(selection + "/"))
                and (candidate.endswith(_MANIFEST_SUFFIXES) or PurePosixPath(candidate).name == "Kustomization")
            }
        )
        if any(
            PurePosixPath(path).name in {"kustomization.yaml", "kustomization.yml", "Kustomization"} for path in files
        ):
            # Keep generator evaluation at the production source owner. Only
            # non-secret objects cross this transport into benchmark records.
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
            if not isinstance(rendered, list) or any(
                not isinstance(obj, dict) or obj.get("kind") == "Secret" for obj in rendered
            ):
                raise ValueError("accepted source renderer returned invalid non-secret inventory")
            return tuple(rendered)
        objects: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for path in files:
            for document in yaml.safe_load_all(self._git("show", f"{self.revision}:{path}")):
                if not isinstance(document, dict):
                    continue
                value = json.loads(json.dumps(document, default=str))
                metadata = value.get("metadata") or {}
                kind, name = value.get("kind"), metadata.get("name")
                if not (value.get("apiVersion") and kind and name) or kind == "Secret":
                    continue
                if kind in CLUSTER_SCOPED_KINDS:
                    metadata.pop("namespace", None)
                else:
                    metadata.setdefault("namespace", self.namespace)
                value["metadata"] = metadata
                identity = (kind, metadata.get("namespace", ""), name)
                if identity in seen:
                    raise ValueError(f"duplicate accepted manifest for {kind}/{name} in {path}")
                seen.add(identity)
                objects.append(value)
        return tuple(objects)
