"""Build the two inputs of an attached fuzzops run from a fuzzops app pack.

* The SDO application repository: the pack's manifests as plain source, plus the ``DeploymentPlan`` that makes
  ``sdo-operator host`` deploy exactly those manifests (no deployer agent). SDO then owns the app's desired state.
* The attached pack: the same journeys, entry and app-specific faults, but ``up`` only waits for the app and
  ``down`` refuses, so fuzzops never deploys over or removes what SDO operates (``fuzzops run --skip-reset``).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_CONFIG_TYPE = "type.googleapis.com/sdodev.contracts.v1alpha1."
WORKLOAD_KINDS = frozenset({"Deployment", "StatefulSet", "DaemonSet", "Job"})
MANIFEST_DIR = "k8s"

_UP = """#!/usr/bin/env bash
# Attached pack: the app belongs to the operator. Wait until it serves; never apply anything.
set -euo pipefail
CTX="${KUBE_CONTEXT:-kind-${KIND_CLUSTER:-fuzzops}}"
kubectl --context "$CTX" -n "$NAMESPACE" wait --for=condition=Available deploy --all --timeout="${WAIT_TIMEOUT:-300s}"
"""

_DOWN = """#!/usr/bin/env bash
echo "attached pack: the app in $NAMESPACE belongs to its operator; fuzzops must not remove it" >&2
echo "(use fuzzops run --skip-reset)" >&2
exit 1
"""


class PackError(ValueError):
    """A fuzzops pack cannot be turned into an SDO application or an attached pack."""


@dataclass(frozen=True)
class SourcePack:
    """A fuzzops app pack directory (``app.yaml``, journeys, ``manifests/``, optional ``faults/``)."""

    root: Path

    def __post_init__(self) -> None:
        if not isinstance(self.root, Path):
            raise TypeError("root must be a Path")
        if not (self.root / "app.yaml").is_file():
            raise PackError(f"{self.root} has no app.yaml")
        if not (self.root / "manifests").is_dir():
            raise PackError(f"{self.root} has no manifests/ directory (only plain-manifest packs are supported)")

    def app(self) -> dict[str, Any]:
        data = yaml.safe_load((self.root / "app.yaml").read_text())
        if not isinstance(data, dict) or not isinstance(data.get("entry"), dict):
            raise PackError(f"{self.root}/app.yaml has no entry")
        return data

    def manifests(self) -> list[Path]:
        files = sorted(p for p in (self.root / "manifests").iterdir() if p.suffix in {".yaml", ".yml"})
        if not files:
            raise PackError(f"{self.root}/manifests holds no YAML files")
        return files


def workloads(manifests: list[Path]) -> list[tuple[str, str]]:
    """(kind, name) of every workload object in the manifests, in file order."""
    found: list[tuple[str, str]] = []
    for path in manifests:
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict) or doc.get("kind") not in WORKLOAD_KINDS:
                continue
            name = (doc.get("metadata") or {}).get("name")
            if not isinstance(name, str) or not name:
                raise PackError(f"{path}: a {doc['kind']} has no name")
            if (doc.get("metadata") or {}).get("namespace"):
                raise PackError(f"{path}: {doc['kind']}/{name} sets a namespace; pack manifests must not")
            found.append((doc["kind"], name))
    if not found:
        raise PackError("the manifests declare no workload")
    return found


def images(manifests: list[Path]) -> list[str]:
    """Every container image reference the manifests name, sorted and unique."""
    found: set[str] = set()
    for path in manifests:
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict) or doc.get("kind") not in WORKLOAD_KINDS:
                continue
            spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
            for container in [*(spec.get("initContainers") or []), *(spec.get("containers") or [])]:
                if isinstance(container, dict) and isinstance(container.get("image"), str):
                    found.add(container["image"])
    return sorted(found)


def deployment_plan(manifests: list[Path], namespace: str) -> dict[str, Any]:
    """ProtoJSON ``DeploymentPlan``: the raw renderer over ``k8s/`` and every workload it must roll out."""
    if not namespace:
        raise ValueError("namespace is required")
    return {
        "source": {
            "renderer_id": "raw",
            "config": {"@type": _CONFIG_TYPE + "RawRenderConfig", "paths": [MANIFEST_DIR]},
        },
        "images": [],
        "expected_workload_ids": [f"{kind}/{namespace}/{name}" for kind, name in workloads(manifests)],
    }


def write_app_repository(pack: SourcePack, out: Path, *, description: str) -> Path:
    """A fresh Git repository at ``out`` with the pack's manifests under ``k8s/`` and one commit."""
    if out.exists() and any(out.iterdir()):
        raise PackError(f"{out} is not empty")
    (out / MANIFEST_DIR).mkdir(parents=True)
    for path in pack.manifests():
        shutil.copy2(path, out / MANIFEST_DIR / path.name)
    (out / "README.md").write_text(description.rstrip() + "\n")
    git = ["git", "-C", str(out), "-c", "user.name=fuzzops adapter", "-c", "user.email=fuzzops-adapter@invalid"]
    subprocess.run([*git[:3], "init", "-q", "-b", "main"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "application source: plain manifests"], check=True)
    return out


def write_attached_pack(pack: SourcePack, out: Path) -> Path:
    """The pack fuzzops runs against an app its operator deployed: same entry, journeys and faults."""
    app = pack.app()
    journeys = app.get("journeys", "journeys.yaml")
    if not (pack.root / journeys).is_file():
        raise PackError(f"{pack.root} has no {journeys}")
    out.mkdir(parents=True, exist_ok=True)
    attached = {"name": app["name"], "up": "./up.sh", "down": "./down.sh", "entry": app["entry"], "journeys": journeys}
    (out / "app.yaml").write_text(json.dumps(attached, indent=2) + "\n")
    shutil.copy2(pack.root / journeys, out / journeys)
    if (pack.root / "faults").is_dir():
        shutil.copytree(pack.root / "faults", out / "faults", dirs_exist_ok=True)
    for name, text in (("up.sh", _UP), ("down.sh", _DOWN)):
        (out / name).write_text(text)
        (out / name).chmod(0o755)
    return out
