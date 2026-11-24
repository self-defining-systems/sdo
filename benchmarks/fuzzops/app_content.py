"""Deployment identity for the GitLab Helm pack, computed without a cluster."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.request import urlopen

import yaml

if TYPE_CHECKING:
    from collections.abc import Mapping


def _digest(*parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _chart_archive(pack: Path, scratch: Path, env: Mapping[str, str]) -> Path:
    pin = json.loads((pack / "pins.json").read_text())["gitlab_chart"]
    name = f"{pin['name']}-{pin['version']}.tgz"
    cached = Path(env.get("FUZZOPS_CACHE") or Path.home() / ".cache" / "fuzzops") / "gitlab" / name
    if cached.is_file() and _file_digest(cached) == pin["sha256"]:
        return cached
    archive = scratch / name
    with urlopen(pin["url"], timeout=15) as source, archive.open("wb") as file:
        shutil.copyfileobj(source, file)
    if _file_digest(archive) != pin["sha256"]:
        raise ValueError(f"chart {name} does not match its SHA-256 pin")
    return archive


def _render(pack: Path, chart: Path, namespace: str, env: Mapping[str, str], *extra: str) -> str:
    # The release, values and namespace are the arguments used by the pack's up script.
    path = os.pathsep.join((str(Path.home() / ".local/go/bin"), str(Path.home() / ".local/bin"), env.get("PATH", "")))
    return subprocess.run(
        ["helm", "template", "gitlab", str(chart), "-n", namespace, "-f", "values-fuzzops.yaml", *extra],
        cwd=pack,
        env={**env, "PATH": path},
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    ).stdout


def _image_refs(manifests: str) -> list[str]:
    def images(node: Any) -> set[str]:
        if isinstance(node, dict):
            refs = {node["image"]} if isinstance(node.get("image"), str) else set()
            return refs.union(*(images(value) for value in node.values()))
        if isinstance(node, list):
            return set().union(*(images(value) for value in node))
        return set()

    return sorted(images(list(yaml.safe_load_all(manifests))))


def identify(env: Mapping[str, str]) -> dict[str, str] | None:
    """Return deployment digests and the separate digest of assets an attached pack keeps across swaps."""
    pack = Path(env["BENCH_PACK"])
    app = yaml.safe_load((pack / "app.yaml").read_text())
    if app["name"] != "gitlab":
        return None
    with tempfile.TemporaryDirectory(prefix="fuzzops-app-content-") as work:
        scratch = Path(work)
        chart = scratch / "chart"
        shutil.copytree(pack / "chart", chart)
        with tarfile.open(_chart_archive(pack, scratch, env)) as archive:
            archive.extractall(chart / "charts", filter="data")
        rendered = _render(pack, chart, env["BENCH_NAMESPACE"], env)
        refs = _image_refs(rendered)
        if values := env.get("FUZZOPS_VALUES"):
            # The image preload script can use an extra values file even though app up uses only its pinned values.
            refs = sorted(set(refs) | set(_image_refs(_render(pack, chart, env["BENCH_NAMESPACE"], env, "-f", values))))

    # The pack's snapshot key binds its seed size, up and snapshot scripts, and its render. The render and images
    # are compared separately so the manager can report which part of the deployment changed.
    seed_files = {name: _file_digest(pack / name) for name in (app["up"], "snapshot.sh")}
    if seed := app.get("seed"):
        seed_files[seed] = _file_digest(pack / seed)
    journeys = app.get("journeys", "journeys.yaml")
    attachments = {journeys: _file_digest(pack / journeys)}
    attachments.update(
        {str(path.relative_to(pack)): _file_digest(path) for path in (pack / "faults").rglob("*") if path.is_file()}
    )
    image_loader = app.get("images")
    return {
        "render": hashlib.sha256(rendered.encode()).hexdigest(),
        "images": _digest(refs, _file_digest(pack / image_loader) if image_loader else None),
        "data": _digest("gitlab-snapshot-v2", env.get("PROJECTS") or "3", app.get("stores", []), seed_files),
        "attachments": _digest(app["entry"], attachments),
    }
