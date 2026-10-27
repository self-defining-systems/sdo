"""The social-network frontends must not fetch their files from the network at pod start.

Stock DeathStarBench runs an ``alpine/git`` init container that clones the project from
GitHub into emptyDir volumes on every pod start of ``media-frontend`` and ``nginx-thrift``.
A rollout then sits in ``Init`` for 1.5-2 minutes (the clone competes for a 100m CPU limit)
and cannot start at all when GitHub is unreachable. The source overlay builds ConfigMaps
from the checkout's own files and mounts them where the clone used to put them.
"""

import posixpath
import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
OVERLAY = ROOT / "apps/social_network/source-overlays/helm-chart"
STOCK_SOURCE = ROOT / "apps/deathstarbench/socialNetwork"
FETCH_TOOLS = ("git", "curl", "wget")
KEY = re.compile(r"[-._a-zA-Z0-9]+")  # ConfigMap key alphabet

# chart -> volume -> source directory (relative to socialNetwork/) the old clone copied from
SOURCES = {
    "media-frontend": {"lua-scripts": "media-frontend/lua-scripts"},
    "nginx-thrift": {
        "lua-scripts": "nginx-web-server/lua-scripts",
        "lua-thrift": "docker/openresty-thrift/lua-thrift",
        "pages": "nginx-web-server/pages",
        "gen-lua": "gen-lua",
        "keys": "keys",
    },
}


def network_fetches(documents: list[dict[str, Any]]) -> list[str]:
    """Pod containers that run a network fetch tool, and init containers of any kind."""
    found = []
    for document in documents:
        if document["kind"] not in {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"}:
            continue
        spec = document["spec"]["template"]["spec"]
        name = document["metadata"]["name"]
        found += [f"{name}: init container {c['name']}" for c in spec.get("initContainers") or []]
        for container in spec.get("containers") or []:
            words = " ".join([*container.get("command", []), *container.get("args", [])]).replace("&&", " ").split()
            found += [f"{name}: {container['name']} runs {w}" for w in words if w in FETCH_TOOLS]
    return found


def render(chart: Path) -> list[dict[str, Any]]:
    out = subprocess.run(
        ["helm", "template", "social-network", str(chart / "helm-chart/socialnetwork"), "-n", "social-network"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def test_overlay_links_every_frontend_volume_to_its_source_directory() -> None:
    for chart, volumes in SOURCES.items():
        chart_dir = OVERLAY / "socialnetwork/charts" / chart
        values = yaml.safe_load((chart_dir / "values.yaml").read_text())
        assert "initContainer" not in values, f"{chart} still declares an init container"
        assert {v["name"] for v in values["volumes"]} == set(volumes)
        for volume, source in volumes.items():
            link = chart_dir / "files" / volume
            assert link.is_symlink(), f"{link} must link to the checkout's {source}, not copy it"
            location = posixpath.join("helm-chart/socialnetwork/charts", chart, "files")
            assert posixpath.normpath(posixpath.join(location, str(link.readlink()))) == source, link


@pytest.mark.skipif(
    shutil.which("helm") is None or not STOCK_SOURCE.is_dir() or not (STOCK_SOURCE / "gen-lua").is_dir(),
    reason="needs helm and the initialized apps/deathstarbench source",
)
def test_fresh_social_source_fetches_nothing_at_pod_start(fresh_social_chart: Callable[[bool], Path]) -> None:
    stock = render(fresh_social_chart(False))
    assert network_fetches(stock), "the stock chart is expected to clone at pod start; the overlay is what fixes it"

    documents = render(fresh_social_chart(True))
    assert network_fetches(documents) == []

    configs = {d["metadata"]["name"]: d["data"] for d in documents if d["kind"] == "ConfigMap"}
    deployments = {d["metadata"]["name"]: d for d in documents if d["kind"] == "Deployment"}
    for chart, volumes in SOURCES.items():
        pod = deployments[chart]["spec"]["template"]["spec"]
        mounted = {v["name"]: v for v in pod["volumes"]}
        for volume, source in volumes.items():
            reference = mounted[volume]["configMap"]
            assert any(m["name"] == volume for c in pod["containers"] for m in c["volumeMounts"])
            files = {p.relative_to(STOCK_SOURCE / source): p for p in (STOCK_SOURCE / source).rglob("*") if p.is_file()}
            assert files, source
            assert all(KEY.fullmatch(rel.as_posix().replace("/", "__")) for rel in files), (chart, volume)
            assert not any("__" in rel.as_posix() for rel in files), "file keys join path parts with __"
            projected = {i["path"]: configs[reference["name"]][i["key"]] for i in reference["items"]}
            assert projected == {rel.as_posix(): path.read_text() for rel, path in files.items()}, (chart, volume)
