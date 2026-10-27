"""Every social-network MongoDB must bound its own memory on a fresh seed.

mongod 4.4 ignores cgroup v2 limits and sizes the WiredTiger cache from host
RAM, so an unbounded instance grows until its container is killed. The fresh
source overlay must give every ``*-mongodb`` Deployment a ``cacheSizeGB`` in its
mounted mongod.conf and a memory limit that holds that cache plus process
overhead. The benchmark's own "MongoDB 128Mi OOM" fault is injected later and
is not part of the seed.
"""

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
STOCK_CHART = ROOT / "apps/deathstarbench/socialNetwork/helm-chart"
OVERHEAD_MIB = 512  # connections, index builds and allocator slack beyond the cache
UNITS = {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024}


def mib(quantity: str) -> float:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(Ki|Mi|Gi)", str(quantity))
    assert match, f"unsupported memory quantity {quantity!r}"
    return float(match.group(1)) * UNITS[match.group(2)]


def violations(documents: list[dict[str, Any]]) -> list[str]:
    configs = {d["metadata"]["name"]: d.get("data", {}) for d in documents if d["kind"] == "ConfigMap"}
    mongos = [d for d in documents if d["kind"] == "Deployment" and d["metadata"]["name"].endswith("-mongodb")]
    found = [] if mongos else ["no *-mongodb Deployment rendered"]
    for deployment in mongos:
        name = deployment["metadata"]["name"]
        spec = deployment["spec"]["template"]["spec"]
        container = next(c for c in spec["containers"] if c["name"] == name)
        limit = container.get("resources", {}).get("limits", {}).get("memory")
        mounted = [v["configMap"]["name"] for v in spec.get("volumes", []) if "configMap" in v]
        conf = next((configs[m]["mongod.conf"] for m in mounted if "mongod.conf" in configs.get(m, {})), None)
        cache = (((yaml.safe_load(conf or "") or {}).get("storage") or {}).get("wiredTiger") or {}).get("engineConfig")
        cache_gb = (cache or {}).get("cacheSizeGB")
        if cache_gb is None:
            found.append(f"{name}: mongod.conf has no wiredTiger cacheSizeGB")
        elif limit is None or float(cache_gb) * 1024 + OVERHEAD_MIB > mib(limit):
            found.append(f"{name}: memory limit {limit} does not hold a {cache_gb} GB cache plus {OVERHEAD_MIB}Mi")
    return found


def render(chart: Path) -> list[dict[str, Any]]:
    out = subprocess.run(
        ["helm", "template", "social-network", str(chart / "helm-chart/socialnetwork"), "-n", "social-network"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [d for d in yaml.safe_load_all(out) if d]


@pytest.mark.skipif(
    shutil.which("helm") is None or not (STOCK_CHART / "socialnetwork").is_dir(),
    reason="needs helm and the initialized apps/deathstarbench source",
)
def test_fresh_social_source_bounds_every_mongodb(fresh_social_chart: Callable[[bool], Path]) -> None:
    assert violations(render(fresh_social_chart(False))), (
        "the stock chart is expected to be unbounded; the overlay is what fixes it"
    )

    assert violations(render(fresh_social_chart(True))) == []


def test_overlay_names_every_social_mongodb_chart() -> None:
    charts = {p.parent.name for p in OVERLAY.glob("socialnetwork/charts/*-mongodb/values.yaml")}
    assert charts == {
        "media-mongodb",
        "post-storage-mongodb",
        "social-graph-mongodb",
        "url-shorten-mongodb",
        "user-mongodb",
        "user-timeline-mongodb",
    }
    for values_path in OVERLAY.glob("socialnetwork/charts/*-mongodb/values.yaml"):
        container = yaml.safe_load(values_path.read_text())["container"]
        assert mib(container["resources"]["limits"]["memory"]) >= 1024, values_path
        assert mib(container["resources"]["requests"]["memory"]) >= 512, values_path
