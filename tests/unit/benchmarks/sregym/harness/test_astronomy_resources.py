"""Keep the benchmark's rendered proxy budget at the proven LH no-fault baseline."""

import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml

from benchmarks.sregym.adapter.lane_resources import LaneResourcePolicy

ROOT = Path(__file__).resolve().parents[5]
APPLICATIONS = ROOT / "third_party/sregym/SREGym-applications"
OVERRIDES = ROOT / "third_party/sregym/sregym/service/metadata/astronomy-shop-resources.yaml"
CHART = APPLICATIONS / "astronomy-shop/charts/opentelemetry-demo"
DEPENDENCIES = ROOT / "tests/fixtures/benchmarks/astronomy-chart-dependencies"
MINIMUM_PROXY_MEMORY_MIB = 192
GIB = 1024**3
# OpenSearch must stay writable on a busy shared host that still passes lane admission.
WATERMARK_FLOOR_MARGIN_BYTES = 50 * GIB
# Independent floors from LH's accepted 9/9 no-fault baseline (decision 287).
PROVEN_LIMITS_MIB = {
    "accounting": 384,
    "ad": 576,
    "cart": 160,
    "checkout": 128,
    "currency": 128,
    "email": 192,
    "flagd": 128,
    "flagd-ui": 512,
    "fraud-detection": 576,
    "frontend": 256,
    "frontend-proxy": 192,
    "image-provider": 128,
    "kafka": 1152,
    "llm": 192,
    "load-generator": 1500,
    "locust-exporter": 128,
    "payment": 256,
    "postgresql": 256,
    "product-catalog": 128,
    "product-reviews": 192,
    "quote": 128,
    "recommendation": 500,
    "shipping": 128,
    "valkey-cart": 128,
    "opentelemetry-collector": 448,
    "opensearch": 2240,
    "grafana": 384,
    "grafana-sc-alerts": 256,
    "grafana-sc-dashboard": 192,
    "grafana-sc-datasources": 192,
}


def test_astronomy_proxy_override_preserves_proven_memory_floor() -> None:
    values = yaml.safe_load(OVERRIDES.read_text())
    proxy = values.get("components", {}).get("frontend-proxy", {})
    memory = proxy.get("resources", {}).get("limits", {}).get("memory", "0")
    assert values["opentelemetry-collector"]["resources"] == {
        "limits": {"memory": "448Mi"},
        "requests": {"memory": "200Mi"},
    }
    assert memory.endswith("Mi")
    assert int(memory[:-2]) >= MINIMUM_PROXY_MEMORY_MIB, (
        "Astronomy frontend-proxy needs the proven 192Mi memory floor; "
        "the old 65Mi chart default OOMs during the no-fault rehearsal"
    )


def assert_watermarks_below_admission_floor(search_config: str) -> None:
    settings = yaml.safe_load(search_config)
    watermarks = {
        key: settings[f"cluster.routing.allocation.disk.watermark.{key}"] for key in ("low", "high", "flood_stage")
    }
    assert all(value.endswith("gb") for value in watermarks.values()), "watermarks must be absolute free space"
    parsed = {key: int(value.removesuffix("gb")) * GIB for key, value in watermarks.items()}
    floor = LaneResourcePolicy().host_disk_floor_bytes
    assert parsed["flood_stage"] < parsed["high"] < parsed["low"], parsed
    # The read-only block releases only above `high`; even `low` must sit far below the admission floor.
    assert parsed["low"] + WATERMARK_FLOOR_MARGIN_BYTES <= floor, (watermarks, floor)


def test_astronomy_opensearch_watermarks_sit_far_below_admission_floor() -> None:
    values = yaml.safe_load(OVERRIDES.read_text())
    assert_watermarks_below_admission_floor(values["opensearch"]["config"]["opensearch.yml"])


@pytest.mark.skipif(
    shutil.which("helm") is None or not (CHART / "Chart.yaml").exists(),
    reason="rendered contract requires Helm and the populated harness chart",
)
def test_astronomy_rendered_proxy_preserves_proven_memory_floor(tmp_path: Path) -> None:
    # Always start cold: local helm dependency builds must not affect this test.
    chart = tmp_path / "opentelemetry-demo"
    shutil.copytree(CHART, chart, ignore=shutil.ignore_patterns("charts"))
    dependencies = yaml.safe_load((chart / "Chart.yaml").read_text())["dependencies"]
    for dependency in dependencies:
        name, version = dependency["name"], dependency["version"]
        archive = DEPENDENCIES / f"{name}-{version}.tgz"
        with tarfile.open(archive) as package:
            metadata = package.extractfile(f"{name}/Chart.yaml")
            assert metadata is not None
            packaged = yaml.safe_load(metadata.read())
        assert (packaged["name"], packaged["version"]) == (name, version)
        (chart / "charts").mkdir(exist_ok=True)
        shutil.copy2(archive, chart / "charts" / archive.name)
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "astronomy-shop",
            str(chart),
            "--namespace",
            "astronomy-shop",
            "--values",
            str(OVERRIDES),
            "--set",
            "prometheus.enabled=false",
            "--show-only",
            "templates/component.yaml",
            "--show-only",
            "charts/opentelemetry-collector/templates/daemonset.yaml",
            "--show-only",
            "charts/opensearch/templates/statefulset.yaml",
            "--show-only",
            "charts/grafana/templates/deployment.yaml",
            "--show-only",
            "charts/opensearch/templates/configmap.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    workloads = [
        item
        for item in yaml.safe_load_all(rendered.stdout)
        if item and item.get("kind") in {"Deployment", "DaemonSet", "StatefulSet"}
    ]
    containers = {
        container["name"]: container
        for item in workloads
        for container in item["spec"]["template"]["spec"]["containers"]
    }
    for name, minimum in PROVEN_LIMITS_MIB.items():
        assert name in containers, f"missing rendered baseline container: {name}"
        memory = containers[name]["resources"]["limits"]["memory"]
        assert memory.endswith("Mi"), (name, memory)
        assert int(memory[:-2]) >= minimum, (name, memory, minimum)
    assert containers["opentelemetry-collector"]["resources"]["requests"]["memory"] == "200Mi"
    configurations = [item for item in yaml.safe_load_all(rendered.stdout) if item and item.get("kind") == "ConfigMap"]
    search_config = next(
        item["data"]["opensearch.yml"] for item in configurations if "opensearch.yml" in item.get("data", {})
    )
    assert_watermarks_below_admission_floor(search_config)
