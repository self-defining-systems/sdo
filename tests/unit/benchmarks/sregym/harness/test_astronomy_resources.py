"""Keep the benchmark's rendered proxy budget at the upstream Helm default."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[5]
APPLICATIONS = ROOT / "third_party/sregym/SREGym-applications"
OVERRIDES = ROOT / "third_party/sregym/sregym/service/metadata/astronomy-shop-resources.yaml"
CHART = APPLICATIONS / "astronomy-shop/charts/opentelemetry-demo"
MINIMUM_PROXY_MEMORY_MIB = 90


def test_astronomy_proxy_override_preserves_upstream_memory_floor() -> None:
    values = yaml.safe_load(OVERRIDES.read_text())
    proxy = values.get("components", {}).get("frontend-proxy", {})
    memory = proxy.get("resources", {}).get("limits", {}).get("memory", "0")
    assert memory.endswith("Mi")
    assert int(memory[:-2]) >= MINIMUM_PROXY_MEMORY_MIB, (
        "Astronomy frontend-proxy needs the upstream 90Mi memory floor; "
        "the old 65Mi chart default OOMs during the no-fault rehearsal"
    )


@pytest.mark.skipif(
    shutil.which("helm") is None or not (CHART / "Chart.yaml").exists(),
    reason="rendered contract requires Helm and the populated harness chart",
)
def test_astronomy_rendered_proxy_preserves_upstream_memory_floor() -> None:
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "astronomy-shop",
            str(CHART),
            "--namespace",
            "astronomy-shop",
            "--values",
            str(OVERRIDES),
            "--set",
            "prometheus.enabled=false",
            "--show-only",
            "templates/component.yaml",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    deployments = [
        item
        for item in yaml.safe_load_all(rendered.stdout)
        if item and item.get("kind") == "Deployment" and item["metadata"]["name"] == "frontend-proxy"
    ]
    assert len(deployments) == 1
    containers = deployments[0]["spec"]["template"]["spec"]["containers"]
    proxy = next(item for item in containers if item["name"] == "frontend-proxy")
    memory = proxy["resources"]["limits"]["memory"]
    assert memory.endswith("Mi")
    assert int(memory[:-2]) >= MINIMUM_PROXY_MEMORY_MIB
