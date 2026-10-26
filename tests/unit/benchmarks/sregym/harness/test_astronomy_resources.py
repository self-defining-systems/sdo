"""Keep the benchmark's rendered proxy budget at the proven LH no-fault baseline."""

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[5]
APPLICATIONS = ROOT / "third_party/sregym/SREGym-applications"
OVERRIDES = ROOT / "third_party/sregym/sregym/service/metadata/astronomy-shop-resources.yaml"
CHART = APPLICATIONS / "astronomy-shop/charts/opentelemetry-demo"
MINIMUM_PROXY_MEMORY_MIB = 192
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


@pytest.mark.skipif(
    shutil.which("helm") is None or not (CHART / "Chart.yaml").exists(),
    reason="rendered contract requires Helm and the populated harness chart",
)
def test_astronomy_rendered_proxy_preserves_proven_memory_floor() -> None:
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
            "--show-only",
            "charts/opentelemetry-collector/templates/daemonset.yaml",
            "--show-only",
            "charts/opensearch/templates/statefulset.yaml",
            "--show-only",
            "charts/grafana/templates/deployment.yaml",
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
