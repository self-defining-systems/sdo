"""Keep the benchmark's rendered proxy budget at the proven LH no-fault baseline."""

from pathlib import Path

import yaml

from benchmarks.sregym.adapter.lane_resources import LaneResourcePolicy

ROOT = Path(__file__).resolve().parents[5]
SOURCE = ROOT / "apps/astronomy_shop/kubernetes/astronomy-shop.yaml"
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


def documents():
    return [value for value in yaml.safe_load_all(SOURCE.read_text()) if value]


def containers():
    return {
        container["name"]: container
        for value in documents()
        if value["kind"] in {"Deployment", "DaemonSet", "StatefulSet"}
        for container in value["spec"]["template"]["spec"]["containers"]
    }


def test_accepted_proxy_source_preserves_proven_memory_floor() -> None:
    selected = containers()
    assert selected["opentelemetry-collector"]["resources"] == {
        "limits": {"memory": "448Mi"},
        "requests": {"memory": "200Mi"},
    }
    memory = selected["frontend-proxy"]["resources"]["limits"]["memory"]
    assert memory.endswith("Mi")
    assert int(memory[:-2]) >= MINIMUM_PROXY_MEMORY_MIB


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


def test_accepted_opensearch_watermarks_sit_below_admission_floor() -> None:
    config = next(
        item["data"]["opensearch.yml"]
        for item in documents()
        if item["kind"] == "ConfigMap" and "opensearch.yml" in item.get("data", {})
    )
    assert_watermarks_below_admission_floor(config)


def test_accepted_render_preserves_every_proven_container_memory_floor() -> None:
    selected = containers()
    for name, minimum in PROVEN_LIMITS_MIB.items():
        assert name in selected, f"missing accepted baseline container: {name}"
        memory = selected[name]["resources"]["limits"]["memory"]
        assert memory.endswith("Mi"), (name, memory)
        assert int(memory[:-2]) >= minimum, (name, memory, minimum)
