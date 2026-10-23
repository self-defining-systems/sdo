"""The fresh source overlay must serve the tracing ports it declares."""

from pathlib import Path

import yaml


def test_social_seed_jaeger_matches_stock_version_and_enables_zipkin() -> None:
    root = Path(__file__).resolve().parents[3]
    values = yaml.safe_load(
        (root / "apps/social_network/source-overlays/helm-chart/socialnetwork/charts/jaeger/values.yaml").read_text()
    )
    assert values["container"]["image"] == "jaegertracing/all-in-one"
    assert values["container"]["imageVersion"] == "1.57"
    assert values["container"]["env"] == [{"name": "COLLECTOR_ZIPKIN_HOST_PORT", "value": ":9411"}]
    assert {p["port"] for p in values["ports"]} >= {5778, 9411, 16686, 14268}
