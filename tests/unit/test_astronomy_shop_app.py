"""Source deployment contract for the pinned Astronomy Shop evaluation app."""

import json
from pathlib import Path

import yaml

APP = Path(__file__).resolve().parents[2] / "apps" / "astronomy_shop"


def test_astronomy_shop_deployment_matches_harness_contract() -> None:
    objects = list(yaml.safe_load_all((APP / "kubernetes" / "astronomy-shop.yaml").read_text()))
    indexed = {(obj["kind"], obj["metadata"]["name"]): obj for obj in objects}
    assert len(indexed) == len(objects)
    assert all(obj["metadata"].get("namespace") == "astronomy-shop" for obj in objects)
    deployments = {name for kind, name in indexed if kind == "Deployment"}
    assert deployments == {
        "accounting",
        "ad",
        "cart",
        "checkout",
        "currency",
        "email",
        "flagd",
        "fraud-detection",
        "frontend",
        "frontend-proxy",
        "grafana",
        "image-provider",
        "jaeger",
        "kafka",
        "llm",
        "load-generator",
        "payment",
        "postgresql",
        "product-catalog",
        "product-reviews",
        "quote",
        "recommendation",
        "shipping",
        "valkey-cart",
    }
    assert ("StatefulSet", "opensearch") in indexed
    assert ("DaemonSet", "otel-collector-agent") in indexed
    assert not any("prometheus" in name for _, name in indexed)
    proxy = indexed["Service", "frontend-proxy"]
    assert proxy["spec"]["ports"][0]["port"] == 8080
    generator = indexed["Deployment", "load-generator"]["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item.get("value") for item in generator["env"]}
    assert env["LOCUST_BROWSER_TRAFFIC_ENABLED"] == "false"
    assert env["LOCUST_HOST"] == "http://frontend-proxy:8080"
    flags = json.loads(indexed["ConfigMap", "flagd-config"]["data"]["demo.flagd.json"])["flags"]
    for name in ("adFailure", "cartFailure", "paymentFailure", "recommendationCacheFailure"):
        assert flags[name]["defaultVariant"] == "off"
    for name in deployments - {"grafana", "jaeger", "flagd", "postgresql", "valkey-cart"}:
        containers = indexed["Deployment", name]["spec"]["template"]["spec"]["containers"]
        assert containers[0]["image"] == f"ghcr.io/open-telemetry/demo:2.2.0-{name}"


def test_astronomy_shop_source_pins_are_explicit() -> None:
    pins = json.loads((APP / "pins.json").read_text())
    assert pins["demo_version"] == "2.2.0"
    assert pins["demo_commit"] == "b74a7bc7bbe66099c61951f42b24dab8b6f02d18"
    assert pins["chart_commit"] == "7d7b074714345a0c282b0be75af7a2c504b44c95"
    assert pins["sregym_commit"] == "4aba015ce5a0cd4ffe959a967c3afc6f83c074fb"
