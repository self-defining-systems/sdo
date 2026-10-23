"""Source deployment contract for the pinned Astronomy Shop evaluation app."""

import importlib.util
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
    assert pins["sregym_commit"] == "a1c8751818cb43dcb3e8e817a7060cb772f0afd8"


def test_astronomy_baseline_resources_and_metrics_match_running_components() -> None:
    objects = list(yaml.safe_load_all((APP / "kubernetes/astronomy-shop.yaml").read_text()))
    indexed = {(o["kind"], o["metadata"]["name"]): o for o in objects}
    expected = {
        ("accounting", "accounting"): "384Mi",
        ("shipping", "shipping"): "128Mi",
        ("flagd", "flagd-ui"): "512Mi",
    }
    for (workload, container), limit in expected.items():
        containers = indexed["Deployment", workload]["spec"]["template"]["spec"]["containers"]
        actual = next(c for c in containers if c["name"] == container)
        assert actual["resources"]["limits"]["memory"] == limit
        assert "cpu" not in actual["resources"]["limits"]
    collector = yaml.safe_load(indexed["ConfigMap", "otel-collector-agent"]["data"]["relay"])
    readers = collector["service"]["telemetry"]["metrics"]["readers"]
    assert any(
        r.get("pull", {}).get("exporter", {}).get("prometheus") == {"host": "0.0.0.0", "port": 8888} for r in readers
    )
    assert any("periodic" in r for r in readers)
    for name in ["opensearch", "opensearch-headless"]:
        assert {p["port"] for p in indexed["Service", name]["spec"]["ports"]} == {9200, 9300}
    container = indexed["StatefulSet", "opensearch"]["spec"]["template"]["spec"]["containers"][0]
    assert {p["containerPort"] for p in container["ports"]} == {9200, 9300}


def test_astronomy_renderer_preserves_baseline_policy(tmp_path, monkeypatch) -> None:
    """Regenerating the pinned chart must retain the independently verified fixes."""
    spec = importlib.util.spec_from_file_location("astronomy_render", APP / "render.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    pins = json.loads((APP / "pins.json").read_text())
    (tmp_path / "pins.json").write_text(json.dumps(pins))
    monkeypatch.setattr(module, "__file__", str(tmp_path / "render.py"))
    # The checked-in pre-fix chart output is the input shape. Reset only the
    # policy fields, so a no-op renderer cannot satisfy this contract.
    objects = list(yaml.safe_load_all((APP / "kubernetes/astronomy-shop.yaml").read_text()))
    for o in objects:
        if o["kind"] == "Deployment" and o["metadata"]["name"] in {"accounting", "shipping", "flagd"}:
            for c in o["spec"]["template"]["spec"]["containers"]:
                old = {"accounting": "120Mi", "shipping": "20Mi", "flagd-ui": "250Mi"}
                if c["name"] in old:
                    c["resources"]["limits"]["memory"] = old[c["name"]]
        if o["kind"] == "ConfigMap" and o["metadata"]["name"] == "otel-collector-agent":
            config = yaml.safe_load(o["data"]["relay"])
            metrics = config["service"]["telemetry"]["metrics"]
            metrics["readers"] = [r for r in metrics["readers"] if "pull" not in r]
            o["data"]["relay"] = yaml.safe_dump(config, sort_keys=False)
        if (
            o["kind"] == "Service"
            and o["metadata"]["name"] in {"opensearch", "opensearch-headless"}
            and not any(p["port"] == 9600 for p in o["spec"]["ports"])
        ):
            o["spec"]["ports"].append({"name": "metrics", "port": 9600})
        if o["kind"] == "StatefulSet" and o["metadata"]["name"] == "opensearch":
            o["spec"]["template"]["spec"]["containers"][0]["ports"].append({"name": "metrics", "containerPort": 9600})
    rendered = yaml.safe_dump_all(objects, sort_keys=False)

    def command(argv, **kwargs):
        if argv[0] == "git":
            return pins["demo_commit"] if argv[2].endswith("upstream") else pins["chart_commit"]
        return pins["helm_version"] if argv[1] == "version" else rendered

    monkeypatch.setattr(module.subprocess, "check_output", command)
    module.main()
    output = list(yaml.safe_load_all((tmp_path / "kubernetes/astronomy-shop.yaml").read_text()))
    deployment = next(o for o in output if o["kind"] == "Deployment" and o["metadata"]["name"] == "shipping")
    assert deployment["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"] == "128Mi"
    collector = next(o for o in output if o["kind"] == "ConfigMap" and o["metadata"]["name"] == "otel-collector-agent")
    readers = yaml.safe_load(collector["data"]["relay"])["service"]["telemetry"]["metrics"]["readers"]
    assert any("pull" in r for r in readers)
    for o in output:
        if o["kind"] == "Service" and o["metadata"]["name"] in {"opensearch", "opensearch-headless"}:
            assert {p["port"] for p in o["spec"]["ports"]} == {9200, 9300}


def test_astronomy_memory_rule_covers_every_container_and_preserves_requests() -> None:
    """All application/telemetry/init containers receive measured headroom."""
    import copy
    import math

    spec = importlib.util.spec_from_file_location("astronomy_memory_render", APP / "render.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    policy = json.loads((APP / "memory-baseline.json").read_text())
    measurements = {(r["kind"], r["workload"], r["container"], r["init"]): r for r in policy["containers"]}
    objects = list(yaml.safe_load_all((APP / "kubernetes/astronomy-shop.yaml").read_text()))
    seen = set()
    for obj in objects:
        if obj["kind"] not in {"Deployment", "StatefulSet", "DaemonSet"}:
            continue
        stock = copy.deepcopy(obj)
        for group in ("containers", "initContainers"):
            for c in stock["spec"]["template"]["spec"].get(group, []):
                key = (obj["kind"], obj["metadata"]["name"], c["name"], group == "initContainers")
                row = measurements[key]
                seen.add(key)
                resources = c.setdefault("resources", {})
                resources.setdefault("requests", {}).pop("memory", None)
                if row["stock_limit_mib"]:
                    resources.setdefault("limits", {})["memory"] = f"{row['stock_limit_mib']}Mi"
                else:
                    resources.get("limits", {}).pop("memory", None)
        before = copy.deepcopy(stock)
        module.prepare_baseline(stock)
        for group in ("containers", "initContainers"):
            actual = stock["spec"]["template"]["spec"].get(group, [])
            original = before["spec"]["template"]["spec"].get(group, [])
            checked_in = obj["spec"]["template"]["spec"].get(group, [])
            for c, prior, checked in zip(actual, original, checked_in, strict=True):
                row = measurements[obj["kind"], obj["metadata"]["name"], c["name"], group == "initContainers"]
                expected = max(
                    128, row["stock_limit_mib"], math.ceil(2 * row["observed_peak_bytes"] / (64 * 2**20)) * 64
                )
                assert c["resources"]["limits"]["memory"] == f"{expected}Mi", (obj["metadata"]["name"], c["name"])
                assert checked["resources"]["limits"]["memory"] == f"{expected}Mi"
                assert c["resources"]["requests"]["memory"] == row["effective_request_memory"]
                assert c["resources"]["requests"].get("cpu") == prior["resources"].get("requests", {}).get("cpu")
                assert c["resources"]["limits"].get("cpu") == prior["resources"].get("limits", {}).get("cpu")
    assert seen == set(measurements)
    assert len(seen) == 37
