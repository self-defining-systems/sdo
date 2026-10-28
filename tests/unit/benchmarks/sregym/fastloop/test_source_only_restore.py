"""Captured application content cannot become restore authority."""

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, accepted_restore_baselines
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source


@pytest.mark.parametrize("sealed", [False, True])
def test_restore_inventory_and_bytes_are_only_the_accepted_render(tmp_path, sealed):
    from benchmarks.sregym.fastloop.baseline_recovery import _baseline_digest

    declared = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "settings", "namespace": "app"},
        "data": {"selected": "accepted"},
    }
    added = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "new-service", "namespace": "app"},
        "spec": {"ports": [{"port": 80}]},
    }
    source = commit_accepted_source(tmp_path / "repo", [declared, added])
    stale = deepcopy(declared)
    stale["data"] = {"selected": "old", "harness-only": "must disappear"}
    stale["metadata"]["annotations"] = {"harness-only": "must disappear"}
    undeclared = deepcopy(stale)
    undeclared["metadata"]["name"] = "captured-only"
    objects = [stale, undeclared]
    capture = ApplicationBaseline("app", objects, _baseline_digest(objects) if sealed else None)
    restored = accepted_restore_baselines(source, (capture,), seed_commit=source.revision)
    assert {b.namespace: b.objects for b in restored} == {"app": list(source.manifests)}


@pytest.mark.requires_sregym
@pytest.mark.parametrize(
    ("module", "class_name", "app_name", "namespace"),
    [
        ("hotel_reservation", "HotelReservation", "Hotel Reservation", "hotel-reservation"),
        ("social_network", "SocialNetwork", "Social Network", "social-network"),
        ("astronomy_shop", "AstronomyShop", "OpenTelemetry Demo Astronomy Shop", "astronomy-shop"),
    ],
)
def test_native_deploy_consumes_the_same_accepted_render(
    tmp_path, monkeypatch, module, class_name, app_name, namespace
):
    import importlib
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.source_deployment import configure_source_deployment

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[5] / "third_party/sregym"))
    builds = importlib.import_module("sregym.service.source_deploy")
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(tmp_path / "repo"))
    monkeypatch.setenv("SREGYM_DEPLOY_FROM_SOURCE", "1")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "offline")
    monkeypatch.setattr(builds._SUPPORTED_ADAPTERS[app_name], "ensure_images_loaded", lambda **kwargs: None)
    (tmp_path / "repo").mkdir()
    (tmp_path / "repo/build-input").write_text("accepted build input")
    source = commit_accepted_source(
        tmp_path / "repo",
        [
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "settings", "namespace": namespace},
                "data": {"value": "accepted"},
            }
        ],
        namespace=namespace,
        path="kubernetes/astronomy-shop.yaml" if module == "astronomy_shop" else "kubernetes/app.yaml",
    )
    # Dirty working bytes must not affect either deployment or restoration.
    next((tmp_path / "repo/kubernetes").glob("*.yaml")).write_text("not accepted")
    (tmp_path / "repo/build-input").write_text("unaccepted build input")
    applied = []

    app = SimpleNamespace(name=app_name, namespace=namespace)

    def deploy():
        import os

        assert (Path(os.environ["SREGYM_APP_SOURCE_DIR"]) / "build-input").read_text() == "accepted build input"
        with builds.plan_for_app(app) as plan:
            applied.extend(json.loads(plan.manifest_path.read_text())["items"])

    app.deploy = deploy
    configure_source_deployment(app)
    app.deploy()
    assert applied == list(source.manifests)
    assert accepted_restore_baselines(source, (ApplicationBaseline(namespace, []),))[0].objects == applied
    assert "SREGYM_ACCEPTED_RENDER" not in __import__("os").environ
