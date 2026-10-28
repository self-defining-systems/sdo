"""A lane deploys the image refs the app source declares, through the fastloop delivery path."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import toy_images, worker_bridge

pytestmark = pytest.mark.requires_sregym

ROOT = Path(__file__).resolve().parents[5]


def test_bridge_delivery_then_in_node_tag_publishes_the_declared_ref(monkeypatch, tmp_path):
    """Real SREGym adapters through the bridge's registry delivery: each lane-built image is delivered under its
    private tag and then published on the nodes under the tag the source declares, never deployed under the private tag.
    """
    monkeypatch.syspath_prepend(str(ROOT / "third_party" / "sregym"))
    import importlib

    source = importlib.import_module("sregym.service.source_deploy")
    # The bridge's lazy imports run while import_module is stubbed below; resolve them first.
    importlib.import_module("benchmarks.sregym.fastloop.clean_start")
    events: list[tuple[str, str]] = []
    monkeypatch.setattr(
        toy_images, "load_images", lambda cluster, images, **kw: events.extend(("deliver", i) for i in images) or {}
    )
    monkeypatch.setattr(source, "_build_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(source, "_BUILT_IMAGES", set())
    monkeypatch.setattr(
        source,
        "tag_image_in_kind_nodes",
        lambda cluster, built, declared: events.append(("tag", f"{built} {declared}")),
    )
    monkeypatch.setenv("FASTLOOP_DELIVERY_RECORD", str(tmp_path / "delivery.jsonl"))
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "lane-1")
    monkeypatch.delenv("SREGYM_SOURCE_BUILD_CACHE", raising=False)

    class Worker:
        def handle(self, request):
            return {}

    def run() -> int:
        for app in ("Hotel Reservation", "Social Network"):
            with source.plan_for_app(SimpleNamespace(name=app), node_architectures={"amd64"}) as plan:
                assert plan.helm_extra_args == ()
        return 0

    worker = SimpleNamespace(FastloopWorker=Worker, main=run, probe_pod_manifest=lambda *args: {})
    infra = SimpleNamespace()
    modules = {"sregym.worker_infra": infra, "sregym.service.source_deploy": source}
    monkeypatch.setattr(worker_bridge.importlib, "import_module", lambda name: modules.get(name, worker))
    (tmp_path / "kubernetes").mkdir()
    rendered = tmp_path / "accepted.json"
    rendered.write_text("[]")
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(rendered))
    assert worker_bridge.main() == 0

    private = "sregym-src-"
    assert [kind for kind, _ in events] == ["deliver", "tag"] * 4
    for (_, delivered), (_, tagged) in zip(events[0::2], events[1::2], strict=True):
        built, declared = tagged.split()
        assert delivered == built
        assert private in built
        assert private not in declared
        assert built.rpartition(":")[0] == declared.rpartition(":")[0]
    assert {tagged.split()[1] for _, tagged in events[1::2]} == {
        "yinfangchen/hotelreservation:latest",
        "deathstarbench/social-network-microservices:latest",
        "yg397/openresty-thrift:xenial",
        "yg397/media-frontend:xenial",
    }


def test_astronomy_source_plan_preserves_seed_manifest(monkeypatch, tmp_path):
    import importlib

    monkeypatch.syspath_prepend(str(ROOT / "third_party/sregym"))
    source = importlib.import_module("sregym.service.source_deploy")
    manifest = tmp_path / "kubernetes/astronomy-shop.yaml"
    manifest.parent.mkdir()
    manifest.write_text("synthetic accepted source")
    monkeypatch.setenv("SREGYM_APP_SOURCE_DIR", str(tmp_path))
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(manifest))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "synthetic-lane")
    monkeypatch.setattr(source._AstronomyShopAdapter, "ensure_images_loaded", lambda *a, **kw: None)
    with source.plan_for_app(
        SimpleNamespace(name="OpenTelemetry Demo Astronomy Shop"), node_architectures={"amd64"}
    ) as plan:
        assert plan.manifest_path == manifest
        assert plan.helm_extra_args == ()
