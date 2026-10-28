"""A build is published once; each lane consumes the verified immutable manifest."""

from __future__ import annotations

import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from subprocess import CompletedProcess
from threading import Thread

import pytest

from benchmarks.sregym.fastloop import image_delivery

CONFIG = "sha256:" + "a" * 64
OTHER_CONFIG = "sha256:" + "b" * 64


@pytest.fixture
def registry(monkeypatch):
    """Real manifest HTTP reads; only the Docker/kind process boundary is synthetic."""
    manifests = {}
    commands = []
    state = {"config": CONFIG, "push_config": None, "node_config": None, "corrupt": False}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/v2/":
                self.send_response(200)
                self.end_headers()
                return
            raw = manifests.get(self.path)
            if raw is None:
                self.send_error(404)
                return
            self.send_response(200)
            digest = "sha256:" + hashlib.sha256(raw).hexdigest()
            self.send_header("Docker-Content-Digest", CONFIG if state["corrupt"] else digest)
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host = f"127.0.0.1:{server.server_port}"
    pushed = {}

    def run(argv, **kwargs):
        commands.append((argv, kwargs))
        output = ""
        if argv[:3] == ["docker", "container", "inspect"]:
            output = json.dumps(
                [
                    {
                        "Config": {"Labels": {"sdo.fastloop.registry": "host-v1"}},
                        "State": {"Running": True},
                        "NetworkSettings": {
                            "Ports": {"5000/tcp": [{"HostPort": str(server.server_port)}]},
                            "Networks": {"kind": {}},
                        },
                    }
                ]
            )
        elif argv[:3] == ["docker", "image", "inspect"]:
            output = state["config"]
        elif argv[:3] == ["kind", "get", "nodes"]:
            output = argv[-1] + "-control-plane\n" + argv[-1] + "-worker\n"
        elif argv[:2] == ["docker", "push"]:
            local = argv[-1]
            repository, tag = local.removeprefix(host + "/").rsplit(":", 1)
            raw = json.dumps(
                {"schemaVersion": 2, "config": {"digest": state["push_config"] or state["config"]}, "layers": []}
            ).encode()
            digest = "sha256:" + hashlib.sha256(raw).hexdigest()
            manifests[f"/v2/{repository}/manifests/{tag}"] = raw
            manifests[f"/v2/{repository}/manifests/{digest}"] = raw
            pushed[digest] = local
            output = f"{tag}: digest: {digest} size: {len(raw)}"
        elif any(arg.startswith("target.digest==") for arg in argv):
            digest = argv[-1].removeprefix("target.digest==")
            local = pushed[digest]
            output = local.rsplit(":", 1)[0] + "@" + digest + "\n"
        elif "inspecti" in argv:
            output = json.dumps({"status": {"id": state["node_config"] or state["config"]}})
        return CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    try:
        yield commands, manifests, state, host
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_two_lanes_publish_once_and_pull_digest_on_every_node(registry):
    commands, _, _, host = registry
    metrics = {}
    first = image_delivery.registry_delivery("img1-a", ["controller:frozen"], metrics=metrics)
    second_metrics = {}
    second = image_delivery.registry_delivery("img1-b", ["controller:frozen"], metrics=second_metrics)
    assert len(first) == len(second) == 2
    assert len([argv for argv, _ in commands if argv[:2] == ["docker", "push"]]) == 1
    assert all(
        argv[-1] == "sdo-fastloop-registry" for argv, _ in commands if argv[:3] == ["docker", "container", "inspect"]
    )
    pulls = [argv for argv, _ in commands if "pull" in argv]
    assert len(pulls) == 4
    assert all(argv[-1].startswith(host + "/") and "@sha256:" in argv[-1] for argv in pulls)
    assert {argv[2] for argv in pulls} == {node for evidence in (first, second) for node in evidence}
    configs = [kwargs["input"] for argv, kwargs in commands if "/dev/stdin" in argv]
    assert len(configs) == 4
    assert all("http://sdo-fastloop-registry:5000" in config for config in configs)
    assert metrics["uploads"] == 1
    assert metrics["cache_hits"] == 0
    assert second_metrics["uploads"] == 0
    assert second_metrics["cache_hits"] == 1
    assert second_metrics["frozen_ids"] == {"controller:frozen": CONFIG}
    assert second_metrics["publication_seconds"] >= 0
    assert second_metrics["pull_seconds"] >= 0


def test_advancing_a_source_tag_preserves_old_build_and_skips_on_return(registry):
    commands, _, state, _ = registry
    first = image_delivery.registry_delivery("img1-a", ["controller:current"])
    state["config"] = OTHER_CONFIG
    second = image_delivery.registry_delivery("img1-b", ["controller:current"])
    state["config"] = CONFIG
    third = image_delivery.registry_delivery("img1-c", ["controller:current"])
    assert first["img1-a-worker"] == third["img1-c-worker"]
    assert first["img1-a-worker"] != second["img1-b-worker"]
    assert len([argv for argv, _ in commands if argv[:2] == ["docker", "push"]]) == 2
    tags = [argv for argv, _ in commands if argv[:2] == ["docker", "tag"]]
    assert [argv[2] for argv in tags] == [CONFIG, OTHER_CONFIG]
    assert all(":current" not in argv[-1] for argv in tags)


@pytest.mark.parametrize("failure", ["corrupt", "push_config", "node_config"])
def test_corrupt_manifest_or_wrong_frozen_image_is_rejected(registry, failure):
    commands, _, state, _ = registry
    state[failure] = True if failure == "corrupt" else OTHER_CONFIG
    with pytest.raises(ValueError, match="digest|identity"):
        image_delivery.registry_delivery("img1-a", ["controller:frozen"])
    if failure != "node_config":
        assert not any("pull" in argv for argv, _ in commands)


def test_existing_wrong_content_tag_is_never_a_cache_hit(registry):
    commands, manifests, state, _ = registry
    image_delivery.registry_delivery("img1-a", ["controller:frozen"])
    for key in list(manifests):
        manifests[key] = json.dumps({"config": {"digest": OTHER_CONFIG}}).encode()
    state["node_config"] = CONFIG
    before = len(commands)
    with pytest.raises(ValueError, match="identity"):
        image_delivery.registry_delivery("img1-b", ["controller:frozen"])
    assert not any("pull" in argv for argv, _ in commands[before:])
    assert len([argv for argv, _ in commands if argv[:2] == ["docker", "push"]]) == 1


def test_delivery_entrypoint_records_real_publication_and_cache_timings(registry, monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import toy_images
    from benchmarks.sregym.fastloop.host_limits import HostLimits

    monkeypatch.setattr(toy_images, "assert_host_disk_headroom", lambda **kw: {"passed": True})
    monkeypatch.setattr(toy_images, "retain_lane_images", lambda *a: {})
    path = tmp_path / "delivery.jsonl"
    for lane in ("img1-a", "img1-b"):
        toy_images.load_images(
            lane, ["controller:frozen"], record_path=path, host_limits=HostLimits(tmp_path / "locks")
        )
    first, second = [json.loads(line) for line in path.read_text().splitlines()]
    assert first["registry"]["uploads"] == 1
    assert second["registry"]["uploads"] == 0
    assert second["registry"]["cache_hits"] == 1
    assert second["registry"]["manifest_digests"] == first["registry"]["manifest_digests"]
    assert all(row["success"] and row["path"] == "registry" for row in (first, second))


@pytest.mark.parametrize("existing", [False, True])
def test_host_registry_creation_restart_and_network_port_refresh(monkeypatch, existing):
    calls = []
    ready = []
    connected = False
    inspected = 0

    def run(argv, **kwargs):
        nonlocal connected, inspected
        calls.append(argv)
        output = ""
        if argv[:3] == ["docker", "container", "inspect"]:
            inspected += 1
            if not existing and inspected == 1:
                return CompletedProcess(argv, 1, "", "No such container: sdo-fastloop-registry")
            output = json.dumps(
                [
                    {
                        "Config": {"Labels": {"sdo.fastloop.registry": "host-v1"}},
                        "State": {"Running": False},
                        "NetworkSettings": {
                            "Ports": {"5000/tcp": [{"HostPort": "32124" if connected else "32123"}]},
                            "Networks": {},
                        },
                    }
                ]
            )
        elif argv[:3] == ["docker", "network", "connect"]:
            connected = True
        return CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    monkeypatch.setattr(image_delivery, "_wait_registry", ready.append)
    assert image_delivery._host_registry() == "127.0.0.1:32124"
    assert ready == ["127.0.0.1:32124"]
    if existing:
        assert ["docker", "start", "sdo-fastloop-registry"] in calls
        assert not any(argv[:2] == ["docker", "run"] for argv in calls)
    else:
        created = next(argv for argv in calls if argv[:2] == ["docker", "run"])
        assert "sdo.fastloop.registry=host-v1" in created
        assert created[created.index("--name") + 1] == "sdo-fastloop-registry"


def test_unavailable_docker_does_not_create_a_registry(monkeypatch):
    from subprocess import CalledProcessError

    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return CompletedProcess(argv, 1, "", "Cannot connect to the Docker daemon")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    with pytest.raises(CalledProcessError):
        image_delivery._host_registry()
    assert calls == [["docker", "container", "inspect", "sdo-fastloop-registry"]]


def test_lane_cleanup_cannot_delete_host_registry(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return CompletedProcess(argv, 1, "", "No such container: img1-a-registry")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    image_delivery.remove_registry("img1-a")
    assert calls == [["docker", "container", "inspect", "img1-a-registry"]]


def test_identical_images_under_different_source_names_publish_once(registry):
    commands, _, _, _ = registry
    evidence = image_delivery.registry_delivery("img1-a", ["controller:one", "mirror.local:5000/copy:two"])
    assert len([argv for argv, _ in commands if argv[:2] == ["docker", "push"]]) == 1
    assert len(set(evidence["img1-a-worker"].values())) == 1
