"""Source-built app images are built once per content identity and pulled by every other lane and host."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from benchmarks.sregym.fastloop import app_images

IDENTITY = "1" * 64
IMAGE = f"org/app:sregym-src-{IDENTITY}"


class Registry:
    """A real HTTP registry endpoint; an image's config blob is its identity label."""

    def __init__(self) -> None:
        self.manifests: dict[str, bytes] = {}
        self.blobs: dict[str, bytes] = {}
        registry = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/v2/":
                    self.send_response(200)
                    self.end_headers()
                    return
                raw = registry.manifests.get(self.path) or registry.blobs.get(self.path.rpartition("/")[2])
                if raw is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Docker-Content-Digest", "sha256:" + hashlib.sha256(raw).hexdigest())
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.host = f"127.0.0.1:{self.server.server_port}"

    def push(self, repository: str, tag: str, label: str) -> None:
        config = "sha256:" + hashlib.sha256(label.encode()).hexdigest()
        self.blobs[config] = label.encode()
        raw = json.dumps({"schemaVersion": 2, "config": {"digest": config, "size": len(label)}, "layers": []}).encode()
        digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        for selector in (tag, digest):
            self.manifests[f"/v2/{repository}/manifests/{selector}"] = raw


class Loader:
    """`docker load` reading the streamed archive: its legacy manifest names the image, its config is the label."""

    def __init__(self, host: Host) -> None:
        self.host = host
        self.returncode = 0
        self.stdout = io.BytesIO(b"")
        loader = self

        class Stdin(io.BytesIO):
            def close(self):
                loader.load(self.getvalue())
                super().close()

        self.stdin = Stdin()

    def load(self, data: bytes) -> None:
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            [entry] = json.loads(archive.extractfile("manifest.json").read())
            [descriptor] = json.loads(archive.extractfile("index.json").read())["manifests"]
            # The containerd store resolves only fully qualified names.
            assert descriptor["annotations"]["io.containerd.image.name"] == "docker.io/" + entry["RepoTags"][0]
            label = archive.extractfile(entry["Config"]).read().decode()
            self.host.images[entry["RepoTags"][0]] = label

    def kill(self) -> None:
        self.returncode = -9

    def wait(self, timeout=None) -> int:
        return self.returncode


class Host:
    """One Docker daemon: its local image store and the commands it ran."""

    def __init__(self, registries: dict[str, Registry]) -> None:
        self.registries = registries
        self.images: dict[str, str] = {}
        self.commands: list[list[str]] = []
        self.builds = 0

    def popen(self, argv, **_kwargs):
        assert argv == ["docker", "load"]
        self.commands.append(list(argv))
        return Loader(self)

    def run(self, argv, **_kwargs):
        self.commands.append(list(argv))
        if argv[:2] == ["docker", "tag"]:
            self.images[argv[3]] = self.images[argv[2]]
        elif argv[:2] == ["docker", "push"]:
            registry, _, rest = argv[2].partition("/")
            if registry not in self.registries:
                raise subprocess.CalledProcessError(1, argv, "", "connection refused")
            repository, _, tag = rest.rpartition(":")
            self.registries[registry].push(repository, tag, self.images[argv[2]])
        return CompletedProcess(argv, 0, "", "")

    def ensure(self, registry: str, lock_dir: Path, *, build_seconds: float = 0.0) -> dict:
        def build() -> None:
            self.builds += 1
            time.sleep(build_seconds)
            self.images[IMAGE] = IDENTITY

        return app_images.ensure_app_image(
            IMAGE, IDENTITY, build, local_identity=self.images.get, registry=registry, lock_dir=lock_dir
        )


def use(monkeypatch, host: Host) -> None:
    monkeypatch.setattr(subprocess, "run", host.run)
    monkeypatch.setattr(subprocess, "Popen", host.popen)


@pytest.fixture
def shared():
    registry = Registry()
    yield registry
    registry.server.shutdown()
    registry.server.server_close()


def test_two_lanes_build_once_and_a_second_host_pulls_from_the_one_shared_registry(shared, monkeypatch, tmp_path):
    first_host, second_host = Host({shared.host: shared}), Host({shared.host: shared})

    use(monkeypatch, first_host)
    built = first_host.ensure(shared.host, tmp_path / "a-locks")
    reused = first_host.ensure(shared.host, tmp_path / "a-locks")
    assert first_host.builds == 1
    assert (built["source"], built["published"]) == ("build", True)
    assert (reused["source"], reused["published"]) == ("local", False)

    # Another host names the same registry (through its loopback relay) and never publishes elsewhere.
    use(monkeypatch, second_host)
    pulled = second_host.ensure(shared.host, tmp_path / "b-locks")
    again = second_host.ensure(shared.host, tmp_path / "b-locks")
    assert second_host.builds == 0
    assert (pulled["source"], pulled["published"]) == ("registry", False)
    assert again["source"] == "local"
    assert second_host.images[IMAGE] == IDENTITY
    assert ["docker", "load"] in second_host.commands
    assert not any(argv[:2] == ["docker", "push"] for argv in second_host.commands)


def test_concurrent_lanes_wait_for_one_build_of_an_identity(shared, monkeypatch, tmp_path):
    host = Host({shared.host: shared})
    use(monkeypatch, host)
    results = []
    lanes = [
        threading.Thread(target=lambda: results.append(host.ensure(shared.host, tmp_path, build_seconds=0.3)))
        for _ in range(3)
    ]
    for lane in lanes:
        lane.start()
    for lane in lanes:
        lane.join()
    assert host.builds == 1
    assert sorted(result["source"] for result in results) == ["build", "local", "local"]


def test_registry_content_without_the_identity_never_deploys(shared, monkeypatch, tmp_path):
    shared.push(app_images.APP_REPOSITORY, IMAGE.rpartition(":")[2], "2" * 64)
    host = Host({shared.host: shared})
    use(monkeypatch, host)
    with pytest.raises(ValueError, match="does not carry identity"):
        host.ensure(shared.host, tmp_path)
    assert host.builds == 0


def test_a_registry_blob_that_is_not_its_digest_never_loads(shared, monkeypatch, tmp_path):
    shared.push(app_images.APP_REPOSITORY, IMAGE.rpartition(":")[2], IDENTITY)
    [config] = shared.blobs
    shared.blobs[config] = b"2" * 64
    host = Host({shared.host: shared})
    use(monkeypatch, host)
    with pytest.raises(ValueError, match="blob digest mismatch"):
        host.ensure(shared.host, tmp_path)
    assert IMAGE not in host.images


def test_unreachable_shared_registry_is_recorded_and_the_lane_still_deploys(monkeypatch, tmp_path):
    host = Host({})
    use(monkeypatch, host)
    record = host.ensure("127.0.0.1:9", tmp_path)
    assert (host.builds, record["source"], record["published"]) == (1, "build", False)
    assert record["registry_error"]
    assert not any(argv[:2] == ["docker", "push"] for argv in host.commands)


def test_the_shared_registry_is_configured_once_for_every_host():
    assert app_images.shared_registry({app_images.REGISTRY_ENV: " 127.0.0.1:55300 "}) == "127.0.0.1:55300"


def test_image_not_tagged_by_its_identity_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="not tagged by its identity"):
        app_images.ensure_app_image(
            "org/app:sregym-src-lane-1",
            IDENTITY,
            lambda: None,
            local_identity={}.get,
            registry="127.0.0.1:9",
            lock_dir=tmp_path,
        )


@pytest.mark.requires_sregym
def test_real_source_adapter_on_a_fresh_host_pulls_what_another_host_built(shared, monkeypatch, tmp_path):
    """Producer to consumer: SREGym's hotel adapter on two hosts through the bridge's shared builds."""
    import importlib

    root = Path(__file__).resolve().parents[5]
    monkeypatch.syspath_prepend(str(root / "third_party" / "sregym"))
    source_deploy = importlib.import_module("sregym.service.source_deploy")
    source = tmp_path / "hotel"
    source.mkdir()
    (source / "Dockerfile").write_text("FROM scratch\nCOPY . /src\n")
    (source / "main.go").write_text("package main\n")
    monkeypatch.setenv("SREGYM_SOURCE_BUILD_CACHE", "1")
    monkeypatch.setattr(source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"], "_source_dir", lambda: source)
    monkeypatch.setattr(source_deploy, "_kind_load_image", lambda **kwargs: None)
    monkeypatch.setattr(source_deploy, "tag_image_in_kind_nodes", lambda *args: None)
    build_image = source_deploy._build_image

    class DockerHost(Host):
        def run(self, argv, **kwargs):
            if argv[:3] == ["docker", "image", "inspect"]:
                self.commands.append(list(argv))
                label = self.images.get(argv[-1])
                return CompletedProcess(argv, 0 if label else 1, (label or "") + "\n", "")
            if argv[:2] == ["docker", "build"]:
                self.commands.append(list(argv))
                self.builds += 1
                self.images[argv[argv.index("-t") + 1]] = argv[argv.index("--label") + 1].split("=", 1)[1]
                return CompletedProcess(argv, 0, "", "")
            return super().run(argv, **kwargs)

    record = tmp_path / "app_images.jsonl"
    hosts = [DockerHost({shared.host: shared}), DockerHost({shared.host: shared})]
    monkeypatch.setenv(app_images.REGISTRY_ENV, shared.host)
    for index, host in enumerate(hosts):
        use(monkeypatch, host)
        monkeypatch.setattr(source_deploy, "_BUILT_IMAGES", set())
        monkeypatch.setattr(source_deploy, "_build_image", build_image)
        app_images.install_shared_builds(source_deploy, record_path=record, lock_dir=tmp_path / f"host-{index}")
        for lane in ("lane-1", "lane-2"):
            source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"].ensure_images_loaded(
                cluster_name=lane, node_architectures={"amd64"}
            )
    assert [host.builds for host in hosts] == [1, 0]
    sources = [json.loads(line)["source"] for line in record.read_text().splitlines()]
    assert sources == ["build", "local", "registry", "local"]
    assert {image for image in hosts[1].images if image.startswith("yinfangchen/hotelreservation:")} == {
        image for image in hosts[0].images if image.startswith("yinfangchen/hotelreservation:")
    }
