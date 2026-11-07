"""Prepared node images are checked by manifest digest, not CRI config ID."""

import json
from subprocess import CompletedProcess

import pytest

from benchmarks.sregym.fastloop.image_delivery import ImageDeliveryError, node_reference
from benchmarks.sregym.fastloop.toy_images import _load_archive_images as load_images


def test_complete_preloaded_manifest_avoids_archive_transfer(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if argv[:3] == ["kind", "get", "nodes"]:
            return CompletedProcess(argv, 0, "sdo-toy-control-plane\n", "")
        if argv[:3] == ["docker", "image", "inspect"]:
            return CompletedProcess(argv, 0, "sha256:manifest\n", "")
        if "inspecti" in argv:
            return CompletedProcess(argv, 0, '{"status": {"id": "sha256:manifest"}}', "")
        if "check" in argv:
            return CompletedProcess(
                argv,
                0,
                "REF TYPE DIGEST STATUS SIZE UNPACKED\n"
                "docker.io/library/toy:v1 oci sha256:manifest complete (3/3) 10MiB false\n",
                "",
            )
        return CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_images.subprocess.run", run)
    load_images("sdo-toy", ["toy:v1"])
    assert not any(call[:2] == ["kind", "load"] for call in calls)
    assert any("io.cri-containerd.image=managed" in call for call in calls)


def test_stale_preloaded_image_is_loaded_from_host(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        output = "sdo-toy-control-plane\n" if argv[0] == "kind" else "sha256:new\n"
        if "inspecti" in argv:
            output = '{"status": {"id": "sha256:new"}}'
        if "check" in argv:
            output = "docker.io/library/toy:v1 oci sha256:old complete (3/3) 10MiB true\n"
        return CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_images.subprocess.run", run)
    load_images("sdo-toy", ["toy:v1"])
    assert any(call[:3] == ["kind", "load", "image-archive"] for call in calls)
    assert any(call[:3] == ["docker", "image", "save"] and "--platform" in call for call in calls)


PIN = "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v1.5.2@sha256:" + "e8" * 32
CONFIG = "sha256:" + "c0" * 32


class FakeNode:
    """A fake docker/kind/crictl world: images by CRI name, an archive import naming the image by date."""

    def __init__(self, *, import_aliases: bool = True) -> None:
        self.names: dict[str, str] = {}  # CRI name -> image id
        self.calls: list[list[str]] = []
        self.import_aliases = import_aliases

    def run(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[:3] == ["kind", "get", "nodes"]:
            return CompletedProcess(argv, 0, "n-control-plane\n", "")
        if argv[:3] == ["docker", "image", "inspect"]:
            return CompletedProcess(argv, 0, CONFIG + "\n", "")
        if argv[:3] == ["kind", "load", "image-archive"]:
            if self.import_aliases:
                self.names["docker.io/library/import-2026-11-06:latest"] = CONFIG
            return CompletedProcess(argv, 0, "", "")
        if "check" in argv:
            return CompletedProcess(argv, 0, "REF TYPE DIGEST STATUS SIZE UNPACKED\n", "")
        if "inspecti" in argv:
            image = argv[-1]
            if image == PIN:  # the CRI drops the tag when a digest is present
                image = image.split("@")[0].rsplit(":", 1)[0] + "@" + image.split("@")[1]
            image = image if image.count("/") > 1 else "docker.io/" + image
            if image not in self.names:
                return CompletedProcess(argv, 1, "", "no such image")
            return CompletedProcess(argv, 0, json.dumps({"status": {"id": self.names[image]}}), "")
        if "--force" in argv:
            self.names[argv[-1]] = self.names[argv[-2]]
            return CompletedProcess(argv, 0, "", "")
        if "images" in argv and argv[-2:] == ["-o", "json"]:
            rows = [{"id": i, "repoTags": [n], "repoDigests": []} for n, i in self.names.items()]
            return CompletedProcess(argv, 0, json.dumps({"images": rows}), "")
        return CompletedProcess(argv, 0, "", "")


def test_digest_pinned_untagged_image_is_delivered_under_the_manifest_reference(monkeypatch):
    node = FakeNode()
    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_images.subprocess.run", node.run)
    load_images("sdo-toy", [PIN])
    cri_name = "registry.k8s.io/ingress-nginx/kube-webhook-certgen@sha256:" + "e8" * 32
    assert node.names[cri_name] == CONFIG
    assert node_reference(PIN) == cri_name


def test_delivery_fails_fast_when_the_reference_cannot_be_produced(monkeypatch):
    node = FakeNode(import_aliases=False)
    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_images.subprocess.run", node.run)
    with pytest.raises(ImageDeliveryError, match="cannot resolve image .*kube-webhook-certgen:v1.5.2@sha256"):
        load_images("sdo-toy", [PIN])


def test_node_reference_keeps_tag_only_and_drops_tag_beside_digest():
    assert node_reference("toy:v1") == "docker.io/library/toy:v1"
    assert node_reference("localhost:5000/a/b@sha256:" + "a" * 64) == "localhost:5000/a/b@sha256:" + "a" * 64
    assert node_reference("a/b:t@sha256:" + "a" * 64) == "docker.io/a/b@sha256:" + "a" * 64
