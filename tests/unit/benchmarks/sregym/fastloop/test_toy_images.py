"""Prepared node images are checked by manifest digest, not CRI config ID."""

from subprocess import CompletedProcess

from benchmarks.sregym.fastloop.toy_images import _load_archive_images as load_images


def test_complete_preloaded_manifest_avoids_archive_transfer(monkeypatch):
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if argv[:3] == ["kind", "get", "nodes"]:
            return CompletedProcess(argv, 0, "sdo-toy-control-plane\n", "")
        if argv[:3] == ["docker", "image", "inspect"]:
            return CompletedProcess(argv, 0, "sha256:manifest\n", "")
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
        if "check" in argv:
            output = "docker.io/library/toy:v1 oci sha256:old complete (3/3) 10MiB true\n"
        return CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.toy_images.subprocess.run", run)
    load_images("sdo-toy", ["toy:v1"])
    assert any(call[:3] == ["kind", "load", "image-archive"] for call in calls)
    assert any(call[:3] == ["docker", "image", "save"] and "--platform" in call for call in calls)
