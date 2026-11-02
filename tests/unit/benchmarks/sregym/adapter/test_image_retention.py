"""Lane GC deletes only old, unused, exclusively SDO-owned node images."""

import json
from subprocess import CompletedProcess

import pytest

from benchmarks.sregym.adapter import image_retention as retention


def image(version, repo="sdo-controller", **kwargs):
    return {
        "id": "sha256:" + version,
        "repoTags": [f"docker.io/library/{repo}:{version}"],
        "repoDigests": [],
        "size": "100",
        **kwargs,
    }


class NodeRuntime:
    def __init__(self):
        self.images = [image(v) for v in ("current", "previous", "old", "used")]
        self.images += [image("foreign", "mongo"), image("user", "someone/sdo-controller")]
        self.containers = [{"imageRef": "sha256:used", "image": {"image": "sdo-controller:used"}}]
        self.state = {"sdo-controller": ["sha256:previous", "sha256:old"]}
        self.removed = []

    def run(self, argv, **kwargs):
        argv = [part for part in argv if part != "-i"]
        assert argv[:3] == ["docker", "exec", "lane-worker"]
        command = argv[3:]
        output = ""
        if command == ["crictl", "images", "-o", "json"]:
            output = json.dumps({"images": self.images})
        elif command == ["crictl", "ps", "-a", "-o", "json"]:
            output = json.dumps({"containers": self.containers})
        elif command[0] == "cat":
            output = json.dumps(self.state)
        elif command[0] == "df":
            output = "Avail\n" + str(1000 + 50 * len(self.removed)) + "\n"
        elif command[:2] == ["crictl", "rmi"]:
            self.removed.append(command[-1])
            self.images = [row for row in self.images if row["id"] != command[-1]]
        elif command[:2] == ["sh", "-c"]:
            self.state = json.loads(kwargs["input"])
        else:
            pytest.fail(f"unexpected node operation: {argv}")
        return CompletedProcess(argv, 0, output, "")


def test_delivery_retains_current_previous_used_and_foreign_images(monkeypatch):
    runtime = NodeRuntime()
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    evidence = retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == ["sha256:old"]
    assert runtime.state == {"sdo-controller": ["sha256:current", "sha256:previous"]}
    assert evidence["freed_bytes"] == 50
    assert evidence["removed"] == [
        {"id": "sha256:old", "references": ["docker.io/library/sdo-controller:old"], "size_bytes": 100}
    ]


@pytest.mark.parametrize(
    "container",
    [
        {"imageRef": "sha256:old", "image": {"image": "sdo-controller:old"}},
        {"imageRef": "docker.io/library/sdo-controller@sha256:manifest", "image": {"image": "sdo-controller:old"}},
    ],
)
def test_in_use_identity_and_foreign_alias_prevent_removal(monkeypatch, container):
    runtime = NodeRuntime()
    runtime.containers.append(container)
    runtime.images[2]["repoDigests"] = ["docker.io/library/sdo-controller@sha256:manifest"]
    runtime.images.append(image("shared", repoTags=["sdo-controller:shared", "someone/backup:shared"]))
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


def test_repeat_and_partial_delivery_preserve_previous_and_other_roles(monkeypatch):
    runtime = NodeRuntime()
    runtime.state["sdo-responder"] = ["sha256:r2", "sha256:r1"]
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.state == {
        "sdo-controller": ["sha256:current", "sha256:previous"],
        "sdo-responder": ["sha256:r2", "sha256:r1"],
    }
    assert runtime.removed == ["sha256:old"]


def test_first_delivery_preserves_unknown_prior_generation(monkeypatch):
    runtime = NodeRuntime()
    runtime.state = {}
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []
    assert set(runtime.state["sdo-controller"]) == {"sha256:" + v for v in ("current", "previous", "old", "used")}


def test_app_only_delivery_does_not_rotate_sdo_history(monkeypatch):
    monkeypatch.setattr(retention.subprocess, "run", lambda *a, **kw: pytest.fail("app delivery triggers GC"))
    assert retention.retain_node_images("lane-worker", ["app:v1"]) == {"removed": [], "freed_bytes": 0}


def test_unseen_sdo_repository_is_not_pruned(monkeypatch):
    runtime = NodeRuntime()
    runtime.images.append(image("r-old", "sdo-responder"))
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == ["sha256:old"]


def test_container_starting_during_cleanup_is_protected(monkeypatch):
    runtime = NodeRuntime()
    original = runtime.run
    reads = 0

    def run(argv, **kwargs):
        nonlocal reads
        if argv[-5:] == ["crictl", "ps", "-a", "-o", "json"]:
            reads += 1
            if reads == 2:
                runtime.containers.append({"imageRef": "sha256:old"})
        return original(argv, **kwargs)

    monkeypatch.setattr(retention.subprocess, "run", run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


@pytest.mark.parametrize("state", [[], {"sdo-controller": []}, {"someone/sdo-controller": ["sha256:old"]}])
def test_malformed_history_refuses_deletion(monkeypatch, state):
    runtime = NodeRuntime()
    runtime.state = state
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    with pytest.raises(ValueError, match="history malformed"):
        retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


@pytest.mark.parametrize("fallback", [False, True])
def test_cleanup_follows_successful_delivery_and_is_in_evidence(monkeypatch, tmp_path, fallback):
    from benchmarks.sregym.fastloop import toy_images

    events = []
    monkeypatch.setattr(toy_images, "assert_host_disk_headroom", lambda **kw: events.append("disk") or {"passed": True})

    def deliver(*args, **kwargs):
        events.append("registry")
        if fallback:
            raise RuntimeError("offline")
        return {}

    def cleanup(cluster, images):
        events.append("cleanup")
        return {"freed_bytes": 42, "nodes": {"node": {"removed": [{"id": "old"}]}}}

    monkeypatch.setattr(toy_images, "registry_delivery", deliver)
    monkeypatch.setattr(toy_images, "_load_archive_images", lambda *a: events.append("archive"))
    monkeypatch.setattr(toy_images, "retain_lane_images", cleanup)
    path = tmp_path / "delivery.jsonl"
    toy_images.load_images("lane", ["sdo-controller:current"], record_path=path)
    assert events == ["disk", "registry", *(["archive"] if fallback else []), "cleanup"]
    record = json.loads(path.read_text())
    assert record["retention"]["freed_bytes"] == 42
    assert record["success"]


def test_configured_disk_floor_blocks_delivery_and_fallback(monkeypatch):
    from benchmarks.sregym.adapter.lane_resources import LaneResourcePolicy, evaluate_host_disk
    from benchmarks.sregym.fastloop import toy_images

    def disk(*, policy):
        return evaluate_host_disk(mount="fixture", total_bytes=1000, free_bytes=99, policy=policy)

    monkeypatch.setattr(toy_images, "assert_host_disk_headroom", disk)
    monkeypatch.setattr(toy_images, "registry_delivery", lambda *a: pytest.fail("delivery before disk gate"))
    monkeypatch.setattr(toy_images, "_load_archive_images", lambda *a: pytest.fail("fallback after disk refusal"))
    with pytest.raises(ValueError, match="image-delivery disk floor.*require >=100 bytes"):
        toy_images.load_images(
            "lane", ["sdo-controller:current"], resource_policy=LaneResourcePolicy(host_disk_floor_bytes=100)
        )


def test_failed_delivery_never_rotates_retention(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import toy_images

    monkeypatch.setattr(toy_images, "assert_host_disk_headroom", lambda **kw: {"passed": True})

    def fail(*args, **kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr(toy_images, "registry_delivery", fail)
    monkeypatch.setattr(toy_images, "_load_archive_images", fail)
    monkeypatch.setattr(toy_images, "retain_lane_images", lambda *a: pytest.fail("GC after failed delivery"))
    with pytest.raises(RuntimeError, match="delivery failed"):
        toy_images.load_images("lane", ["sdo-controller:current"], record_path=tmp_path / "delivery.jsonl")
    assert not json.loads((tmp_path / "delivery.jsonl").read_text())["success"]


def test_manifest_digest_only_container_identity_is_protected(monkeypatch):
    runtime = NodeRuntime()
    runtime.images[2]["repoDigests"] = ["docker.io/library/sdo-controller@sha256:manifest"]
    runtime.containers.append({"imageRef": "sha256:manifest"})
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


def test_partial_cleanup_failure_preserves_removed_evidence(monkeypatch):
    from subprocess import CalledProcessError

    runtime = NodeRuntime()
    runtime.images.append(image("old2"))
    original = runtime.run

    def run(argv, **kwargs):
        if argv[-3:] == ["crictl", "rmi", "sha256:old2"]:
            raise CalledProcessError(1, argv, stderr="runtime unavailable")
        return original(argv, **kwargs)

    monkeypatch.setattr(retention.subprocess, "run", run)
    with pytest.raises(retention.ImageRetentionError) as error:
        retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert error.value.evidence["removed"][0]["id"] == "sha256:old"
    assert error.value.evidence["freed_bytes"] == 50


@pytest.mark.parametrize("suffix", [":current", "@sha256:manifest"])
def test_registry_aliases_of_our_exact_repositories_are_collectable(monkeypatch, suffix):
    import hashlib

    runtime = NodeRuntime()
    key = hashlib.sha256(b"docker.io/library/sdo-controller").hexdigest()[:16]
    runtime.images[2]["repoDigests"] = [f"127.0.0.1:32124/sdo/{key}{suffix}"]
    runtime.images.append(
        image("foreign-alias", repoTags=["sdo-controller:foreign-alias", "127.0.0.1:32124/sdo/other:old"])
    )
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == ["sha256:old"]


def test_bootstrap_protects_previous_content_with_only_registry_alias(monkeypatch):
    import hashlib

    runtime = NodeRuntime()
    runtime.state = {}
    key = hashlib.sha256(b"docker.io/library/sdo-controller").hexdigest()[:16]
    runtime.images.append(image("alias-only", repoTags=[], repoDigests=[f"127.0.0.1:32124/sdo/{key}@sha256:alias"]))
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []
    assert "sha256:alias-only" in runtime.state["sdo-controller"]


@pytest.mark.parametrize(
    "reference",
    ["sdo-controller:current", "library/sdo-controller:current", "docker.io/library/sdo-controller:current"],
)
def test_equivalent_delivered_names_resolve_same_identity(monkeypatch, reference):
    runtime = NodeRuntime()
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", [reference])
    assert runtime.state["sdo-controller"] == ["sha256:current", "sha256:previous"]


def test_rollback_rotates_history_without_losing_current(monkeypatch):
    runtime = NodeRuntime()
    runtime.state = {"sdo-controller": ["sha256:current", "sha256:previous"]}
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:previous"])
    assert runtime.state == {"sdo-controller": ["sha256:previous", "sha256:current"]}
    assert runtime.removed == ["sha256:old"]


def test_duplicate_identity_cannot_hide_a_foreign_alias(monkeypatch):
    runtime = NodeRuntime()
    runtime.images.append(image("old", "someone/backup"))
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    with pytest.raises(ValueError, match="duplicate image identity"):
        retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


def test_container_tag_normalization_matches_delivered_names(monkeypatch):
    runtime = NodeRuntime()
    runtime.containers.append({"imageRef": "library/sdo-controller:old"})
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


def test_foreign_alias_added_during_cleanup_is_protected(monkeypatch):
    runtime = NodeRuntime()
    original = runtime.run
    reads = 0

    def run(argv, **kwargs):
        nonlocal reads
        if argv[-4:] == ["crictl", "images", "-o", "json"]:
            reads += 1
            if reads == 2:
                runtime.images[2]["repoTags"].append("someone/backup:old")
        return original(argv, **kwargs)

    monkeypatch.setattr(retention.subprocess, "run", run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == []


@pytest.mark.parametrize("foreign", [False, True])
def test_shared_registry_alias_needs_exclusively_owned_source_refs(monkeypatch, foreign):
    runtime = NodeRuntime()
    digest = "sha256:" + "a" * 64
    runtime.images[2]["repoDigests"] = ["127.0.0.1:32123/sdo/images@" + digest]
    if foreign:
        runtime.images[2]["repoTags"].append("someone/backup:old")
    # A bare shared alias proves no SDO role and grants no deletion authority.
    runtime.images.append(image("bare", repoTags=[], repoDigests=["127.0.0.1:32123/sdo/images@" + digest]))
    monkeypatch.setattr(retention.subprocess, "run", runtime.run)
    retention.retain_node_images("lane-worker", ["sdo-controller:current"])
    assert runtime.removed == ([] if foreign else ["sha256:old"])
