"""``fastloop up`` refuses lane images built from other inputs than the tree it will run, from labels alone."""

import json
import subprocess
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop.image_provenance import (
    IMAGE_TARGET_LABEL,
    MANIFEST_TREE_LABEL,
    REVISION_LABEL,
    SOURCE_TREE_LABEL,
    WORKER_TARGET_LABEL,
    image_source,
)

REAL_RUN = subprocess.run


def labels_of(repo, target: str, commit: str = "HEAD", worker_target: str | None = None) -> dict[str, str]:
    """The labels `scripts/build_sdo_images.sh` records for an image built from ``commit``."""
    source = image_source(repo.path, commit, target, worker_target)
    labels = {
        IMAGE_TARGET_LABEL: target,
        REVISION_LABEL: repo.git("rev-parse", commit),
        SOURCE_TREE_LABEL: source.identity,
        MANIFEST_TREE_LABEL: source.manifest_identity,
    }
    if worker_target:
        labels[WORKER_TARGET_LABEL] = worker_target
    return labels


def fake_inspect(monkeypatch, images: dict[str, dict[str, str]]) -> list[list[str]]:
    """Answer `docker image inspect` from ``images``; any other docker command fails the test."""
    from benchmarks.sregym.fastloop import source_preflight

    calls: list[list[str]] = []

    def run(args, **kwargs):
        if args[0] == "git":
            return REAL_RUN(args, **kwargs)
        calls.append(list(args))
        if args[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(args, 0, stdout="27.0\n")
        assert args[:3] == ["docker", "image", "inspect"], f"freshness may only read labels: {args}"
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps([{"Config": {"Labels": images[args[3]]}}]))

    monkeypatch.setattr(source_preflight.subprocess, "run", run)
    return calls


def check(repo, *, dev_mount: bool = True, **roles: str) -> None:
    from benchmarks.sregym.fastloop.source_preflight import assert_images_fresh

    assert_images_fresh(roles, repo.path, dev_mount=dev_mount)


def test_images_built_from_the_current_tree_pass_with_and_without_a_mount(image_repo, monkeypatch):
    images = {"c:1": labels_of(image_repo, "controller"), "r:1": labels_of(image_repo, "benchmark")}
    images["r:1"] = labels_of(image_repo, "benchmark", worker_target="sregym-responder")
    calls = fake_inspect(monkeypatch, images)
    for dev_mount in (True, False):
        check(image_repo, dev_mount=dev_mount, controller="c:1", responder="r:1")
    assert calls
    assert all(call[:3] == ["docker", "image", "inspect"] for call in calls)


@pytest.mark.parametrize("manifest", ["sdo/operator/go.mod", "sdo/operator/go.sum", "sdo/operator/Dockerfile.runtime"])
def test_manifest_change_after_the_build_fails_even_with_a_dev_mount(image_repo, monkeypatch, manifest):
    labels = labels_of(image_repo, "controller")
    image_repo.commit({manifest: (image_repo.path / manifest).read_text() + "\n# dep added\n"}, "dependency change")
    fake_inspect(monkeypatch, {"sdo-controller:oc1": labels})
    with pytest.raises(ValueError, match=rf"controller image sdo-controller:oc1 .*{manifest}") as raised:
        check(image_repo, controller="sdo-controller:oc1")
    assert "SDO_IMAGES=controller SDO_IMAGE_TAG=oc1 bash scripts/build_sdo_images.sh" in str(raised.value)
    assert "--allow-stale-images" in str(raised.value)


def test_code_edits_do_not_fail_a_dev_mount_lane_but_fail_a_mountless_one(image_repo, monkeypatch):
    labels = labels_of(image_repo, "controller")
    image_repo.commit({"sdo/operator/main.go": "package main // edited\n"}, "code change")
    fake_inspect(monkeypatch, {"c:1": labels})
    check(image_repo, controller="c:1")
    with pytest.raises(ValueError, match=r"sdo/operator/main.go"):
        check(image_repo, dev_mount=False, controller="c:1")


def test_unlabelled_image_is_refused_with_the_rebuild_line(image_repo, monkeypatch):
    fake_inspect(monkeypatch, {"sdo-controller:old": {}, "sdo-sregym-responder:old": {}})
    with pytest.raises(ValueError, match=r"records no source inputs.*SDO_IMAGES=controller SDO_IMAGE_TAG=old"):
        check(image_repo, controller="sdo-controller:old")
    with pytest.raises(ValueError, match=r"SDO_IMAGES=benchmark SDO_IMAGE_TAG=old"):
        check(image_repo, responder="sdo-sregym-responder:old")


def test_image_labelled_only_with_a_revision_is_judged_by_that_revisions_manifests(image_repo, monkeypatch):
    built = {IMAGE_TARGET_LABEL: "controller", REVISION_LABEL: image_repo.head}
    image_repo.commit({"sdo/operator/main.go": "package main // edited\n"}, "code change")
    fake_inspect(monkeypatch, {"c:1": built})
    check(image_repo, controller="c:1")
    image_repo.commit({"sdo/operator/go.sum": "h1:new\n"}, "module change")
    with pytest.raises(ValueError, match=r"go\.sum"):
        check(image_repo, controller="c:1")


def test_unknown_revision_without_a_matching_tree_label_is_refused(image_repo, monkeypatch):
    labels = {**labels_of(image_repo, "controller"), REVISION_LABEL: "0" * 40}
    image_repo.commit({"sdo/operator/go.sum": "h1:new\n"}, "module change")
    fake_inspect(monkeypatch, {"c:1": labels})
    with pytest.raises(ValueError, match="cannot compare"):
        check(image_repo, controller="c:1")


def test_uncommitted_manifest_edit_is_refused_but_uncommitted_code_is_not_with_a_mount(image_repo, monkeypatch):
    labels = labels_of(image_repo, "controller")
    fake_inspect(monkeypatch, {"c:1": labels})
    (image_repo.path / "sdo/operator/main.go").write_text("package main // wip\n")
    check(image_repo, controller="c:1")
    (image_repo.path / "sdo/operator/go.mod").write_text("module fixture\n\nrequire x v1\n")
    with pytest.raises(ValueError, match=r"uncommitted changes \(sdo/operator/go\.mod\)"):
        check(image_repo, controller="c:1")


def test_scripted_worker_image_is_judged_as_the_scripted_stage_and_rebuilt_as_one(image_repo, monkeypatch):
    scripted = labels_of(image_repo, "benchmark", worker_target="scripted-worker")
    plain = labels_of(image_repo, "benchmark", worker_target="sregym-responder")
    assert scripted[MANIFEST_TREE_LABEL] != plain[MANIFEST_TREE_LABEL]
    image_repo.commit(
        {
            "benchmarks/sregym/Dockerfile.worker": (image_repo.path / "benchmarks/sregym/Dockerfile.worker").read_text()
            + "\n# bwrap\n"
        },
        "worker tools",
    )
    fake_inspect(monkeypatch, {"w:s": scripted})
    with pytest.raises(ValueError, match=r"Dockerfile\.worker") as raised:
        check(image_repo, responder="w:s")
    assert "SDO_SCRIPTED_WORKER=1 SDO_IMAGES=benchmark SDO_IMAGE_TAG=s bash" in str(raised.value)


def test_benchmark_image_follows_its_responder_base(image_repo, monkeypatch):
    labels = labels_of(image_repo, "benchmark", worker_target="sregym-responder")
    image_repo.commit({"sdo/operator/go.sum": "h1:new\n"}, "module change under the responder base")
    fake_inspect(monkeypatch, {"w:1": labels})
    with pytest.raises(ValueError, match=r"go\.sum"):
        check(image_repo, responder="w:1")


def stale_up_args(**overrides):
    values = {
        "kind_worker_nodes": 1,
        "app": "toy",
        "sregym_dir": None,
        "controller_image": "c:1",
        "responder_image": "r:1",
    }
    return SimpleNamespace(**{**values, **overrides})


def test_up_preflight_rejects_stale_images_before_any_smoke_and_the_flag_overrides(image_repo, monkeypatch):
    import shutil

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import cli, source_preflight

    controller, responder = (
        labels_of(image_repo, "controller"),
        labels_of(image_repo, "benchmark", worker_target="sregym-responder"),
    )
    image_repo.commit({"sdo/operator/go.sum": "h1:new\n"}, "module change")
    fake_inspect(monkeypatch, {"c:1": controller, "r:1": responder})
    monkeypatch.setattr(cli.subprocess, "run", source_preflight.subprocess.run)
    monkeypatch.setattr(shutil, "which", lambda _: "/fixture/tool")
    monkeypatch.setattr(cli, "check_host_capacity", lambda _: None)
    smoked = []
    monkeypatch.setattr(adapter, "assert_image_entrypoints", lambda ref, modules, **kw: smoked.append(ref))
    with pytest.raises(ValueError, match=r"controller image c:1 .*go\.sum"):
        cli.up_preflight(stale_up_args(dev_mount=image_repo.path))
    assert smoked == []
    cli.up_preflight(stale_up_args(dev_mount=image_repo.path, allow_stale_images=True))
    assert smoked == ["c:1", "r:1"]


@pytest.mark.parametrize("command", ["up", "preflight"])
def test_override_flag_exists_on_every_command_that_checks_images(command):
    from benchmarks.sregym.fastloop import cli

    extra = ["--run-dir", "x"] if command == "up" else []
    assert cli.build_parser().parse_args([command, *extra, "--allow-stale-images"]).allow_stale_images is True
    assert cli.build_parser().parse_args([command, *extra]).allow_stale_images is False


def test_a_lane_started_with_the_override_skips_the_run_time_source_check(tmp_path):
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="c",
        kubeconfig=tmp_path / "k",
        namespace="n",
        application="a",
        workspace=tmp_path,
        sregym_dir=tmp_path,
        private_tmp=None,
        allow_stale_images=True,
    )
    assert FastloopEnvironment.model_validate_json(environment.model_dump_json()).allow_stale_images is True
