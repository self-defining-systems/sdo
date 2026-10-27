"""Image provenance follows the tree of exactly the paths each image is built from."""

import json
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[5]
REAL_RUN = subprocess.run


def preflight(monkeypatch, repo, labels: dict[str, dict[str, str]]) -> None:
    """Run the real preflight with fake Docker; the responder is always current-source."""
    from benchmarks.sregym.fastloop import source_preflight
    from benchmarks.sregym.fastloop.environment import Images

    labels = {**labels, "responder": {"org.opencontainers.image.revision": repo.head}}

    def run(args, **kwargs):
        if args[0] == "git":
            return REAL_RUN(args, **kwargs)
        if args[:3] == ["docker", "image", "inspect"]:
            role = next(role for role in labels if role in args[3])
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps([{"Config": {"Labels": labels[role]}}]))
        return subprocess.CompletedProcess(args, 0, stdout="go version fixture\n")

    monkeypatch.setattr(source_preflight.subprocess, "run", run)
    source_preflight.assert_runtime_images(
        Images(controller="sdo-controller:x", validator="sdo-detector-validator:x", responder="responder:x"),
        repo.path,
    )


def revision_labels(revision: str) -> dict[str, dict[str, str]]:
    label = {"org.opencontainers.image.revision": revision}
    return {"controller": label, "validator": label}


def test_benchmarks_only_commit_keeps_images_built_from_its_parent(image_repo, monkeypatch):
    built = image_repo.head
    image_repo.commit({"benchmarks/sregym/fastloop/admission.py": "BUDGET = 2\n"}, "eval-only fix")
    preflight(monkeypatch, image_repo, revision_labels(built))


@pytest.mark.parametrize(
    "path", ["sdo/operator/main_test.go", "sdo/operator/testdata/case.json", "sdo/operator/README.md"]
)
def test_files_the_build_context_drops_never_invalidate_images(image_repo, monkeypatch, path):
    built = image_repo.head
    image_repo.commit({path: (image_repo.path / path).read_text() + "changed\n"}, "test-only change")
    image_repo.commit({"sdo/operator/new_test.go": "package main\n"}, "new test")
    preflight(monkeypatch, image_repo, revision_labels(built))


def test_test_file_edits_in_the_worktree_are_not_uncommitted_image_source(image_repo, monkeypatch):
    built = image_repo.head
    (image_repo.path / "sdo/operator/main_test.go").write_text("package main // edited\n")
    (image_repo.path / "sdo/operator/other_test.go").write_text("package main\n")
    preflight(monkeypatch, image_repo, revision_labels(built))
    (image_repo.path / "sdo/operator/other.go").write_text("package main\n")
    with pytest.raises(ValueError, match="uncommitted"):
        preflight(monkeypatch, image_repo, revision_labels(built))


def test_tree_label_ignores_test_files_and_follows_production_go(image_repo):
    from benchmarks.sregym.fastloop.image_provenance import image_source

    before = image_source(image_repo.path, "HEAD", "controller").identity
    image_repo.commit({"sdo/operator/main_test.go": "package main // edited\n"}, "test-only")
    assert image_source(image_repo.path, "HEAD", "controller").identity == before
    image_repo.commit({"sdo/operator/main.go": "package main // edited\n"}, "production")
    assert image_source(image_repo.path, "HEAD", "controller").identity != before


def test_the_real_build_context_sends_no_go_tests():
    from benchmarks.sregym.fastloop.image_provenance import image_source

    for target in ("controller", "validator"):
        files = [path for path, _ in image_source(ROOT, "HEAD", target).objects]
        assert not [path for path in files if path.endswith("_test.go") or "/testdata/" in path]
        assert "sdo/operator/go.mod" in files


def test_unsupported_dockerignore_exceptions_fail_closed():
    from benchmarks.sregym.fastloop.image_provenance import _ignore_matcher

    with pytest.raises(ValueError, match="exception"):
        _ignore_matcher("**/*_test.go\n!keep_test.go\n")


@pytest.mark.parametrize("path", ["sdo/operator/main.go", "sdo/agents/registrations/responder.json", ".dockerignore"])
def test_change_under_a_built_path_names_the_differing_file(image_repo, monkeypatch, path):
    built = image_repo.head
    image_repo.commit({path: (image_repo.path / path).read_text() + "// changed\n"}, "production change")
    with pytest.raises(ValueError, match=rf"eval: image sdo-controller:x .*{re.escape(path)}"):
        preflight(monkeypatch, image_repo, revision_labels(built))


def test_registration_change_reaches_controller_only(image_repo):
    from benchmarks.sregym.fastloop.image_provenance import image_source

    built = image_repo.head
    image_repo.commit({"sdo/agents/registrations/responder.json": '{"v": 2}\n'}, "registration")
    assert image_source(image_repo.path, built, "validator") == image_source(image_repo.path, "HEAD", "validator")
    assert image_source(image_repo.path, built, "controller") != image_source(image_repo.path, "HEAD", "controller")


def test_tree_label_accepts_image_whose_commit_is_unknown(image_repo, monkeypatch):
    from benchmarks.sregym.fastloop.image_provenance import SOURCE_TREE_LABEL, image_source

    labels = {
        target: {
            "org.opencontainers.image.revision": "0" * 40,
            SOURCE_TREE_LABEL: image_source(image_repo.path, "HEAD", target).identity,
        }
        for target in ("controller", "validator")
    }
    preflight(monkeypatch, image_repo, labels)
    labels["validator"][SOURCE_TREE_LABEL] = "tree-v1:stale"
    with pytest.raises(ValueError, match=r"sdo-detector-validator:x .*0{40}.*fetch"):
        preflight(monkeypatch, image_repo, labels)


def test_uncommitted_built_path_is_rejected(image_repo, monkeypatch):
    built = image_repo.head
    (image_repo.path / "sdo/agents/registrations/new.json").write_text("{}\n")
    with pytest.raises(ValueError, match="uncommitted"):
        preflight(monkeypatch, image_repo, revision_labels(built))
    (image_repo.path / "sdo/agents/registrations/new.json").unlink()
    (image_repo.path / "benchmarks/sregym/fastloop/scratch.py").write_text("x = 1\n")
    preflight(monkeypatch, image_repo, revision_labels(built))


def test_build_paths_follow_the_real_dockerfile_stage_graph():
    from benchmarks.sregym.fastloop.image_provenance import image_source

    assert image_source(ROOT, "HEAD", "controller").roots == (
        ".dockerignore",
        "sdo/agents/registrations",
        "sdo/operator",
    )
    assert image_source(ROOT, "HEAD", "validator").roots == (".dockerignore", "sdo/operator")


def test_every_source_checked_image_is_labelled_with_its_tree():
    bake = (ROOT / "scripts/sdo-images.hcl").read_text()
    build = (ROOT / "scripts/build_sdo_images.sh").read_text()
    for target in ("controller", "validator"):
        block = re.search(rf'^target "{target}" \{{(.*?)^\}}', bake, re.MULTILINE | re.DOTALL)
        assert block is not None
        variable = f"SDO_{target.upper()}_SOURCE_TREE"
        assert f'"dev.sdo.source-tree" = {variable}' in block[1]
        assert f"export {variable}=" in build


def test_unsupported_build_shapes_fail_closed():
    from benchmarks.sregym.fastloop.image_provenance import bake_target, stage_sources

    with pytest.raises(ValueError, match="named contexts"):
        bake_target((ROOT / "scripts/sdo-images.hcl").read_text(), "benchmark")
    with pytest.raises(ValueError, match="bind-mounts"):
        stage_sources("FROM x AS a\nRUN --mount=type=bind,target=/src make\n", "a")
    graph = 'FROM x AS a\nCOPY a.txt ./\nFROM a AS b\nCOPY --from=a /a /a\nADD ["b", "/b"]\n'
    assert stage_sources(graph, "b") == ("a.txt", "b")
