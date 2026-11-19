"""A claim's seed: the baseline format of a learned memory and how ``swap`` takes it (the import itself is
``devswap.seed``, tested in ``tests/unit/benchmarks/devswap/test_seed.py``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmarks.devswap import controller
from benchmarks.fuzzops import config, hooks

COMMIT = "a" * 40


def baseline_dir(
    tmp_path: Path, name: str, render: str, commit: str = COMMIT, namespace: str = "fg1"
) -> hooks.Baseline:
    root = tmp_path / name
    root.mkdir()
    (root / "render.yaml").write_text(render)
    (root / "memory.bundle").write_bytes(b"bundle")
    manifest = {
        "namespace": namespace,
        "bundle": "memory.bundle",
        "branch": "main",
        "commit": commit,
        "render": "render.yaml",
    }
    (root / hooks.BASELINE_FILE).write_text(json.dumps(manifest))
    return hooks.Baseline.load(root)


def test_a_seed_made_on_another_render_or_namespace_is_refused(tmp_path: Path) -> None:
    lane = baseline_dir(tmp_path, "lane", "render one", commit="c" * 40)
    assert hooks.seed_problem(baseline_dir(tmp_path, "same", "render one"), lane) == ""
    assert "render" in hooks.seed_problem(baseline_dir(tmp_path, "other", "render two"), lane)
    assert "namespace" in hooks.seed_problem(baseline_dir(tmp_path, "ns", "render one", namespace="fg2"), lane)


def test_the_operator_block_carries_the_seed(tmp_path: Path) -> None:
    path = tmp_path / "operator.json"
    path.write_text(
        json.dumps({"hooks": "sdo", "revision": "abcdef12", "baseline": "/b", "seed": "/s", "settings": {}})
    )
    assert hooks.OperatorBlock.load(path).seed == Path("/s")
    path.write_text(json.dumps({"hooks": "sdo", "revision": "abcdef12", "baseline": "/b", "settings": {}}))
    assert hooks.OperatorBlock.load(path).seed is None


def test_the_restore_script_imports_a_bundle_whatever_its_refs_are_named(tmp_path: Path) -> None:
    """A learned memory's bundle names its branch (no HEAD ref); the restore must still find the commit."""
    import subprocess

    def git(cwd: Path, *args: str) -> str:
        env = {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@t",
        }
        return subprocess.run(
            ["git", "-c", "safe.directory=*", *args], cwd=cwd, env={**__import__("os").environ, **env},
            capture_output=True, text=True, check=True,
        ).stdout.strip()  # fmt: skip

    source, lane = tmp_path / "source", tmp_path / "lane"
    for repo in (source, lane):
        repo.mkdir()
        git(repo, "init", "-q", "-b", "main")
    git(source, "commit", "-q", "--allow-empty", "-m", "learned")
    head = git(source, "rev-parse", "HEAD")
    git(source, "branch", "learned-branch")
    git(source, "bundle", "create", str(tmp_path / "m.bundle"), "learned-branch")
    (tmp_path / "memory.bundle").write_bytes((tmp_path / "m.bundle").read_bytes())
    (tmp_path / "memory.ready").touch()
    script = controller.restore_script(head, True, root=str(lane), bundle_dir=str(tmp_path))
    subprocess.run(["sh", "-c", script], check=True, capture_output=True)
    assert git(lane, "rev-parse", "HEAD") == head


def test_merge_config_lays_the_overlay_over_the_installed_document() -> None:
    base = {"response": {"timeout": "1800s", "page_only": False}, "agent": {"model": "m"}}
    merged = config.merge(base, {"response": {"page_only": True}})
    assert merged == {"response": {"timeout": "1800s", "page_only": True}, "agent": {"model": "m"}}
    assert base["response"]["page_only"] is False


def test_a_seed_may_overlay_what_the_hook_sets_for_an_install(tmp_path: Path) -> None:
    seed = baseline_dir(tmp_path, "seed", "r")
    manifest = json.loads((seed.root / hooks.BASELINE_FILE).read_text())  # type: ignore[operator]
    manifest["lane_overlay"] = {"agent": {"model": "other"}}
    (seed.root / hooks.BASELINE_FILE).write_text(json.dumps(manifest))  # type: ignore[operator]
    assert hooks.Baseline.load(seed.root).lane_overlay == {"agent": {"model": "other"}}
    manifest["config"] = {"agent": {"model": "other"}}
    (seed.root / hooks.BASELINE_FILE).write_text(json.dumps(manifest))  # type: ignore[operator]
    with pytest.raises(ValueError, match="agent.model"):
        hooks.Baseline.load(seed.root)  # type: ignore[arg-type]


def test_baseline_state_reads_stores_and_promotions_from_the_controller_log() -> None:
    log = (
        '{"controller_iteration":1,"evaluated_detectors":["cluster-wiring","store-state-postgresql","store-state-redis"]}\n'
        "2026/11/16 05:20:00 INFO store_baseline_promoted component=postgresql\n"
    )
    expected, promoted = hooks.baseline_state(log)
    assert expected == {"postgresql", "redis"}
    assert promoted == {"postgresql"}
