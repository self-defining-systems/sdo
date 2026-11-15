"""A claim's seed: a learned memory imported into a pool lane's accepted repository by ``swap``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmarks.fuzzops import hooks

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


def steps(calls: list[str], head: str = COMMIT, status: str = "", idle: str = "") -> hooks.SeedSteps:
    return hooks.SeedSteps(
        reset_incidents=lambda: calls.append("incidents"),
        retire_standby=lambda: calls.append("retire"),
        stop_controller=lambda: calls.append("stop"),
        import_memory=lambda commit: calls.append(f"import {commit[:1]}"),
        start_controller=lambda: calls.append("start"),
        wait_standby=lambda: calls.append("standby"),
        repository_state=lambda: {"head": head, "status": status, "outcomes": ""},
        controller_idle=lambda: idle,
    )


def test_seed_stops_the_controller_around_the_import(tmp_path: Path) -> None:
    calls: list[str] = []
    seed = baseline_dir(tmp_path, "seed", "r")
    receipt = hooks.apply_seed(steps(calls), seed, lambda _line: None)
    assert calls == ["incidents", "retire", "stop", "import a", "start", "standby"]
    assert receipt["seed"] == COMMIT


@pytest.mark.parametrize(
    ("head", "status", "idle", "message"),
    [
        ("b" * 40, "", "", "not aaaaaaaaaaaa"),
        (COMMIT, " M x", "", "not aaaaaaaaaaaa"),
        (COMMIT, "", "busy", "not idle"),
    ],
)
def test_seed_fails_unless_the_lane_is_the_seeds_memory_and_idle(
    tmp_path: Path, head: str, status: str, idle: str, message: str
) -> None:
    seed = baseline_dir(tmp_path, "seed", "r")
    with pytest.raises(hooks.HookError, match=message):
        hooks.apply_seed(steps([], head, status, idle), seed, lambda _line: None)


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
