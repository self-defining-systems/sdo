"""A claim's seed: a learned memory imported into a pool lane's accepted repository by ``swap``."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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


def test_the_restore_pod_receives_the_bundle_in_its_own_writable_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    """The controller image runs with a read-only root: the bundle goes to an emptyDir, never to /tmp."""
    head = "a" * 40
    job = {
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "repo"}}],
                    "containers": [
                        {
                            "name": hooks.CONTROLLER_CONTAINER,
                            "image": "img",
                            "securityContext": {"readOnlyRootFilesystem": True},
                            "volumeMounts": [{"name": "repository", "mountPath": "/repo"}],
                        }
                    ],
                }
            }
        }
    }
    applied: list[dict[str, Any]] = []

    def fake_run(cmd: list[str], what: str, timeout: float, **kw: Any) -> Any:
        if kw.get("input") is not None:
            applied.append(json.loads(kw["input"]))
        return SimpleNamespace(stdout=head, stderr="")

    monkeypatch.setattr(hooks, "_run", fake_run)
    monkeypatch.setattr(hooks, "send_bundle", lambda *a, **k: None)
    monkeypatch.setattr(
        hooks.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="Succeeded", stderr="", returncode=0)
    )
    kube = SimpleNamespace(_json=lambda *a: job, cmd=lambda *a: list(a))
    hooks.restore_repository(kube, "ctl", head, bundle=Path("b"))  # type: ignore[arg-type]
    pod = applied[0]["spec"]
    container = pod["containers"][0]
    assert {"name": "seed", "mountPath": hooks.BUNDLE_DIR} in container["volumeMounts"]
    assert {"name": "seed", "emptyDir": {}} in pod["volumes"]
    assert "/tmp/" not in container["command"][2]
    assert f"{hooks.BUNDLE_DIR}/memory.bundle" in container["command"][2]
    # a memory with another history leaves no broker proposals or ledger describing the old one
    assert "rm -rf /repo/worktrees/*" in container["command"][2]


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
    script = hooks.restore_script(head, True, root=str(lane), bundle_dir=str(tmp_path))
    subprocess.run(["sh", "-c", script], check=True, capture_output=True)
    assert git(lane, "rev-parse", "HEAD") == head
