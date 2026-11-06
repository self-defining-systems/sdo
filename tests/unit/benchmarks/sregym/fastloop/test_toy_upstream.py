"""Toy upstream: manifests, seeding and scripted-commit replay against real local Git remotes."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
from typing import TYPE_CHECKING

import pytest

from apps.toy.variants.assemble import VARIANTS
from benchmarks.sregym.fastloop import toy_upstream as up
from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.toy import prepare_workspace

if TYPE_CHECKING:
    from pathlib import Path

LAYOUT = up.UpstreamLayout("sdo-toy")


def git(path: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True, text=True).stdout.strip()


def bare_remote(tmp_path: Path) -> str:
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-q", "-b", "main", str(remote)], check=True)
    return f"file://{remote}"


def seeded(tmp_path: Path, variant: str = "helm") -> tuple[Path, str]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    prepare_workspace(workspace, "unused:v1", fresh=True, variant=variant)
    return workspace, bare_remote(tmp_path)


def test_layout_rejects_bad_values():
    for bad in ("", "Has_Upper", "a" * 60):
        with pytest.raises(ValueError, match="DNS label"):
            up.UpstreamLayout(bad)
    with pytest.raises(ValueError, match="plain branch"):
        up.UpstreamLayout("sdo-toy", ref="../x")
    with pytest.raises(ValueError, match="poll interval"):
        up.UpstreamLayout("sdo-toy", poll_interval="soon")


def test_source_args_name_the_in_cluster_service():
    args = up.source_args(LAYOUT)
    assert args[:3] == ["--source-kind", "git", "--source-config"]
    assert json.loads(args[3]) == {
        "repository": "git://toy-upstream.sdo-toy-upstream.svc.cluster.local:9418/toy.git",
        "ref": "main",
        "poll_interval": "5s",
    }


def test_manifests_serve_push_enabled_daemon_in_the_upstream_namespace():
    namespace, deployment, service = up.upstream_manifests(LAYOUT, "img:1")
    assert namespace["metadata"]["name"] == "sdo-toy-upstream"
    assert {m["metadata"].get("namespace") for m in (deployment, service)} == {"sdo-toy-upstream"}
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "img:1"
    assert "--enable=receive-pack" in container["command"][-1]
    assert service["spec"]["ports"][0]["port"] == up.GIT_PORT
    with pytest.raises(ValueError, match="image"):
        up.upstream_manifests(LAYOUT, "")


def test_seed_pushes_workspace_commit_and_is_idempotent(tmp_path):
    workspace, remote = seeded(tmp_path)
    head = git(workspace, "rev-parse", "HEAD")
    assert up.seed_upstream(workspace, remote, LAYOUT) == head
    assert up.seed_upstream(workspace, remote, LAYOUT) == head
    assert git(tmp_path / "remote.git", "rev-parse", "refs/heads/main") == head


def test_seed_refuses_a_remote_on_another_commit(tmp_path):
    workspace, remote = seeded(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    prepare_workspace(other, "unused:v1", fresh=True, variant="raw")
    up.seed_upstream(other, remote, LAYOUT)
    with pytest.raises(up.UpstreamError, match="refusing to reseed"):
        up.seed_upstream(workspace, remote, LAYOUT)


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_commit_applies_to_every_variant_and_fast_forwards(tmp_path, variant):
    workspace, remote = seeded(tmp_path, variant)
    seed = up.seed_upstream(workspace, remote, LAYOUT)
    sha = up.replay_commit("benign", remote, LAYOUT)
    remote_dir = tmp_path / "remote.git"
    assert git(remote_dir, "rev-parse", "refs/heads/main") == sha
    assert git(remote_dir, "rev-parse", f"{sha}^") == seed
    assert git(remote_dir, "diff", "--name-only", seed, sha) == "main.go"
    assert "v2" in git(remote_dir, "show", f"{sha}:main.go")
    tip = up.replay_commit("broken", remote, LAYOUT)
    assert git(remote_dir, "rev-parse", f"{tip}^") == sha
    assert "not ready" in git(remote_dir, "show", f"{tip}:main.go")
    assert "v2" in git(remote_dir, "show", f"{tip}:main.go")


def test_commits_are_scoped_to_the_scenarios():
    assert up.available_commits() == ("benign", "broken")
    benign = up.patch_path("benign").read_text()
    broken = up.patch_path("broken").read_text()
    assert "+++ b/main.go" in benign
    assert "+++ b/main.go" in broken
    assert "503" in broken
    assert "503" not in benign
    for bad in ("nope", "../benign", "Benign"):
        with pytest.raises(up.UpstreamError):
            up.patch_path(bad)


def test_replay_that_does_not_apply_changes_nothing_upstream(tmp_path):
    workspace, remote = seeded(tmp_path)
    up.seed_upstream(workspace, remote, LAYOUT)
    after_benign = up.replay_commit("benign", remote, LAYOUT)
    patches = tmp_path / "patches"
    patches.mkdir()
    shutil.copy(up.patch_path("benign"), patches / "again.patch")  # already applied: the context no longer matches
    with pytest.raises(up.UpstreamError):
        up.replay_commit("again", remote, LAYOUT, directory=patches)
    assert git(tmp_path / "remote.git", "rev-parse", "refs/heads/main") == after_benign


@pytest.mark.go_integration
def test_patched_toy_source_builds_and_is_gofmt_clean(tmp_path):
    if shutil.which("go") is None:
        pytest.skip("go is not installed")
    workspace, remote = seeded(tmp_path)
    up.seed_upstream(workspace, remote, LAYOUT)
    up.replay_commit("benign", remote, LAYOUT)
    up.replay_commit("broken", remote, LAYOUT)
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", remote, str(clone)], check=True)
    built = subprocess.run(
        ["go", "build", "-o", str(tmp_path / "toy"), "main.go"], cwd=clone, capture_output=True, text=True
    )
    assert built.returncode == 0, built.stderr
    assert subprocess.run(["gofmt", "-l", "."], cwd=clone, capture_output=True, text=True).stdout == ""


def test_replay_against_a_missing_ref_fails(tmp_path):
    with pytest.raises(up.UpstreamError, match="clone"):
        up.replay_commit("benign", bare_remote(tmp_path), LAYOUT)


@pytest.mark.skipif(shutil.which("git") is None, reason="git is required")
def test_the_manifest_daemon_script_serves_seed_and_push_over_git_protocol(tmp_path):
    """Run the Deployment's own shell (base path swapped for a temp dir) and talk to it as SDO and the replayer do."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = tmp_path / "srv"
    base.mkdir()
    script = up.daemon_script(str(base), port=port).replace("--listen=0.0.0.0", "--listen=127.0.0.1")
    script = script.replace("git daemon ", "git daemon --verbose ")
    process = subprocess.Popen(["bash", "-c", script], stderr=subprocess.PIPE, text=True, env={**os.environ})
    try:
        assert process.stderr is not None
        for line in process.stderr:  # the daemon announces readiness once it is listening
            if "Ready to rumble" in line:
                break
        else:
            pytest.fail("git daemon exited before it was ready")
        url = f"git://127.0.0.1:{port}/{up.REPOSITORY}"
        workspace, _ = seeded(tmp_path)
        head = up.seed_upstream(workspace, url, LAYOUT)
        sha = up.replay_commit("benign", url, LAYOUT)
        assert git(base / up.REPOSITORY, "rev-parse", "refs/heads/main") == sha != head
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_cli_push_and_source_args(tmp_path, capsys):
    workspace, remote = seeded(tmp_path)
    up.seed_upstream(workspace, remote, LAYOUT)
    parser = build_parser()
    args = parser.parse_args(["toy", "push", "--commit", "benign", "--remote", remote, "--lane-namespace", "sdo-toy"])
    assert args.handler(args) == 0
    pushed = json.loads(capsys.readouterr().out)
    assert pushed["pushed"] == git(tmp_path / "remote.git", "rev-parse", "main")
    args = parser.parse_args(["toy", "source-args", "--lane-namespace", "sdo-toy"])
    assert args.handler(args) == 0
    assert json.loads(capsys.readouterr().out)[:2] == ["--source-kind", "git"]
    with pytest.raises(SystemExit):
        parser.parse_args(["toy", "push", "--commit", "unknown", "--remote", remote])
