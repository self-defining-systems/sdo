"""Run-branch shared memory: endpoint config, branch seeding, lane adoption and run close."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.remote_memory import (
    SSH_DIR,
    SSH_SECRET,
    MemoryEndpoint,
    adopt_run_branch,
    close_run_branch,
    controller_job_with_upstream,
    endpoint_for,
    ensure_run_branch,
    load_memory_endpoints,
    remote_head,
    run_branch,
    write_memory_endpoints,
)

ROOT = Path(__file__).resolve().parents[5]
ENDPOINT = MemoryEndpoint(repo="example-org/toy-sdo", branch="live/20261028-synthetic")


def git(repository: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repository), "-c", "user.name=Test", "-c", "user.email=test@invalid", *args], text=True
    ).strip()


def accepted(tmp_path: Path, name: str, files: dict[str, str]) -> Path:
    root = tmp_path / name
    root.mkdir()
    git(root, "init", "--quiet")
    for path, body in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(body)
    git(root, "add", ".")
    git(root, "commit", "--quiet", "-m", f"{name} seed")
    return root


def bare(tmp_path: Path) -> Path:
    remote = tmp_path / "memory.git"
    subprocess.run(["git", "init", "--quiet", "--bare", str(remote)], check=True)
    return remote


@pytest.mark.parametrize(
    ("repo", "branch"),
    [
        ("example-org/toy-sdo", "main"),
        ("example-org/toy-sdo", "master"),
        ("example-org/toy-sdo", ""),
        ("example-org/toy-sdo", "live/bad..name"),
        ("toy-sdo", "live/x"),
        ("example-org/toy-sdo.git", "live/x"),
        ("git@github.com:example-org/toy-sdo", "live/x"),
    ],
)
def test_endpoint_refuses_default_or_malformed_targets(repo: str, branch: str) -> None:
    with pytest.raises(ValueError, match="eval: memory endpoint"):
        MemoryEndpoint(repo=repo, branch=branch)


def test_endpoints_file_round_trips_and_requires_repo_and_branch_per_app(tmp_path: Path) -> None:
    path = write_memory_endpoints(tmp_path / "memory-endpoints.json", {"hotel-reservation": ENDPOINT})
    assert json.loads(path.read_text()) == {"hotel-reservation": {"repo": ENDPOINT.repo, "branch": ENDPOINT.branch}}
    assert load_memory_endpoints(path) == {"hotel-reservation": ENDPOINT}
    assert ENDPOINT.url == "git@github.com:example-org/toy-sdo.git"
    with pytest.raises(ValueError, match="every run must set one per app"):
        endpoint_for(load_memory_endpoints(path), "social-network")
    for broken in ({}, {"hotel-reservation": {"repo": ENDPOINT.repo}}, {"a": {**vars(ENDPOINT), "broker_url": "x"}}):
        path.write_text(json.dumps(broken))
        with pytest.raises(ValueError, match="eval:"):
            load_memory_endpoints(path)


def test_run_branch_is_one_dated_branch_per_run() -> None:
    assert run_branch("Mixed10 S42", datetime(2026, 10, 28, tzinfo=UTC)) == "live/20261028-mixed10-s42"
    with pytest.raises(ValueError, match="run label"):
        run_branch("--")


def test_lanes_seed_then_adopt_one_branch_history_without_losing_memory(tmp_path: Path) -> None:
    remote = bare(tmp_path)
    files = {"app.txt": "app\n", ".sdo/goal.md": "goal\n"}
    first = accepted(tmp_path, "lane-a", files)
    second = accepted(tmp_path, "lane-b", files)
    assert remote_head(ENDPOINT, str(remote)) == ""
    head = ensure_run_branch(ENDPOINT, first, str(remote))
    assert ensure_run_branch(ENDPOINT, second, str(remote)) == head
    assert git(second, "rev-parse", "HEAD") != head
    assert adopt_run_branch(second, ENDPOINT, str(remote)) == head
    assert git(second, "rev-parse", "HEAD") == head
    assert git(second, "status", "--porcelain") == ""
    assert git(second, "remote") == ""

    private = accepted(tmp_path, "lane-c", {**files, ".sdo/playbooks/private/README.md": "private\n"})
    with pytest.raises(ValueError, match="matches no commit of the run branch"):
        adopt_run_branch(private, ENDPOINT, str(remote))


def test_controller_job_gets_branch_and_read_only_key_like_the_go_installer() -> None:
    job = {
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "repository"}],
                    "containers": [{"name": "controller", "args": ["controller", "--app", "/workspace/application"]}],
                }
            }
        }
    }
    once = controller_job_with_upstream(json.loads(json.dumps(job)), ENDPOINT)
    twice = controller_job_with_upstream(json.loads(json.dumps(once)), ENDPOINT)
    assert once == twice
    container = once["spec"]["template"]["spec"]["containers"][0]
    assert container["args"][-6:] == [
        *("--memory-upstream-url", ENDPOINT.url, "--memory-upstream-branch", ENDPOINT.branch),
        *("--memory-upstream-ssh-dir", SSH_DIR),
    ]
    assert {"name": "memory-upstream-ssh", "mountPath": SSH_DIR, "readOnly": True} in container["volumeMounts"]
    assert {"name": "memory-upstream-ssh", "secret": {"secretName": SSH_SECRET, "defaultMode": 0o444}} in once["spec"][
        "template"
    ]["spec"]["volumes"]
    installer = (ROOT / "sdo/operator/install/resources.go").read_text()
    assert f'MemoryUpstreamSSHDir = "{SSH_DIR}"' in installer
    for flag in ("--memory-upstream-url", "--memory-upstream-branch", "--memory-upstream-ssh-dir"):
        assert f'"{flag.removeprefix("--")}"' in (ROOT / "sdo/operator/operatorcli/controller.go").read_text()


def test_close_tags_the_branch_and_indexes_it_on_main_once(tmp_path: Path) -> None:
    remote = bare(tmp_path)
    index = accepted(tmp_path, "index", {"README.md": "# toy-sdo\n\n| branch | run |\n|---|---|\n"})
    git(index, "push", "--quiet", str(remote), "HEAD:refs/heads/main")
    lane = accepted(tmp_path, "lane", {"app.txt": "app\n"})
    head = ensure_run_branch(ENDPOINT, lane, str(remote))
    row = f"| {ENDPOINT.branch} | synthetic |"
    tag = close_run_branch(ENDPOINT, row, message="synthetic run", url=str(remote))
    assert git(remote, "rev-parse", f"{tag}^{{commit}}") == head
    readme = git(remote, "show", "main:README.md")
    assert readme.count(row) == 1
    assert git(remote, "rev-parse", f"refs/heads/{ENDPOINT.branch}") == head


def test_host_mirror_follows_the_branch_and_carries_the_shared_identity(tmp_path: Path) -> None:
    from benchmarks.sregym.adapter.native_operator import refresh_memory_mirror
    from benchmarks.sregym.fastloop.parallel_memory import memory_head
    from benchmarks.sregym.fastloop.remote_memory import create_memory_mirror, shared_identity

    remote = bare(tmp_path)
    lane = accepted(tmp_path, "lane", {"app.txt": "app\n"})
    head = ensure_run_branch(ENDPOINT, lane, str(remote))
    mirror = create_memory_mirror(tmp_path / "mirror", ENDPOINT, str(remote))
    identity = (mirror / ".git" / "sdo-shared-id").read_text().strip()
    assert identity == shared_identity(ENDPOINT) == shared_identity(MemoryEndpoint(ENDPOINT.repo, ENDPOINT.branch))
    assert identity != shared_identity(MemoryEndpoint(ENDPOINT.repo, "live/20261028-other"))
    assert memory_head(mirror) == head

    (lane / "learned.txt").write_text("peer learning\n")
    git(lane, "add", ".")
    git(lane, "commit", "--quiet", "-m", "peer publication")
    git(lane, "push", "--quiet", str(remote), f"HEAD:refs/heads/{ENDPOINT.branch}")
    assert memory_head(mirror) == git(lane, "rev-parse", "HEAD")
    assert (mirror / "learned.txt").read_text() == "peer learning\n"

    plain = accepted(tmp_path, "plain", {"app.txt": "app\n"})
    before = git(plain, "rev-parse", "HEAD")
    refresh_memory_mirror(plain)
    assert git(plain, "rev-parse", "HEAD") == before
