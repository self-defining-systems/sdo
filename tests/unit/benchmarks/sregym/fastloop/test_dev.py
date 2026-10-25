"""Dev-mount builds publish complete binaries safely across overlapping lanes."""

import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from benchmarks.sregym.fastloop import dev


@pytest.fixture
def worktree(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    (root / "sdo/operator").mkdir(parents=True)
    (root / "sdo/operator/go.mod").write_text("module fixture\n")
    (root / ".fastloop").mkdir()
    binary = root / ".fastloop/sdo-operator"
    binary.write_bytes(b"previous binary")
    binary.chmod(0o755)
    return root


def test_overlapping_builds_publish_complete_binaries(worktree: Path, tmp_path: Path, monkeypatch):
    writing = Barrier(2, timeout=10)
    compiled = Barrier(2, timeout=10)
    payloads = [b"first-build\n" * 4096, b"second-build\n" * 4096]
    destinations = []

    def build(args: list[str], *, cwd: Path, check: bool) -> subprocess.CompletedProcess:
        assert args[:4] == ["go", "build", "-buildvcs=false", "-o"]
        assert args[5:] == ["./cmd/sdo-operator"]
        assert cwd == worktree / "sdo/operator"
        assert check
        destination = Path(args[4])
        assert destination.parent == worktree / ".fastloop"
        destinations.append(destination)
        payload = payloads[compiled.wait()]
        with destination.open("wb") as binary:
            binary.write(payload[: len(payload) // 2])
            binary.flush()
            writing.wait()
            binary.write(payload[len(payload) // 2 :])
        destination.chmod(0o755)
        # Both compilers have finished writing before either build can publish.
        compiled.wait()
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(dev.subprocess, "run", build)
    run_dirs = [tmp_path / "lane-one", tmp_path / "lane-two"]
    for directory in run_dirs:
        directory.mkdir()
    output = worktree / ".fastloop/sdo-operator"
    with output.open("rb") as running_binary, ThreadPoolExecutor(max_workers=2) as pool:
        builds = [pool.submit(dev.prepare_dev_mount, worktree, None, directory) for directory in run_dirs]
        configs = [future.result(timeout=15) for future in builds]
        # A process holding the old inode must retain the old complete contents.
        assert running_binary.read() == b"previous binary"
    assert configs == [directory / "kind-dev.yaml" for directory in run_dirs]
    assert all(config.is_file() for config in configs)
    assert len(set(destinations)) == 2
    assert output.read_bytes() in payloads
    assert output.stat().st_mode & 0o777 == 0o755
    assert list(output.parent.iterdir()) == [output]


def test_failed_build_cleans_partial_output_and_retains_previous_binary(worktree: Path, tmp_path: Path, monkeypatch):
    def fail_build(args: list[str], **kwargs) -> None:
        Path(args[4]).write_bytes(b"partial failed build")
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(dev.subprocess, "run", fail_build)
    with pytest.raises(subprocess.CalledProcessError):
        dev.prepare_dev_mount(worktree, None, tmp_path)
    output = worktree / ".fastloop/sdo-operator"
    assert output.read_bytes() == b"previous binary"
    assert output.stat().st_mode & 0o777 == 0o755
    assert list(output.parent.iterdir()) == [output]
    assert not (tmp_path / "kind-dev.yaml").exists()


def test_retarget_copies_into_node_storage_before_covering_old_host_mount(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop.dev import retarget_dev_mount

    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="lane-control-plane\nlane-worker\n", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.dev._copy_dev_snapshot",
        lambda worktree, node, target: calls.append(["snapshot", str(worktree), node, target]),
    )
    retarget_dev_mount("lane", tmp_path)
    mounts = [call for call in calls if "--bind" in call]
    assert len(mounts) == 2
    assert all(call[-1] == "/opt/sdo-dev" for call in mounts)
    for mount in mounts:
        node = mount[2]
        assert next(i for i, call in enumerate(calls) if call[:3] == ["snapshot", str(tmp_path), node]) < calls.index(
            mount
        )
