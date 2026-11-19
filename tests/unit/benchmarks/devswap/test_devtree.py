"""The dev-mounted source tree of a fuzzops lane: what is projected, and what a swap may change."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from benchmarks.devswap import devtree


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel in (
        "sdo/agents/x.py",
        "sdo/operator/go.mod",
        "sdo/operator/a/a.go",
        "libs/l.py",
        "examples/e.txt",
        "benchmarks/__init__.py",
        "benchmarks/sregym/__init__.py",
        "benchmarks/sregym/fastloop/f.py",
        "benchmarks/fuzzops/hooks.py",
        "tests/t.py",
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n")
    _git(tmp_path, "init", "-q", str(root))
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    return root


def test_sync_projects_the_pod_sources_and_the_binary(repo: Path, tmp_path: Path) -> None:
    binary = tmp_path / "sdo-operator"
    binary.write_text("bin")
    dest = tmp_path / "dev"
    devtree.sync(repo, dest, binary)
    assert (dest / "sdo/agents/x.py").is_file()
    assert (dest / "libs/l.py").is_file()
    assert (dest / "benchmarks/sregym/fastloop/f.py").is_file()
    assert (dest / ".fastloop/sdo-operator").read_text() == "bin"
    assert not (dest / "tests").exists()
    assert not (dest / "benchmarks/fuzzops").exists()


def test_sync_removes_what_the_new_tree_dropped(repo: Path, tmp_path: Path) -> None:
    binary = tmp_path / "sdo-operator"
    binary.write_text("bin")
    dest = tmp_path / "dev"
    devtree.sync(repo, dest, binary)
    (repo / "sdo/agents/x.py").unlink()
    devtree.sync(repo, dest, binary)
    assert not (dest / "sdo/agents/x.py").exists()


def test_python_only_change_may_be_swapped(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/agents/x.py").write_text("y\n")
    _git(repo, "commit", "-qam", "py")
    assert devtree.swap_problem(repo, lane) == ""


def test_go_change_cannot_be_swapped(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/a/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "go")
    assert "sdo/operator" in devtree.swap_problem(repo, lane)


def test_go_change_can_be_swapped_with_a_rebuilt_binary(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/a/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "go")
    assert devtree.binary_changed(repo, lane)
    assert devtree.swap_problem(repo, lane, rebuild_binary=True) == ""
    assert not devtree.binary_changed(repo, devtree.fingerprint(repo))


def test_other_compiled_changes_still_refuse_a_binary_swap(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "scripts").mkdir()
    (repo / "scripts/sdo-images.hcl").write_text("y\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "image")
    assert "sdo-images.hcl" in devtree.swap_problem(repo, lane, rebuild_binary=True)


def test_operator_binary_cache_key_follows_the_operator_source(repo: Path) -> None:
    from benchmarks.devswap import binary

    before = binary.operator_cache_key(repo)
    (repo / "sdo/agents/x.py").write_text("changed\n")
    _git(repo, "commit", "-qam", "py")
    assert binary.operator_cache_key(repo) == before
    (repo / "sdo/operator/a/a.go").write_text("changed\n")
    _git(repo, "commit", "-qam", "go")
    assert binary.operator_cache_key(repo) != before


def test_a_lane_without_a_fingerprint_cannot_be_swapped(repo: Path) -> None:
    assert devtree.swap_problem(repo, {})


def test_wipe_problems_name_every_way_the_lane_left_the_prepared_state() -> None:
    from benchmarks.devswap import controller

    before = {"head": "a" * 40, "status": "", "outcomes": ""}
    assert controller.wipe_problems(before, dict(before)) == []
    found = controller.wipe_problems(before, {"head": "b" * 40, "status": " M .sdo/x", "outcomes": "outcome.json"})
    assert len(found) == 3


def test_the_operator_binary_is_built_once_per_source_tree(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.devswap import binary

    built: list[Path] = []

    def build(checkout: Path, out: Path) -> Path:
        built.append(out)
        out.write_text("binary")
        return out

    monkeypatch.setattr(binary, "build_operator", build)
    cache = tmp_path / "cache"
    first = binary.cached_operator_binary(repo, lambda _m: None, cache)
    again = binary.cached_operator_binary(repo, lambda _m: None, cache)
    assert first == again
    assert len(built) == 1
    (repo / "sdo/operator/a/a.go").write_text("changed\n")
    _git(repo, "commit", "-qam", "go")
    assert binary.cached_operator_binary(repo, lambda _m: None, cache) != first
    assert len(built) == 2


def test_a_checkout_with_uncommitted_operator_changes_is_not_built_into_the_cache(repo: Path, tmp_path: Path) -> None:
    from benchmarks.devswap import binary
    from benchmarks.devswap.kube import SwapError

    clean = binary.operator_cache_key(repo)
    (repo / "sdo/operator/a/a.go").write_text("uncommitted\n")
    with pytest.raises(SwapError, match="uncommitted changes"):
        binary.operator_cache_key(repo)
    dirty = binary.operator_cache_key(repo, allow_dirty=True)
    assert dirty != clean  # a dirty build never shares the committed tree's entry
    (repo / "sdo/operator/a/a.go").write_text("uncommitted, differently\n")
    assert binary.operator_cache_key(repo, allow_dirty=True) != dirty
    (repo / "sdo/operator/new_untracked.go").write_text("package x\n")
    with pytest.raises(SwapError, match="uncommitted changes"):
        binary.operator_cache_key(repo)


def test_the_operator_is_built_static_without_a_vcs_stamp(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.devswap import binary

    seen: dict[str, object] = {}

    def run(args: list[str], what: str, timeout: float, **kwargs: object) -> None:
        seen.update(args=args, env=kwargs["env"])
        Path(args[args.index("-o") + 1]).write_text("binary")

    monkeypatch.setattr(binary, "run", run)
    binary.build_operator(repo, tmp_path / "out" / "sdo-operator")
    assert "-buildvcs=false" in seen["args"]  # type: ignore[operator]
    assert seen["env"]["CGO_ENABLED"] == "0"  # type: ignore[index]


def test_claims_that_need_the_same_tree_at_once_build_it_once(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first claim holds the build lock inside its build; the others start only then and finish after it."""
    import threading

    from benchmarks.devswap import binary

    building = threading.Event()
    release = threading.Event()
    builds: list[Path] = []

    def build(checkout: Path, out: Path) -> Path:
        builds.append(out)
        building.set()
        assert release.wait(timeout=30)
        out.write_text("binary")
        return out

    monkeypatch.setattr(binary, "build_operator", build)
    results: list[Path] = []
    cache = tmp_path / "c"

    def claim() -> None:
        results.append(binary.cached_operator_binary(repo, lambda _m: None, cache))

    first = threading.Thread(target=claim)
    first.start()
    assert building.wait(timeout=30)
    others = [threading.Thread(target=claim) for _ in range(3)]
    for thread in others:
        thread.start()
    release.set()
    for thread in [first, *others]:
        thread.join(timeout=30)
    assert len(builds) == 1
    assert len(results) == 4
    assert len(set(results)) == 1


def test_image_inputs_beside_the_operator_source_are_image_changes_not_binary_changes(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/model_cli_requirements.json").write_text("{}\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "model cli pin")
    why = devtree.swap_problem(repo, lane, rebuild_binary=True)
    assert "model_cli_requirements.json" in why
    assert "base images" in why


def test_a_lane_recorded_before_the_image_files_were_listed_is_not_refused_for_them(repo: Path) -> None:
    legacy = {k: v for k, v in devtree.fingerprint(repo).items() if k not in devtree.OPTIONAL}
    assert devtree.swap_problem(repo, legacy, rebuild_binary=True) == ""
