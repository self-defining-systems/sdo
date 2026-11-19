"""``swap_compiled``: the order of a swap's steps, when it restarts the controller, and what a failed step leaves."""

from __future__ import annotations

import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.devswap import binary, cli, controller, devtree, swap, tree
from benchmarks.devswap.kube import SwapError

if TYPE_CHECKING:
    from collections.abc import Iterator

HEAD = "a" * 40


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
    for rel in ("sdo/operator/go.mod", "sdo/operator/a.go", "sdo/operator/Dockerfile.runtime", "uv.lock"):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n")
    _git(tmp_path, "init", "-q", str(root))
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    return root


class Lane:
    """A fake lane: records every step, holds one controller pod that a restart replaces, and a binary on its mount."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        self.calls: list[str] = []
        self.pods = {"pod-1"}
        self.suspended = False
        self.warm: Any = None
        self.state = {"head": HEAD, "status": "", "outcomes": ""}
        self.tmp_path = tmp_path
        self.mount = tmp_path / "mount-sdo-operator"
        self.mount.write_text("old binary")
        self.persisted: list[swap.SwapState] = []
        monkeypatch.setattr(swap, "retire_standby", lambda *_: self.calls.append("retire"))
        monkeypatch.setattr(swap, "wait_standby", lambda *_: self.calls.append("standby"))
        monkeypatch.setattr(swap, "repository_state", lambda *_: dict(self.state))
        monkeypatch.setattr(swap, "controller_pod_ids", lambda *_: set(self.pods))
        monkeypatch.setattr(swap, "controller_stopped", self.stopped)
        monkeypatch.setattr(swap, "cached_operator_binary", lambda *a, **k: self.built())
        monkeypatch.setattr(swap, "start_go_cache_warm", self.start_warm)

    @contextmanager
    def stopped(self, _kube: object, _control: str) -> Iterator[None]:
        if self.warm is not None:
            self.warm.finish()  # what the real devswap.controller.set_controller_suspended does before any stop
        self.calls.append("stop")
        self.suspended = True
        try:
            yield
        finally:
            self.suspended = False
            self.calls.append("start")
            self.pods = {f"pod-{len(self.calls)}"}

    def start_warm(self, _kube: object, _control: str, _out: object) -> object:
        lane = self

        class Warm:
            waited = 0.0
            running = True

            def finish(self) -> None:
                if self.running:
                    self.running = False
                    lane.warm = None
                    lane.calls.append("warm finish")

            def abort(self) -> None:
                if self.running:
                    self.running = False
                    lane.warm = None
                    lane.calls.append("warm abort")

        self.calls.append("warm start")
        self.warm = Warm()
        return self.warm

    def built(self) -> Path:
        self.calls.append("build")
        path = self.tmp_path / "built-sdo-operator"
        path.write_text("new binary")
        return path

    def place(self, new: Path | None) -> None:
        self.calls.append("place binary" if new else "place")
        if new is not None:
            self.mount.write_text(new.read_text())

    def plan(self, repo: Path, lane_compiled: dict[str, str], **extra: Any) -> swap.SwapPlan:
        values: dict[str, Any] = {
            "kube": None,
            "control": "ns",
            "checkout": repo,
            "lane_compiled": lane_compiled,
            "lane_contracts": devtree.contracts_snapshot(repo) if lane_compiled else "",
            "prepared": dict(self.state),
            "place": self.place,
            "idle": lambda: "",
            "wait_idle": lambda: "",
            "work_root": self.tmp_path,
            "current_binary": self.mount,
            "persist": self.persisted.append,
        }
        return swap.SwapPlan(**{**values, **extra})


def _go_change(repo: Path) -> None:
    (repo / "sdo/operator/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "go")


def test_a_python_only_swap_places_the_tree_and_restarts_nothing(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    result = swap.swap_compiled(lane.plan(repo, devtree.fingerprint(repo)), lambda _m: None)
    assert lane.calls == ["place", "retire", "standby"]
    assert not result.binary
    assert not result.restarted


def test_a_changed_operator_is_built_placed_and_restarted_once_at_the_end(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)
    result = swap.swap_compiled(lane.plan(repo, compiled), lambda _m: None)
    # the controller is stopped, the standby retired while it is stopped, and only then started
    # the new tree's Go build cache is warmed while the old controller serves, joined before anything stops it
    assert lane.calls == [
        "build",
        "place binary",
        "warm start",
        "retire",
        "standby",
        "warm finish",
        "stop",
        "retire",
        "start",
        "standby",
    ]
    assert result.binary
    assert result.restarted
    assert result.compiled == devtree.fingerprint(repo)
    assert lane.persisted[-1].compiled == devtree.fingerprint(repo)


def test_the_go_cache_is_warmed_before_a_seed_or_tree_preparation_stops_the_controller(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def seed(_timeline: Any) -> str:
        with lane.stopped(None, "ns"):
            pass
        return "e" * 40

    swap.swap_compiled(lane.plan(repo, compiled, seed=seed), lambda _m: None)
    assert lane.calls.index("warm finish") < lane.calls.index("stop")
    assert lane.calls.count("warm start") == lane.calls.count("warm finish") == 1


def test_no_warm_without_a_new_binary_or_when_switched_off(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    swap.swap_compiled(lane.plan(repo, devtree.fingerprint(repo)), lambda _m: None)  # nothing compiled changed
    assert not [c for c in lane.calls if c.startswith("warm")]
    compiled = devtree.fingerprint(repo)
    _go_change(repo)
    swap.swap_compiled(lane.plan(repo, compiled, warm_go_cache=False), lambda _m: None)
    assert not [c for c in lane.calls if c.startswith("warm")]


def test_a_failed_swap_removes_the_warm_pod(repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def boom() -> None:
        raise SwapError("golden copy differs")

    with pytest.raises(SwapError, match="golden copy differs"):
        swap.swap_compiled(lane.plan(repo, compiled, verify=boom), lambda _m: None)
    assert "warm abort" in lane.calls
    assert "warm finish" not in lane.calls


def test_a_later_step_that_restarts_the_controller_carries_the_binary(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def seed() -> None:
        with lane.stopped(None, "ns"):  # a seed import stops the controller and starts it on the mount's binary
            pass

    result = swap.swap_compiled(lane.plan(repo, compiled, finish=seed), lambda _m: None)
    assert lane.calls.count("start") == 1
    assert result.binary
    assert not result.restarted


def test_an_image_change_is_refused_before_anything_is_placed(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    (repo / "sdo/operator/Dockerfile.runtime").write_text("y\n")
    _git(repo, "commit", "-qam", "image")
    with pytest.raises(SwapError, match="Dockerfile.runtime"):
        swap.swap_compiled(lane.plan(repo, compiled), lambda _m: None)
    assert lane.calls == []


def test_a_lane_that_moved_is_refused_unless_the_caller_restores_it(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    plan = lane.plan(repo, devtree.fingerprint(repo))
    lane.state = {**lane.state, "head": "c" * 40}  # the live head differs from the prepared one
    with pytest.raises(SwapError, match="not the one prepared"):
        swap.swap_compiled(plan, lambda _m: None)


def test_the_accepted_tree_is_regenerated_by_the_placed_binary_after_the_lane_is_asserted(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)
    seen: list[Any] = []

    def regenerate(*args: Any) -> str:
        seen.append(args[-1])  # the binary the preparer is to run
        return "d" * 40

    monkeypatch.setattr(swap, "regenerate_tree", regenerate)
    result = swap.swap_compiled(
        lane.plan(repo, compiled, prepare_tree=lambda _clone, _binary: "unused"), lambda _m: None
    )
    assert seen == [tmp_path / "built-sdo-operator"]
    assert result.head == "d" * 40
    assert lane.persisted[-1].head == "d" * 40


def test_a_failure_after_the_binary_was_placed_puts_the_previous_binary_back_and_records_it(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)
    placed: list[str] = []

    def boom() -> None:
        raise SwapError("golden copy differs")

    with pytest.raises(SwapError, match="golden copy differs"):
        swap.swap_compiled(
            lane.plan(repo, compiled, verify=boom, on_binary=lambda path: placed.append(path.read_text())),
            lambda _m: None,
        )
    assert lane.mount.read_text() == "old binary"  # the mount agrees with the controller that still runs
    assert placed == ["new binary", "old binary"]  # the runner's own copy followed both ways
    assert "stop" not in lane.calls  # the controller never ran the new binary: nothing to restart
    assert lane.persisted[-1].compiled == compiled  # the record says the old build


def test_a_failure_after_the_controller_restarted_on_the_new_binary_restarts_it_on_the_old_one(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def seed_then_fail() -> None:
        with lane.stopped(None, "ns"):  # the seed import restarted the controller on the new binary
            pass
        raise SwapError("seed memory differs")

    with pytest.raises(SwapError, match="seed memory differs"):
        swap.swap_compiled(lane.plan(repo, compiled, finish=seed_then_fail), lambda _m: None)
    assert lane.mount.read_text() == "old binary"
    assert lane.calls.count("start") == 2  # the seed's start and the restart that puts the old binary under it
    assert not lane.suspended
    assert lane.persisted[-1].compiled == compiled


def test_a_failure_after_the_memory_head_moved_records_the_actual_head(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)
    monkeypatch.setattr(swap, "regenerate_tree", lambda *a: "d" * 40)

    def boom() -> None:
        raise SwapError("seed import failed")

    with pytest.raises(SwapError):
        swap.swap_compiled(lane.plan(repo, compiled, prepare_tree=lambda _c, _b: "x", finish=boom), lambda _m: None)
    assert lane.persisted[-1].head == "d" * 40
    assert lane.persisted[-1].compiled == compiled


def test_a_failed_swap_without_a_known_previous_binary_records_what_the_mount_holds(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def boom() -> None:
        raise SwapError("verify failed")

    with pytest.raises(SwapError):
        swap.swap_compiled(lane.plan(repo, compiled, verify=boom, current_binary=None), lambda _m: None)
    assert lane.mount.read_text() == "new binary"
    assert lane.persisted[-1].compiled == devtree.fingerprint(repo)


def test_a_controller_that_cannot_be_read_stops_the_swap_before_anything_is_placed(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = Lane(monkeypatch, tmp_path)
    compiled = devtree.fingerprint(repo)
    _go_change(repo)

    def unreadable(*_a: object) -> set[str]:
        raise SwapError("could not read the controller pods in ns")

    monkeypatch.setattr(swap, "controller_pod_ids", unreadable)
    with pytest.raises(SwapError, match="could not read"):
        swap.swap_compiled(lane.plan(repo, compiled), lambda _m: None)
    assert lane.mount.read_text() == "old binary"


def test_the_kept_install_command_is_rerun_as_prepare_tree_on_the_clone() -> None:
    kept = ["/old/sdo-operator", "install-existing", "--repository", "/r", "--namespace", "app"]
    assert tree.prepare_tree_argv(kept, Path("/clone"), Path("/new/sdo-operator")) == [
        "/new/sdo-operator",
        "prepare-tree",
        "--repository",
        "/clone",
        "--namespace",
        "app",
    ]
    with pytest.raises(SwapError):
        tree.prepare_tree_argv(["x", "install", "--repository", "/r"], Path("/c"))


def test_prepare_tree_runs_the_binary_the_swap_placed_not_the_installs_own(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[list[str]] = []

    def fake_run(args: list[str], *_a: object, **_k: object) -> Any:
        ran.append(args)
        return subprocess.CompletedProcess(args, 0, stdout='{"head": "h"}\n', stderr="")

    monkeypatch.setattr(tree, "run", fake_run)
    prepare = tree.prepare_tree_from_install(["/old/op", "install-existing", "--repository", "/r"], {})
    assert prepare(Path("/clone"), Path("/placed/op")) == "h"
    assert ran[0][0] == "/placed/op"
    assert prepare(Path("/clone"), None) == "h"  # no binary placed: the lane's own
    assert ran[1][0] == "/old/op"


def test_a_revision_is_checked_out_once_and_reused(repo: Path, tmp_path: Path) -> None:
    sha = _git(repo, "rev-parse", "HEAD")
    root = tmp_path / "checkouts"
    first = cli.resolve_checkout(repo, sha, root)
    assert (first / "sdo/operator/go.mod").is_file()
    assert cli.resolve_checkout(repo, sha[:10], root) == first


def test_a_failed_cli_swap_resumes_the_controller(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    resumed: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        cli, "set_controller_suspended", lambda _k, control, suspended: resumed.append((control, suspended))
    )

    def fail(_args: object) -> dict[str, object]:
        raise SwapError("lane differs")

    monkeypatch.setattr(cli, "swap", fail)
    code = cli.main(
        ["swap", "--control-namespace", "ns", "--lane", str(tmp_path / "lane.json"), "--dev-dir", str(tmp_path)]
    )
    assert code == 1
    assert resumed == [("ns", False)]


def test_the_core_names_no_benchmark() -> None:
    from benchmarks.devswap import contractdiff, kube

    for module in (controller, swap, tree, binary, cli, devtree, kube, contractdiff):
        source = Path(module.__file__).read_text()  # type: ignore[arg-type]
        assert "benchmarks.fuzzops" not in source
        assert "benchmarks.sregym" not in source
