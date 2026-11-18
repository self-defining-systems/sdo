"""A seed memory imported into a lane: prepared on the host first, then one controller stop, import and start."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

from benchmarks.devswap import controller, devtree, swap
from benchmarks.devswap.kube import SwapError
from benchmarks.devswap.seed import MemorySeed, PreparedMemory, SeedSteps, apply_seed, prepare_memory

COMMIT = "a" * 40
DATES = {"GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z"}
IDENTITY = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "safe.directory=*", "-C", str(repo), *args],
        env={**os.environ, **IDENTITY, **DATES},
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def make_repo(path: Path, files: dict[str, str]) -> str:
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "main")
    for name, text in files.items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)
    git(path, "add", "-A")
    git(path, "commit", "-qm", "memory")
    return git(path, "rev-parse", "HEAD")


class FakePreparer:
    """``prepare-tree``: adds the generated detector file (deterministically: same input, same commit)."""

    def __init__(self, content: str = "generated v1\n") -> None:
        self.calls = 0
        self.content = content

    def __call__(self, clone: Path, binary: Path | None) -> str:
        self.calls += 1
        generated = clone / ".sdo" / "diagnostics" / "generic.go"
        generated.parent.mkdir(parents=True, exist_ok=True)
        if not generated.exists() or generated.read_text() != self.content:
            generated.write_text(self.content)
            git(clone, "add", "-A")
            git(clone, "commit", "-qm", "sdo: prepare the generic detectors")
        return git(clone, "rev-parse", "HEAD")


@pytest.fixture
def seed_memory(tmp_path: Path) -> MemorySeed:
    source = tmp_path / "source"
    commit = make_repo(source, {".sdo/goal.md": "goal\n", ".sdo/playbooks/a.go": "package a\n"})
    bundle = tmp_path / "memory.bundle"
    git(source, "bundle", "create", "-q", str(bundle), "main")
    return MemorySeed(commit, bundle, {"agent": {"model": "m"}})


def steps(calls: list[str], prepared: PreparedMemory, head: str | None = None, status: str = "", idle: str = "") -> Any:
    return SeedSteps(
        prepare=lambda: calls.append("prepare") or prepared,
        reset_incidents=lambda: calls.append("incidents"),
        retire_standby=lambda: calls.append("retire"),
        stop_controller=lambda: calls.append("stop"),
        apply_config=lambda overlay: calls.append(f"config {sorted(overlay)}"),
        import_memory=lambda memory: calls.append(f"import {memory.head[:1]}"),
        start_controller=lambda: calls.append("start"),
        wait_standby=lambda: calls.append("standby"),
        repository_state=lambda: {"head": head or prepared.head, "status": status, "outcomes": ""},
        controller_idle=lambda: idle,
        is_ancestor=lambda _a, _d: False,
        settle=lambda head: calls.append(f"settle {head[:1]}") or head,
    )


def test_the_seed_is_prepared_before_the_lane_is_touched_and_the_controller_restarts_once(
    seed_memory: MemorySeed,
) -> None:
    calls: list[str] = []
    prepared = PreparedMemory("b" * 40, Path("prepared.bundle"))
    receipt = apply_seed(steps(calls, prepared), seed_memory, lambda _line: None)
    assert calls == ["prepare", "incidents", "stop", "retire", "config ['agent']", "import b", "start", "standby"]
    assert calls.count("stop") == calls.count("start") == 1
    assert receipt["seed"] == seed_memory.commit
    assert receipt["prepared"] == "b" * 40


def test_a_seed_that_cannot_be_prepared_leaves_the_lane_alone(seed_memory: MemorySeed) -> None:
    calls: list[str] = []
    failing = steps(calls, PreparedMemory(COMMIT, Path("x")))

    def broken() -> PreparedMemory:
        calls.append("prepare")
        raise SwapError("prepare-tree failed")

    with pytest.raises(SwapError, match="prepare-tree failed"):
        apply_seed(SeedSteps(**{**failing.__dict__, "prepare": broken}), seed_memory, lambda _line: None)
    assert calls == ["prepare"]  # no incident reset, no controller stop: nothing to undo


def test_an_import_that_fails_leaves_the_controller_running(seed_memory: MemorySeed) -> None:
    calls: list[str] = []
    failing = steps(calls, PreparedMemory(COMMIT, Path("x")))

    def broken(_memory: PreparedMemory) -> None:
        raise SwapError("restore pod failed")

    with pytest.raises(SwapError, match="restore pod failed"):
        apply_seed(SeedSteps(**{**failing.__dict__, "import_memory": broken}), seed_memory, lambda _line: None)
    assert calls == ["prepare", "incidents", "stop", "retire", "config ['agent']", "start"]


@pytest.mark.parametrize(
    ("head", "status", "idle", "message"),
    [
        ("c" * 40, "", "", "not bbbbbbbbbbbb"),
        ("b" * 40, " M x", "", "not bbbbbbbbbbbb"),
        ("b" * 40, "", "busy", "not idle"),
    ],
)
def test_the_lane_must_be_the_prepared_seed_and_idle(
    seed_memory: MemorySeed, head: str, status: str, idle: str, message: str
) -> None:
    prepared = PreparedMemory("b" * 40, Path("x"))
    with pytest.raises(SwapError, match=message):
        apply_seed(steps([], prepared, head, status, idle), seed_memory, lambda _line: None)


def test_the_controllers_startup_upkeep_on_top_of_the_prepared_seed_is_settled(seed_memory: MemorySeed) -> None:
    """A fresh build migrates the seed and then prepares it; here the migration runs after the preparation, so the
    head the controller holds is prepared once more (nothing happens when that changes nothing)."""
    calls: list[str] = []
    prepared = PreparedMemory("b" * 40, Path("x"))
    lane = steps(calls, prepared, "c" * 40)
    on_top = SeedSteps(**{**lane.__dict__, "is_ancestor": lambda a, d: (a, d) == ("b" * 40, "c" * 40)})
    assert apply_seed(on_top, seed_memory, lambda _line: None)["head"] == "c" * 40
    assert calls[-1] == "settle c"
    settled = SeedSteps(**{**on_top.__dict__, "settle": lambda head: "d" * 40})
    assert apply_seed(settled, seed_memory, lambda _line: None)["head"] == "d" * 40


def test_a_lane_that_started_on_the_prepared_head_is_not_settled(seed_memory: MemorySeed) -> None:
    calls: list[str] = []
    apply_seed(steps(calls, PreparedMemory("b" * 40, Path("x"))), seed_memory, lambda _line: None)
    assert not any(call.startswith("settle") for call in calls)


def test_a_seed_without_a_preparer_goes_in_as_it_is(tmp_path: Path, seed_memory: MemorySeed) -> None:
    assert prepare_memory(seed_memory, None, None, tmp_path) == PreparedMemory(seed_memory.commit, seed_memory.bundle)


def test_preparation_runs_on_a_clone_of_the_seed_bundle_and_bundles_the_prepared_head(
    tmp_path: Path, seed_memory: MemorySeed
) -> None:
    preparer = FakePreparer()
    prepared = prepare_memory(seed_memory, preparer, None, tmp_path, lambda _m: None)
    assert prepared.head != seed_memory.commit
    assert preparer.calls == 2  # a changed head is prepared again: the preparation must be stable
    check = tmp_path / "check"
    git(tmp_path, "init", "-q", str(check))
    git(check, "fetch", "-q", str(prepared.bundle), "HEAD")
    assert git(check, "rev-parse", "FETCH_HEAD") == prepared.head
    assert git(check, "log", "--format=%s", "FETCH_HEAD").splitlines()[1:] == [
        "memory"
    ]  # the seed's own history is kept
    assert not list(tmp_path.glob("sdo-prepare-seed-*"))  # scratch is removed


def test_a_seed_the_build_already_prepared_is_imported_as_it_is(tmp_path: Path, seed_memory: MemorySeed) -> None:
    preparer = FakePreparer()
    first = prepare_memory(seed_memory, preparer, None, tmp_path, lambda _m: None)
    again = MemorySeed(first.head, first.bundle)
    assert prepare_memory(again, preparer, None, tmp_path, lambda _m: None) == PreparedMemory(first.head, first.bundle)


def test_a_preparation_that_does_not_settle_is_refused(tmp_path: Path, seed_memory: MemorySeed) -> None:
    class Flapping(FakePreparer):
        def __call__(self, clone: Path, binary: Path | None) -> str:
            self.content = f"generated {self.calls}\n"
            return super().__call__(clone, binary)

    with pytest.raises(SwapError, match="not stable"):
        prepare_memory(seed_memory, Flapping(), None, tmp_path, lambda _m: None)


def lane_restore(lane: Path, memory: PreparedMemory, tmp: Path) -> None:
    """The restore pod's own shell, run on a local directory: what ``restore_repository`` does to the lane."""
    seed_dir = Path(tempfile.mkdtemp(prefix=f"seed-{lane.name}-", dir=tmp))
    (seed_dir / "memory.bundle").write_bytes(memory.bundle.read_bytes())
    (seed_dir / "memory.ready").touch()
    script = controller.restore_script(memory.head, True, root=str(lane), bundle_dir=str(seed_dir))
    subprocess.run(["sh", "-c", script], check=True, capture_output=True, env={**os.environ, **IDENTITY})


def lane_snapshot(lane: Path) -> dict[str, str]:
    """What a reader sees: the accepted head, that tree's files, uncommitted files and the outcome store."""
    return {
        "head": git(lane, "rev-parse", "HEAD"),
        "tree": git(lane, "ls-tree", "-r", "HEAD"),
        "status": git(lane, "status", "--porcelain"),
        "outcomes": "\n".join(sorted(p.name for p in (lane / ".sdo" / "outcomes").glob("*"))),
        "history": git(lane, "log", "--format=%H %s", "HEAD"),
    }


def test_the_seeded_lane_is_the_lane_a_fresh_seeded_build_makes(tmp_path: Path, seed_memory: MemorySeed) -> None:
    """A fresh build imports the seed, then prepares the accepted tree it finds (two imports, the controller restarted
    between). The swap prepares the seed on the host and imports once. Both must leave the same accepted head,
    generated files, uncommitted files and outcome store."""
    lane_files = {".sdo/goal.md": "lane goal\n", ".sdo/outcomes/old.json": "{}\n"}
    sequential, single = tmp_path / "sequential", tmp_path / "single"
    make_repo(sequential, lane_files)
    make_repo(single, lane_files)
    preparer = FakePreparer()

    # fresh build: import the seed as it is, then regenerate the lane's tree from what is on it
    lane_restore(sequential, PreparedMemory(seed_memory.commit, seed_memory.bundle), tmp_path)
    clone = tmp_path / "export"
    git(tmp_path, "clone", "-q", str(sequential), str(clone))
    git(clone, "checkout", "-q", "--detach", seed_memory.commit)
    prepared_head = preparer(clone, None)
    bundle = tmp_path / "sequential.bundle"
    git(clone, "bundle", "create", "-q", str(bundle), "HEAD")
    lane_restore(sequential, PreparedMemory(prepared_head, bundle), tmp_path)

    # the swap: prepare on the host, import once
    imports: list[PreparedMemory] = []
    lane = steps([], PreparedMemory("0" * 40, Path("x")))
    prepared = prepare_memory(seed_memory, preparer, None, tmp_path, lambda _m: None)

    def import_memory(memory: PreparedMemory) -> None:
        imports.append(memory)
        lane_restore(single, memory, tmp_path)

    def state() -> dict[str, str]:
        return {
            "head": git(single, "rev-parse", "HEAD"),
            "status": git(single, "status", "--porcelain"),
            "outcomes": "",
        }

    seeded = SeedSteps(
        **{**lane.__dict__, "prepare": lambda: prepared, "import_memory": import_memory, "repository_state": state}
    )
    apply_seed(seeded, seed_memory, lambda _line: None)
    assert len(imports) == 1
    assert lane_snapshot(single) == lane_snapshot(sequential)
    snapshot = lane_snapshot(single)
    assert snapshot["status"] == ""
    assert snapshot["outcomes"] == ""  # the lane's own outcomes went with the old memory
    assert ".sdo/diagnostics/generic.go" in snapshot["tree"]


class SwapLane:
    """The swap's fake lane (as in ``test_swap_sequence``), with a seed callback that restarts the controller once."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        self.calls: list[str] = []
        self.pods = {"pod-1"}
        self.state = {"head": "d" * 40, "status": "", "outcomes": ""}
        self.persisted: list[swap.SwapState] = []
        monkeypatch.setattr(swap, "retire_standby", lambda *_: self.calls.append("retire"))
        monkeypatch.setattr(swap, "wait_standby", lambda *_: self.calls.append("standby"))
        monkeypatch.setattr(swap, "repository_state", lambda *_: dict(self.state))
        monkeypatch.setattr(swap, "controller_pod_ids", lambda *_: set(self.pods))
        monkeypatch.setattr(swap, "regenerate_tree", lambda *_a: self.calls.append("tree") or "e" * 40)
        self.tmp_path = tmp_path

    def seed(self, timeline: swap.Timeline) -> str:
        with timeline.step("prepare"):
            self.calls.append("seed prepare")
        with timeline.step("import_memory"):
            self.calls.append("seed stop+start")
            self.pods = {"pod-2"}
        return "f" * 40

    def plan(self, repo: Path, **extra: Any) -> swap.SwapPlan:
        return swap.SwapPlan(
            kube=None,  # type: ignore[arg-type]
            control="ns",
            checkout=repo,
            lane_compiled=devtree.fingerprint(repo),
            lane_contracts="",
            prepared=dict(self.state),
            place=lambda _binary: self.calls.append("place"),
            idle=lambda: "",
            wait_idle=lambda: "",
            work_root=self.tmp_path,
            persist=self.persisted.append,
            **extra,
        )


@pytest.fixture
def operator_repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel in ("sdo/operator/go.mod", "sdo/operator/a.go", "uv.lock"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text("x\n")
    git(tmp_path, "init", "-q", str(root))
    git(root, "add", "-A")
    git(root, "commit", "-qm", "base")
    return root


def test_a_swap_with_a_seed_imports_it_once_instead_of_preparing_the_lanes_tree(
    operator_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = SwapLane(monkeypatch, tmp_path)
    result = swap.swap_compiled(
        lane.plan(operator_repo, prepare_tree=lambda _c, _b: "unused", seed=lane.seed), lambda _m: None
    )
    assert lane.calls == ["place", "retire", "standby", "seed prepare", "seed stop+start"]  # no "tree"
    assert result.seeded == "f" * 40
    assert result.head == "d" * 40  # the record keeps the lane's own head: a reset never returns to a seed
    assert all(state.head == "d" * 40 for state in lane.persisted)
    assert {"seed", "seed.prepare", "seed.import_memory"} <= set(result.phases.seconds)


def test_a_seed_that_fails_still_times_the_steps_that_ran(
    operator_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = SwapLane(monkeypatch, tmp_path)

    def broken(timeline: swap.Timeline) -> str:
        with timeline.step("prepare"):
            raise SwapError("prepare-tree failed")

    with pytest.raises(SwapError, match="prepare-tree failed"):
        swap.swap_compiled(lane.plan(operator_repo, seed=broken), lambda _m: None)
    assert lane.pods == {"pod-1"}  # the controller was never touched


def migrate(repo: Path, rewrite: bool) -> None:
    """The start-up upkeep: stamps the schema and (``rewrite``) rewrites the detector the preparation generated."""
    (repo / ".sdo" / "schema").write_text("10\n")
    generated = repo / ".sdo" / "diagnostics" / "generic.go"
    if rewrite and generated.exists():
        generated.write_text(generated.read_text().rstrip("\n") + " (migrated)\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "sdo(memory-v10-schema): validated initial")


@pytest.mark.parametrize("migration_rewrites_detectors", [False, True])
def test_a_migration_after_the_preparation_leaves_the_tree_a_fresh_build_makes(
    tmp_path: Path, migration_rewrites_detectors: bool
) -> None:
    """A fresh build migrates the old-schema seed (the controller's upkeep) and then prepares what it finds. The swap
    prepares first and the controller migrates after: when the migration rewrites what the preparation generated the
    head is not a prepared one, and ``settle`` must restore exactly the fresh build's tree."""
    source = tmp_path / "source"
    commit = make_repo(source, {".sdo/goal.md": "goal\n", ".sdo/schema": "9\n", ".sdo/diagnostics/generic.go": "old\n"})
    bundle = tmp_path / "memory.bundle"
    git(source, "bundle", "create", "-q", str(bundle), "main")
    memory = MemorySeed(commit, bundle)
    preparer = FakePreparer()

    # fresh build: import the seed, the controller migrates it, tree preparation regenerates what it finds
    fresh = tmp_path / "fresh"
    make_repo(fresh, {".sdo/goal.md": "lane\n"})
    lane_restore(fresh, PreparedMemory(commit, bundle), tmp_path)
    migrate(fresh, migration_rewrites_detectors)
    preparer(fresh, None)

    # the swap: prepared on the host, imported once, the controller migrates, then the head is settled
    swapped = tmp_path / "swapped"
    make_repo(swapped, {".sdo/goal.md": "lane\n"})
    prepared = prepare_memory(memory, preparer, None, tmp_path, lambda _m: None)
    heads: list[str] = []

    def import_memory(imported: PreparedMemory) -> None:
        lane_restore(swapped, imported, tmp_path)
        migrate(swapped, migration_rewrites_detectors)  # the controller's start-up upkeep

    def settle(head: str) -> str:
        heads.append(head)
        preparer(swapped, None)  # regenerate_tree: prepare the accepted tree, import what changed
        return git(swapped, "rev-parse", "HEAD")

    def state() -> dict[str, str]:
        return {
            "head": git(swapped, "rev-parse", "HEAD"),
            "status": git(swapped, "status", "--porcelain"),
            "outcomes": "",
        }

    lane = steps([], PreparedMemory("0" * 40, Path("x")))
    seeded = SeedSteps(
        **{
            **lane.__dict__,
            "prepare": lambda: prepared,
            "import_memory": import_memory,
            "repository_state": state,
            "is_ancestor": lambda ancestor, descendant: git(swapped, "merge-base", ancestor, descendant) == ancestor,
            "settle": settle,
        }
    )
    receipt = apply_seed(seeded, memory, lambda _line: None)
    assert len(heads) == 1  # the migration committed on top of the prepared seed
    assert receipt["head"] == git(swapped, "rev-parse", "HEAD")
    assert git(swapped, "ls-tree", "-r", "HEAD", "--", ".sdo") == git(fresh, "ls-tree", "-r", "HEAD", "--", ".sdo")
    assert git(swapped, "status", "--porcelain") == ""
    assert (swapped / ".sdo/diagnostics/generic.go").read_text() == "generated v1\n"
