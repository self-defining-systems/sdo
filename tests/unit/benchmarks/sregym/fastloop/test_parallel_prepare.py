"""Shared-memory lane preparation runs lanes together and lets a later cohort join one accepted history."""

import json
import subprocess
import threading
import time
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import shared_memory
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan

# A pre-move cluster capture committed into seed memory; lanes never write their own now.
CAPTURE = ".sdo/diagnostics/detectors/health/service-reachability/baseline.json"


def _plan(tmp_path, names, run_id="proof"):
    return ParallelPlan(
        run_id=run_id,
        accepted_repository=tmp_path / "root" / "accepted",
        lanes={name: tmp_path / "lanes" / name for name in names},
        encounters=[Encounter(problem="alpha", lane=name) for name in names],
    )


def _environments(tmp_path, selected, shared_id=""):
    for name, run_dir in selected.lanes.items():
        FastloopEnvironment(
            run_dir=run_dir,
            cluster=f"kind-{name}",
            kubeconfig=run_dir / "config",
            namespace=f"app-{name}",
            application=f"app-{name}",
            workspace=(tmp_path / "root" / "accepted") if shared_id else run_dir / "application",
            sregym_dir=tmp_path / "sregym",
            private_tmp=run_dir / "tmp",
            parallel_root=tmp_path / "root",
            parallel_lane=name if shared_id else "",
            shared_accepted_id=shared_id,
        ).save()


def _no_cluster(monkeypatch):
    monkeypatch.setattr(shared_memory, "lane_nodes", lambda *_: ["node"])
    monkeypatch.setattr(shared_memory, "repository_volume_path", lambda *_: "/opt/sdo-parallel/lanes/x")


def test_lanes_prepare_concurrently_and_attach_waits_for_every_lane(monkeypatch, tmp_path):
    names = ["a", "b", "c", "d"]
    selected = _plan(tmp_path, names)
    _environments(tmp_path, selected)
    _no_cluster(monkeypatch)
    step = 0.4
    spans: dict[str, tuple[float, float]] = {}
    attached: list[tuple[float, list[str]]] = []

    def prepare(plan, name):
        assert plan == selected
        started = time.monotonic()
        time.sleep(step)
        spans[name] = (started, time.monotonic())

    def attach(plan, report):
        attached.append((time.monotonic(), sorted(plan.lanes)))

    monkeypatch.setattr(shared_memory, "_clone_accepted", lambda *_: None)
    started = time.monotonic()
    shared_memory.prepare_shared_lanes(selected, tmp_path / "attachment.json", prepare=prepare, attach=attach)
    wall = time.monotonic() - started

    assert sorted(spans) == names
    # Wall time follows the slowest lane, not the sum of all lanes.
    assert wall < step * 2 < step * len(names)
    assert max(start for start, _ in spans.values()) < min(end for _, end in spans.values())
    # One app-wide attach barrier, after every lane is prepared.
    assert len(attached) == 1
    assert attached[0][0] >= max(end for _, end in spans.values())
    assert attached[0][1] == names


def test_failed_lane_joins_its_peers_and_only_prepared_lanes_attach(monkeypatch, tmp_path):
    selected = _plan(tmp_path, ["a", "b", "c"])
    _environments(tmp_path, selected)
    _no_cluster(monkeypatch)
    finished: list[str] = []
    attached: list[list[str]] = []

    def prepare(_plan, name):
        if name == "b":
            raise RuntimeError("synthetic baseline failure")
        time.sleep(0.1)
        finished.append(name)

    monkeypatch.setattr(shared_memory, "_clone_accepted", lambda *_: None)
    with pytest.raises(RuntimeError, match="b.*synthetic baseline failure"):
        shared_memory.prepare_shared_lanes(
            selected,
            tmp_path / "attachment.json",
            prepare=prepare,
            attach=lambda plan, _report: attached.append(sorted(plan.lanes)),
        )
    assert sorted(finished) == ["a", "c"]
    # Prepared lanes are not wasted: they attach now, and "b" can join later as its own cohort.
    assert attached == [["a", "c"]]


def test_attached_lanes_are_not_prepared_again(monkeypatch, tmp_path):
    running = _plan(tmp_path, ["a"])
    _environments(tmp_path, running, shared_id="shared")
    joining = _plan(tmp_path, ["b", "c"])
    _environments(tmp_path, joining)
    selected = _plan(tmp_path, ["a", "b", "c"])
    _no_cluster(monkeypatch)
    prepared: list[str] = []
    attached: list[list[str]] = []
    monkeypatch.setattr(shared_memory, "_clone_accepted", lambda *_: pytest.fail("accepted memory already exists"))
    (tmp_path / "root" / "accepted").mkdir(parents=True)
    shared_memory.prepare_shared_lanes(
        selected,
        tmp_path / "attachment.json",
        prepare=lambda _plan, name: prepared.append(name),
        attach=lambda plan, _report: attached.append(sorted(plan.lanes)),
    )
    assert sorted(prepared) == ["b", "c"]
    assert attached == [["a", "b", "c"]]


def test_lane_prepare_process_runs_the_real_cli_for_one_lane_with_its_own_kubeconfig(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import cli

    selected = _plan(tmp_path, ["a", "b"])
    _environments(tmp_path, selected)
    launched = []

    def run(command, *, env, **kwargs):
        launched.append((command, env))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(shared_memory.subprocess, "run", run)
    shared_memory.prepare_lane_process(selected, "b")

    ((command, env),) = launched
    lane = FastloopEnvironment.load(selected.lanes["b"])
    assert env["KUBECONFIG"] == env["SREGYM_BASE_KUBECONFIG"] == str(lane.kubeconfig)
    assert int(env["GOMAXPROCS"]) == 4
    assert command[1:3] == ["-m", "benchmarks.sregym.fastloop"]
    # The real CLI consumes the produced command and hands the same plan and lane to the lane body.
    received = []
    monkeypatch.setattr(shared_memory, "prepare_lane", lambda plan, name: received.append((plan, name)))
    assert cli.main(command[3:]) == 0
    assert received == [(selected, "b")]


def test_lane_prepare_process_failure_names_the_lane_log(monkeypatch, tmp_path):
    selected = _plan(tmp_path, ["a"])
    _environments(tmp_path, selected)
    monkeypatch.setattr(shared_memory.subprocess, "run", lambda command, **_: subprocess.CompletedProcess(command, 3))
    with pytest.raises(RuntimeError, match=r"exited 3.*prepare\.log"):
        shared_memory.prepare_lane_process(selected, "a")


def test_parallel_refuses_existing_output_before_any_preparation(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import cli

    selected = _plan(tmp_path, ["a"])
    path = tmp_path / "plan.json"
    path.write_text(selected.model_dump_json())
    (tmp_path / "output").mkdir()
    monkeypatch.setattr(shared_memory, "prepare_shared_lanes", lambda *_: pytest.fail("prepared before output check"))
    with pytest.raises(ValueError, match="output already exists"):
        cli.main(["parallel", "--plan", str(path), "--output", str(tmp_path / "output"), "--prepare"])


# Late cohort attach over real Git repositories.


def _git(repository: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()


def _commit(repository: Path, files: dict[str, str], message: str) -> str:
    for name, text in files.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    _git(repository, "add", "-A")
    _git(repository, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", message)
    return _git(repository, "rev-parse", "HEAD")


class _Cluster:
    """Records every cluster write; lanes run real Git through the host instead of a container."""

    def __init__(self, monkeypatch, tmp_path, lane_repositories):
        self.events: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        self.repositories = lane_repositories
        real = shared_memory._command

        def command(*argv):
            if argv[:2] == ("stat", "-f"):
                return "ext2/ext3"
            if argv[0] == "docker":
                with self.lock:
                    self.events.append(("docker", " ".join(argv)))
                return ""
            return real(*argv)

        def kubectl(argv, *, namespace=None, kubeconfig=None, input_text=None, **_):
            with self.lock:
                self.events.append(("kubectl", f"{namespace} {' '.join(argv)}"))
            return subprocess.CompletedProcess(argv, 0, "", "")

        def json_(environment, *args, namespace=None):
            if args[:2] == ("get", "pods"):
                return {"items": [{"spec": {"nodeName": "node"}, "status": {"phase": "Running"}}]}
            return {
                "metadata": {"name": "sdo-controller-run", "uid": "u"},
                "spec": {
                    "template": {
                        "metadata": {"labels": {}},
                        "spec": {
                            "containers": [{"name": "controller", "args": ["--application", environment.application]}]
                        },
                    }
                },
            }

        def lane_git(environment, _volume, _accepted, *git):
            return ("git", "-C", str(self.repositories[environment.cluster]), *git)

        def pause(_ops, control, **_):
            with self.lock:
                self.events.append(("pause", control))

        from benchmarks.sregym import adapter

        monkeypatch.setattr(shared_memory, "_command", command)
        monkeypatch.setattr(shared_memory, "kubectl", kubectl)
        monkeypatch.setattr(shared_memory, "_json", json_)
        monkeypatch.setattr(shared_memory, "lane_git_command", lane_git)
        monkeypatch.setattr(shared_memory, "lane_nodes", lambda *_: ["node"])
        monkeypatch.setattr(shared_memory, "repository_volume_path", lambda e: f"/opt/sdo-parallel/lanes/{e.cluster}")
        monkeypatch.setattr(shared_memory, "assert_shared_lane", lambda e: self.events.append(("verify", e.cluster)))
        monkeypatch.setattr(adapter, "pause_for_recovery", pause)

    def touched(self, kind):
        return [detail for event, detail in self.events if event == kind]


def _live_memory(tmp_path):
    accepted = tmp_path / "root" / "accepted"
    accepted.mkdir(parents=True)
    _git(accepted, "init", "-q")
    base = _commit(accepted, {"README.md": "seed\n", CAPTURE: "cluster-a\n"}, "baseline")
    learned = _commit(accepted, {".sdo/playbooks/fix/README.md": "learned\n"}, "reflection")
    (accepted / ".git" / "sdo-shared-id").write_text("shared\n")
    return accepted, base, learned


def _lane_repository(tmp_path, name, accepted, commit, files):
    lane = tmp_path / "private" / name
    subprocess.run(["git", "clone", "-q", str(accepted), str(lane)], check=True)
    _git(lane, "reset", "-q", "--hard", commit)
    if files:
        _commit(lane, files, "private baseline")
    return lane


def test_late_cohort_joins_running_memory_without_touching_running_lanes_or_history(monkeypatch, tmp_path):
    accepted, base, learned = _live_memory(tmp_path)
    running = _plan(tmp_path, ["a"])
    _environments(tmp_path, running, shared_id="shared")
    joining = _plan(tmp_path, ["b", "c"], run_id="grow")
    _environments(tmp_path, joining)
    lanes = {
        # Prepared before the running lane published its reflection.
        "kind-b": _lane_repository(tmp_path, "b", accepted, base, {}),
        # Prepared at the current head.
        "kind-c": _lane_repository(tmp_path, "c", accepted, learned, {}),
    }
    cluster = _Cluster(monkeypatch, tmp_path, lanes)
    history = _git(accepted, "rev-list", "HEAD")
    report = tmp_path / "grow.attachment.json"

    shared_memory.attach_shared_memory(joining, report)

    assert _git(accepted, "rev-list", "HEAD") == history
    assert sorted(cluster.touched("pause")) == ["app-b-sdo", "app-c-sdo"]
    stopped = [detail for detail in cluster.touched("kubectl") if " delete job " in f" {detail} "]
    assert sorted(detail.split()[0] for detail in stopped) == ["app-b-sdo", "app-c-sdo"]
    docker = cluster.touched("docker")
    # Running writers may be committing: a late cohort never rewrites ownership of the live checkout.
    assert not [command for command in docker if " chown " in command or " chmod " in command]
    assert sum(" mount --bind " in command for command in docker) == 2
    for name in ("b", "c"):
        lane = FastloopEnvironment.load(joining.lanes[name])
        assert (lane.shared_accepted_id, lane.parallel_lane) == ("shared", name)
        assert lane.workspace == accepted
    saved = json.loads(report.read_text())
    assert (saved["phase"], saved["cohort"], saved["accepted_commit"]) == ("attached", "late", learned)
    assert {row["cluster"]: row["matched_commit"] for row in saved["lanes"]} == {"kind-b": base, "kind-c": learned}
    assert not any("discarded_cluster_captures" in row for row in saved["lanes"])


def test_late_cohort_with_private_memory_is_refused_before_any_writer_stops(monkeypatch, tmp_path):
    accepted, base, _ = _live_memory(tmp_path)
    joining = _plan(tmp_path, ["b"], run_id="grow")
    _environments(tmp_path, joining)
    lanes = {"kind-b": _lane_repository(tmp_path, "b", accepted, base, {".sdo/playbooks/own/README.md": "x\n"})}
    cluster = _Cluster(monkeypatch, tmp_path, lanes)
    with pytest.raises(RuntimeError, match="merge through the broker.*playbooks/own"):
        shared_memory.attach_shared_memory(joining, tmp_path / "grow.attachment.json")
    assert not [detail for detail in cluster.touched("kubectl") if " delete job " in f" {detail} "]
    assert not FastloopEnvironment.load(joining.lanes["b"]).shared_accepted_id


def test_lane_from_another_shared_memory_is_refused(monkeypatch, tmp_path):
    _live_memory(tmp_path)
    selected = _plan(tmp_path, ["a"])
    _environments(tmp_path, selected, shared_id="other")
    joining = _plan(tmp_path, ["b"], run_id="grow")
    _environments(tmp_path, joining)
    _Cluster(monkeypatch, tmp_path, {})
    both = _plan(tmp_path, ["a", "b"], run_id="grow")
    with pytest.raises(ValueError, match="partially attached"):
        shared_memory.attach_shared_memory(both, tmp_path / "grow.attachment.json")


def test_initial_cohort_owns_the_checkout_and_matches_the_current_head(monkeypatch, tmp_path):
    accepted = tmp_path / "root" / "accepted"
    accepted.mkdir(parents=True)
    _git(accepted, "init", "-q")
    head = _commit(accepted, {"README.md": "seed\n", CAPTURE: "cluster-a\n"}, "baseline")
    selected = _plan(tmp_path, ["a", "b"])
    _environments(tmp_path, selected)
    lanes = {
        "kind-a": _lane_repository(tmp_path, "a", accepted, head, {}),
        "kind-b": _lane_repository(tmp_path, "b", accepted, head, {}),
    }
    cluster = _Cluster(monkeypatch, tmp_path, lanes)
    shared_memory.attach_shared_memory(selected, tmp_path / "attachment.json")
    assert any(" chown " in command for command in cluster.touched("docker"))
    saved = json.loads((tmp_path / "attachment.json").read_text())
    assert (saved["cohort"], saved["accepted_commit"]) == ("initial", head)
    assert {row["cluster"]: row["matched_commit"] for row in saved["lanes"]} == {"kind-a": head, "kind-b": head}


# Cluster-local captures are controller state now, so no path is exempt: a lane
# whose tree differs from every accepted commit is refused, capture or not.
@pytest.mark.parametrize(
    "files",
    [
        {".sdo/playbooks/fix/README.md": "private\n"},
        {CAPTURE: "cluster-b\n"},
        {CAPTURE: "cluster-b\n", ".sdo/outcomes/x.json": "{}\n"},
        {".sdo/diagnostics/detectors/health/a/b/baseline.json": "nested\n"},
    ],
)
def test_any_lane_divergence_is_refused(monkeypatch, tmp_path, files):
    accepted, base, learned = _live_memory(tmp_path)
    lane = _lane_repository(tmp_path, "b", accepted, learned, files)
    monkeypatch.setattr(shared_memory, "lane_git_command", lambda _e, _v, _a, *git: ("git", "-C", str(lane), *git))
    with pytest.raises(ValueError, match="merge through the broker"):
        shared_memory.lane_match(object(), "/opt/sdo-parallel/lanes/b", accepted, learned)
    # An unchanged lane prepared before peers published matches its accepted commit.
    clean = _lane_repository(tmp_path, "c", accepted, base, {})
    monkeypatch.setattr(shared_memory, "lane_git_command", lambda _e, _v, _a, *git: ("git", "-C", str(clean), *git))
    assert shared_memory.lane_match(object(), "/x", accepted, learned) == base
