"""The SREGym workload adapter's verbs against a fake runner: the lanes contract (last line JSON, exit codes)."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from benchmarks.sregym.lanes import subject, workload
from benchmarks.sregym.lanes.workload import AdapterError, dispatch, parse_args

PACK = Path(__file__).resolve().parents[5] / "benchmarks/sregym/lanes/apps/hotel_reservation"
NOW = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)


class FakeRunner:
    """Answers commands by their first words; records every call."""

    def __init__(self, answers: dict[str, str] | None = None, fail: tuple[str, ...] = ()) -> None:
        self.answers = answers or {}
        self.fail = fail
        self.calls: list[list[str]] = []

    def __call__(self, argv, *, env=None, cwd=None, tick=None) -> str:
        argv = list(argv)
        self.calls.append(argv)
        text = " ".join(argv)
        for key in self.fail:
            if key in text:
                raise AdapterError(f"{key}: exit 1")
        for key, answer in self.answers.items():
            if key in text:
                return answer
        return ""


def lane_env(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = {
        "SREGYM_LANES_ENV": str(tmp_path / "env"),
        "BENCH_LANE": "sgl1",
        "KUBECONFIG": str(tmp_path / "kube" / "sgl1"),
        "BENCH_PACK": str(PACK),
        "FUZZOPS_NAMESPACE": "hotel-reservation",
        "BENCH_EXPERIMENT": "sg-p1",
    }
    env.update(extra)
    return env


def call(verb: str, action: str, args: list[str], env: dict[str, str], **seams) -> tuple[int, dict]:
    env = {**env, "WORKLOAD_ACTION": action, "WORKLOAD_ARGS": json.dumps(args)}
    return dispatch(verb, env, now=lambda: NOW, **seams)


def write_environment(tmp_path: Path, workspace: Path, seed_commit: str = "") -> Path:
    env_dir = tmp_path / "env" / "sgl1"
    env_dir.mkdir(parents=True)
    kube = tmp_path / "fastloop-kubeconfig"
    kube.write_text("apiVersion: v1\n")
    (env_dir / "fastloop.json").write_text(
        json.dumps(
            {
                "application": "hotel_reservation",
                "namespace": "hotel-reservation",
                "kubeconfig": str(kube),
                "workspace": str(workspace),
                "seed_commit": seed_commit,
            }
        )
    )
    return env_dir


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def accepted_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "accepted"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    (repo / "README").write_text("base\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "base")
    return repo, git(repo, "rev-parse", "HEAD")


def test_parse_args_takes_both_flag_forms_and_bare_flags():
    assert parse_args(json.dumps(["--pack", "p", "--seed=3", "--skip-reset", "--namespace", "ns"])) == {
        "pack": "p",
        "seed": "3",
        "skip-reset": "true",
        "namespace": "ns",
    }
    with pytest.raises(AdapterError):
        parse_args('{"pack": 1}')


def test_identity_reports_the_checkout_revision_and_modified_state(tmp_path):
    runner = FakeRunner({"rev-parse HEAD": "abc123\n", "status --porcelain": " M x.py\n"})
    assert call("identity", "", [], {}, runner=runner) == (
        0,
        {"workload": "sregym", "revision": "abc123", "modified": True},
    )


def test_footprint_and_unknown_actions_are_unsupported(tmp_path):
    assert dispatch("footprint", {}) == (64, {"unsupported": True})
    assert call("app-up", "teleport", [], lane_env(tmp_path))[0] == 64


def test_check_pack_loads_app_json_or_answers_invalid(tmp_path):
    assert call("app-up", "check-pack", ["--pack", str(PACK)], {}) == (
        0,
        {"application": "hotel_reservation", "namespace": "hotel-reservation"},
    )
    code, result = call("app-up", "check-pack", ["--pack", str(tmp_path)], {})
    assert code == workload.EXIT_INVALID
    assert "app pack" in result["error"]


def test_app_up_runs_fastloop_up_for_a_new_lane_and_copies_its_kubeconfig(tmp_path):
    env = lane_env(
        tmp_path,
        SREGYM_LANES_UP_ARGS="--lane-cpus 8",
        BENCH_EVIDENCE_HOST="/ev",
        SREGYM_LANES_CLUSTER_UP="1",
        SREGYM_LANES_SEED_hotel_reservation="/bases/hotel",
        SREGYM_LANES_SEED_social_network="/bases/social",
    )
    runner = FakeRunner({"kind get clusters": "other\n"})
    seen: list[list[str]] = []

    def up(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        write_environment(tmp_path, tmp_path / "ws")
        return ""

    code, result = call(
        "app-up", "up", ["--pack", str(PACK), "--namespace", "hotel-reservation"], env, runner=runner, long_runner=up
    )
    assert code == 0
    assert result["reused"] is False
    argv = seen[0]
    assert argv[argv.index("--cluster-prefix") + 1] == "sgl"
    assert argv[argv.index("--worker-id") + 1] == "1"
    assert argv[argv.index("--problem") + 1] == "misconfig_app_hotel_res"
    assert "--lane-cpus" in argv
    assert argv[argv.index("--evidence-root") + 1] == "/ev"
    assert argv[argv.index("--seed") + 1] == "/bases/hotel"  # the seed of the pack's application
    assert (tmp_path / "kube" / "sgl1").read_text() == "apiVersion: v1\n"


def test_app_up_clears_what_a_failed_up_of_the_lane_left_before_trying_again(tmp_path):
    # A failed `up` left the lane's kind cluster and a half-made environment (no fastloop.json): fastloop refuses an
    # existing cluster name for a new environment, so the lane's own leftovers go first, and only the lane's.
    partial = tmp_path / "env" / "sgl1"
    (partial / "application_workspace").mkdir(parents=True)
    runner = FakeRunner({"kind get clusters": "sgl1\nother\n"})

    def up(argv, *, env=None, cwd=None, tick=None):
        assert not (partial / "application_workspace").exists()
        write_environment(tmp_path, tmp_path / "ws")
        return ""

    code, _ = call(
        "app-up",
        "up",
        ["--pack", str(PACK), "--namespace", "hotel-reservation"],
        lane_env(tmp_path, SREGYM_LANES_CLUSTER_UP="1"),
        runner=runner,
        long_runner=up,
    )
    assert code == 0
    deletes = [argv for argv in runner.calls if argv[:3] == ["kind", "delete", "cluster"]]
    assert deletes == [["kind", "delete", "cluster", "--name", "sgl1"]]


def test_only_the_hosts_cluster_up_makes_a_lane(tmp_path):
    # A build's app-up that finds no lane must not make one outside the lane's cgroup, cores and memory (a kind
    # cluster made here would run unconfined); it fails, and the platform makes the lane through cluster_up.
    runner = FakeRunner({"kind get clusters": "other\n"})

    def never(argv, **_):
        raise AssertionError("fastloop up must not run outside cluster_up")

    code, result = call(
        "app-up",
        "up",
        ["--pack", str(PACK), "--namespace", "hotel-reservation"],
        lane_env(tmp_path),
        runner=runner,
        long_runner=never,
    )
    assert code == 1
    assert "cluster_up makes it" in result["error"]
    assert not any(argv[:2] == ["kind", "delete"] for argv in runner.calls)


def test_app_up_reuses_a_lane_whose_environment_and_cluster_exist(tmp_path):
    write_environment(tmp_path, tmp_path / "ws")
    runner = FakeRunner({"kind get clusters": "sgl1\n"})

    def never(argv, **_):
        raise AssertionError("fastloop up must not run")

    code, result = call(
        "app-up",
        "up",
        ["--pack", str(PACK), "--namespace", "hotel-reservation"],
        lane_env(tmp_path),
        runner=runner,
        long_runner=never,
    )
    assert (code, result["reused"]) == (0, True)


def test_app_up_refuses_a_spec_namespace_the_lane_does_not_serve(tmp_path):
    write_environment(tmp_path, tmp_path / "ws")
    runner = FakeRunner({"kind get clusters": "sgl1\n"})
    code, result = call(
        "app-up", "up", ["--pack", str(PACK), "--namespace", "social"], lane_env(tmp_path), runner=runner
    )
    assert code == 1
    assert "namespace" in result["error"]


def test_lane_name_must_end_in_a_worker_number(tmp_path):
    runner = FakeRunner({"kind get clusters": ""})
    code, result = call(
        "app-up",
        "up",
        ["--pack", str(PACK)],
        lane_env(tmp_path, BENCH_LANE="lane", SREGYM_LANES_CLUSTER_UP="1"),
        runner=runner,
    )
    assert code == 1
    assert "worker number" in result["error"]


def test_down_images_and_recover_are_no_ops(tmp_path):
    for verb, action in (("app-up", "down"), ("app-up", "images"), ("app-reset", "recover")):
        assert call(verb, action, ["--pack", str(PACK)], lane_env(tmp_path))[0] == 0


def binding(name: str, role: str, *subjects: tuple[str, str, str]) -> dict:
    return {
        "metadata": {"name": name},
        "roleRef": {"kind": "ClusterRole", "name": role},
        "subjects": [{"kind": kind, "name": who, "namespace": namespace} for kind, who, namespace in subjects],
    }


def test_owned_names_the_cluster_objects_the_lanes_operator_install_recreates_every_row(tmp_path):
    """The operator is installed at each row start and wiped at the next: its objects are not fault residue."""
    bindings = {
        "items": [
            binding(
                "sdo-admin-hotel-reservation-sdo",
                "sdo-admin-hotel-reservation-sdo",
                ("ServiceAccount", "sdo-controller", "hotel-reservation-sdo"),
            ),
            binding(
                "sdo-prometheus-hotel-reservation-sdo",
                "sdo-prometheus-hotel-reservation-sdo",
                ("ServiceAccount", "sdo-prometheus", "hotel-reservation-sdo"),
            ),
            binding("cluster-admin", "cluster-admin", ("Group", "system:masters", "")),
            binding("fault-binding", "fault-role", ("ServiceAccount", "default", "hotel-reservation")),
            binding(
                "other-lane-admin",
                "sdo-admin-social-network-sdo",
                ("ServiceAccount", "sdo-controller", "social-network-sdo"),
            ),
        ]
    }
    runner = FakeRunner({"get clusterrolebindings": json.dumps(bindings)})
    code, result = call("app-reset", "owned", ["--namespace", "hotel-reservation"], lane_env(tmp_path), runner=runner)
    assert code == 0
    assert sorted(result["objects"]) == [
        "clusterrole.rbac.authorization.k8s.io/sdo-admin-hotel-reservation-sdo",
        "clusterrole.rbac.authorization.k8s.io/sdo-prometheus-hotel-reservation-sdo",
        "clusterrolebinding.rbac.authorization.k8s.io/sdo-admin-hotel-reservation-sdo",
        "clusterrolebinding.rbac.authorization.k8s.io/sdo-prometheus-hotel-reservation-sdo",
        "namespace/hotel-reservation-sdo",
    ]


def test_owned_with_no_operator_installed_still_names_only_its_namespace(tmp_path):
    runner = FakeRunner({"get clusterrolebindings": '{"items": []}'})
    code, result = call("app-reset", "owned", ["--namespace", "hotel-reservation"], lane_env(tmp_path), runner=runner)
    assert (code, result) == (0, {"objects": ["namespace/hotel-reservation-sdo"]})


DEPLOYMENTS = {
    "items": [
        {
            "kind": "Deployment",
            "metadata": {"name": "frontend"},
            "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": "f:1"}]}}},
        }
    ]
}


def test_snapshot_write_then_verify_matches_and_detects_a_changed_image(tmp_path):
    runner = FakeRunner({"get deployments,statefulsets": json.dumps(DEPLOYMENTS)})
    out = tmp_path / "fp.json"
    assert (
        call(
            "app-snapshot",
            "write",
            ["--namespace", "hotel-reservation", "--out", str(out)],
            lane_env(tmp_path),
            runner=runner,
        )[0]
        == 0
    )
    assert (
        call(
            "app-snapshot",
            "verify",
            ["--namespace", "hotel-reservation", "--against", str(out)],
            lane_env(tmp_path),
            runner=runner,
        )[0]
        == 0
    )
    changed = json.loads(json.dumps(DEPLOYMENTS))
    changed["items"][0]["spec"]["template"]["spec"]["containers"][0]["image"] = "f:2"
    runner = FakeRunner({"get deployments,statefulsets": json.dumps(changed)})
    code, result = call(
        "app-snapshot",
        "verify",
        ["--namespace", "hotel-reservation", "--against", str(out)],
        lane_env(tmp_path),
        runner=runner,
    )
    assert code == 1
    assert result["differ"]


def pod(name: str, ready: str, phase: str = "Running") -> dict:
    return {"metadata": {"name": name}, "status": {"phase": phase, "conditions": [{"type": "Ready", "status": ready}]}}


def test_gate_is_clean_only_when_every_running_pod_is_ready(tmp_path):
    ok = {"items": [pod("a", "True"), pod("job", "False", "Succeeded")]}
    assert (
        call(
            "gate", "check", ["--namespace", "n"], lane_env(tmp_path), runner=FakeRunner({"get pods": json.dumps(ok)})
        )[0]
        == 0
    )
    bad = {"items": [pod("a", "True"), pod("b", "False")]}
    code, result = call(
        "gate", "check", ["--namespace", "n"], lane_env(tmp_path), runner=FakeRunner({"get pods": json.dumps(bad)})
    )
    assert code == 1
    assert result["not_ready"] == ["b"]
    assert (
        call(
            "gate", "check", ["--namespace", "n"], lane_env(tmp_path), runner=FakeRunner({"get pods": '{"items": []}'})
        )[0]
        == 1
    )


PROBLEM = "misconfig_app_hotel_res"


def active_after(tmp_path: Path, row: dict | None, *, killed: bool = False) -> bool:
    """`run active` after a run whose fastloop record is row (None: no record), or that was killed mid-way."""
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def fast(argv, **_):
        if killed:
            raise SystemExit("killed")
        run_id = list(argv)[list(argv).index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True)
        if row is not None:
            (env_dir / "results" / run_id / "incidents.jsonl").write_text(
                json.dumps({**incident(run_id), **row}) + "\n"
            )
        return ""

    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x"))
    try:
        call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast)
    except SystemExit:
        pass
    code, result = call("run", "active", ["--file", "/i.json", "--id", PROBLEM], env)
    assert code == 0
    assert f"active={str(result['active']).lower()}" in json.dumps(result)
    return result["active"]


FAILED_ORACLE = {"kind": "mitigation", "success": False, "details": {}}


def test_run_active_is_false_only_when_the_run_verified_its_cleanup(tmp_path):
    assert active_after(tmp_path, {"full_cleanup_verified": True, "oracle": FAILED_ORACLE}) is False


def test_a_fault_the_oracle_saw_mitigated_is_clear_when_fastloop_defers_its_cleanup(tmp_path):
    # fastloop leaves the full cleanup of a mitigated row to the next row's start (cleanup_skipped_reason
    # row_start_tier2): the problem's own oracle saw the fault mitigated, so the lane holds no live fault.
    row = {"full_cleanup_verified": False, "cleanup_skipped_reason": "row_start_tier2"}
    assert active_after(tmp_path, row) is False


@pytest.mark.parametrize(
    "row",
    [
        {"full_cleanup_verified": False, "oracle": FAILED_ORACLE},
        {"full_cleanup_verified": True, "recovery_error": "fault still present"},
        None,
    ],
)
def test_run_active_is_true_when_the_fault_may_still_be_in_place(tmp_path, row):
    assert active_after(tmp_path, row) is True


def test_a_run_killed_mid_way_leaves_the_lane_active(tmp_path):
    assert active_after(tmp_path, None, killed=True) is True


def test_a_run_that_never_injected_leaves_no_fault(tmp_path):
    assert active_after(tmp_path, {"injection": {"state": "not_injected", "stage": "pre_injection"}}) is False


def test_run_active_with_no_verdict_on_the_lane_is_active(tmp_path):
    assert call("run", "active", ["--id", PROBLEM], lane_env(tmp_path))[1]["active"] is True


def incident(run_id: str, success: bool = True, before: str = "", after: str = "") -> dict:
    return {
        "run_id": run_id,
        "problem_id": "misconfig_app_hotel_res",
        "injection": {"state": "injected", "started_at": "2026-11-20T10:01:00Z", "finished_at": "2026-11-20T10:01:02Z"},
        "oracle": {"kind": "mitigation", "success": success, "details": {}},
        "time_to_fix_seconds": 42.0,
        "resolution_path": "agent",
        "memory_before_commit": before,
        "memory_after_commit": after,
    }


def fake_fastloop_run(tmp_path: Path, env_dir: Path, repo: Path):
    """A fastloop run that writes a playbook into the accepted repository and one incident record."""
    calls: list[list[str]] = []

    def run(argv, *, env=None, cwd=None, tick=None):
        argv = list(argv)
        calls.append(argv)
        run_id = argv[argv.index("--run-id") + 1]
        before = git(repo, "rev-parse", "HEAD")
        book = repo / ".sdo" / "playbooks" / "misconfig" / "README.md"
        book.parent.mkdir(parents=True, exist_ok=True)
        book.write_text(f"learned in {run_id}\n")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "learn")
        results = env_dir / "results" / run_id
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(
            json.dumps(incident(run_id, before=before, after=git(repo, "rev-parse", "HEAD"))) + "\n"
        )
        return ""

    return run, calls


RUN_ARGS = [
    "--skip-reset",
    "--operator",
    "exec:sregym-sg-p1-r1",
    "--seed",
    "1",
    "--instance-file",
    "/i.json",
    "--instance",
    "misconfig_app_hotel_res",
]


def test_run_records_the_incident_and_exports_the_learned_memory(tmp_path):
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    fast, calls = fake_fastloop_run(tmp_path, env_dir, repo)
    exec_out = tmp_path / "run" / "exec"
    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(exec_out), BENCH_RUN="r1")
    code, record = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast)
    assert code == 0, record
    argv = calls[0]
    assert argv[argv.index("--planned-commit") + 1] == base
    assert argv[argv.index("--agent-registrations") + 1].endswith("benchmarks/sregym/fastloop/agent_registrations")
    assert json.loads((exec_out / "stream.json").read_text())["incidents"] == ["misconfig_app_hotel_res"]
    assert record["operator"] == "exec:sregym-sg-p1-r1"
    assert record["valid"] is True
    assert record["lifecycle"]["inject_at"] == "2026-11-20T10:01:00Z"
    assert record["sregym"]["success"] is True
    memory = json.loads((exec_out / "memory" / "memory.json").read_text())
    assert memory["commit"] == git(repo, "rev-parse", "HEAD")
    assert memory["base"] == base
    assert memory["playbook_files_changed"] == [".sdo/playbooks/misconfig/README.md"]
    assert (exec_out / "memory" / "memory.bundle").is_file()
    assert (exec_out / "fastloop-results" / "incidents.jsonl").is_file()
    assert record["memory"]["seeded"] is False


def test_a_seeded_run_starts_from_the_seed_commit_fetched_from_its_bundle(tmp_path):
    # p1's lane learned a playbook and exported it.
    first, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, first, seed_commit=base)
    fast, _ = fake_fastloop_run(tmp_path, env_dir, first)
    exec_out = tmp_path / "p1" / "exec"
    call(
        "run",
        "run",
        RUN_ARGS,
        lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(exec_out), BENCH_RUN="r1"),
        runner=workload.run_process,
        long_runner=fast,
    )
    learned = json.loads((exec_out / "memory" / "memory.json").read_text())["commit"]
    # p2 runs on a lane whose accepted repository does not have that commit yet.
    second = tmp_path / "b" / "accepted"
    second.parent.mkdir()
    subprocess.run(["git", "clone", "-q", str(first), str(second)], check=True)
    git(second, "reset", "-q", "--hard", base)
    git(second, "reflog", "expire", "--expire=now", "--all")
    git(second, "gc", "-q", "--prune=now")
    (env_dir / "fastloop.json").write_text(
        json.dumps({**json.loads((env_dir / "fastloop.json").read_text()), "workspace": str(second)})
    )
    operator = tmp_path / "operator.json"
    operator.write_text(json.dumps({"hooks": "sregym", "seed": str(exec_out / "memory")}))
    seen: list[list[str]] = []

    def run2(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        assert git(second, "cat-file", "-t", learned) == "commit"  # fetched before the run
        run_id = argv[argv.index("--run-id") + 1]
        results = env_dir / "results" / run_id
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(json.dumps(incident(run_id, before=learned, after=learned)) + "\n")
        return ""

    env = lane_env(
        tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "p2" / "exec"), BENCH_RUN="r2", BENCH_OPERATOR=str(operator)
    )
    code, record = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=run2)
    assert code == 0, record
    assert seen[0][seen[0].index("--planned-commit") + 1] == learned
    assert record["memory"]["seeded"] is True


def baseline_lane(tmp_path: Path) -> tuple[Path, Path, str, str]:
    """A lane whose accepted repository has a base and a later onboarding commit; a baseline made at the base."""
    from benchmarks.sregym.lanes import baseline

    repo, base = accepted_repo(tmp_path)
    (repo / "later").write_text("onboarding moved on\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "later")
    later = git(repo, "rev-parse", "HEAD")
    write_environment(tmp_path, repo, seed_commit=later)
    baseline.make(repo, base, tmp_path / "baseline")
    return repo, tmp_path / "baseline", base, later


def planned_of(tmp_path: Path, operator: dict) -> tuple[int, dict, list[str]]:
    seen: list[list[str]] = []
    env_dir = tmp_path / "env" / "sgl1"

    def fast(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        run_id = list(argv)[list(argv).index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True, exist_ok=True)
        (env_dir / "results" / run_id / "incidents.jsonl").write_text(json.dumps(incident(run_id)) + "\n")
        return ""

    path = tmp_path / "operator.json"
    path.write_text(json.dumps(operator))
    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x"), BENCH_OPERATOR=str(path))
    code, record = call(
        "run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast, page_cluster=FakePageCluster(True)
    )
    return code, record, (seen[0] if seen else [])


def test_a_run_with_a_baseline_and_no_seed_starts_from_the_baseline(tmp_path):
    _, base_dir, base, later = baseline_lane(tmp_path)
    code, record, argv = planned_of(tmp_path, {"baseline": str(base_dir)})
    assert code == 0, record
    assert argv[argv.index("--planned-commit") + 1] == base  # not the lane's own onboarding commit


def test_a_seed_must_be_learned_on_the_baseline(tmp_path):
    from benchmarks.sregym.lanes import baseline

    repo, base_dir, base, later = baseline_lane(tmp_path)
    git(repo, "checkout", "-q", "--orphan", "other")
    (repo / "unrelated").write_text("x\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "unrelated")
    baseline.make(repo, "HEAD", tmp_path / "foreign-seed")
    code, record, argv = planned_of(tmp_path, {"baseline": str(base_dir), "seed": str(tmp_path / "foreign-seed")})
    assert code == 1
    assert "not learned on the baseline" in record["error"]
    assert argv == []  # refused before fastloop runs
    baseline.make(repo, later, tmp_path / "seed")  # later descends from base
    code, record, argv = planned_of(tmp_path, {"baseline": str(base_dir), "seed": str(tmp_path / "seed")})
    assert code == 0, record
    assert argv[argv.index("--planned-commit") + 1] == later


def test_a_bad_seed_fails_the_run_before_fastloop(tmp_path):
    repo, base = accepted_repo(tmp_path)
    write_environment(tmp_path, repo, seed_commit=base)
    (tmp_path / "seed").mkdir()
    operator = tmp_path / "operator.json"
    operator.write_text(json.dumps({"seed": str(tmp_path / "seed")}))

    def never(argv, **_):
        raise AssertionError("fastloop run must not start")

    code, result = call(
        "run",
        "run",
        RUN_ARGS,
        lane_env(tmp_path, BENCH_OPERATOR=str(operator)),
        runner=workload.run_process,
        long_runner=never,
    )
    assert code == 1
    assert "memory seed" in result["error"]


def test_a_run_without_a_record_is_invalid_with_the_fastloop_failure(tmp_path):
    repo, base = accepted_repo(tmp_path)
    write_environment(tmp_path, repo, seed_commit=base)

    def broken(argv, **_):
        raise AdapterError("fastloop run: exit 2")

    code, record = call(
        "run",
        "run",
        RUN_ARGS,
        lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x")),
        runner=workload.run_process,
        long_runner=broken,
    )
    assert code == 1
    assert record["valid"] is False
    assert "exit 2" in record["error"]
    assert record["operator"] == "exec:sregym-sg-p1-r1"


def test_a_not_injected_record_is_invalid(tmp_path):
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def no_inject(argv, **_):
        run_id = list(argv)[list(argv).index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True)
        row = {
            **incident(run_id),
            "injection": {"state": "not_injected", "stage": "pre_injection"},
            "infra_error": "never_injected_retry",
        }
        (env_dir / "results" / run_id / "incidents.jsonl").write_text(json.dumps(row) + "\n")
        return ""

    code, record = call(
        "run",
        "run",
        RUN_ARGS,
        lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x")),
        runner=workload.run_process,
        long_runner=no_inject,
    )
    assert code == 1
    assert record["valid"] is False
    assert record["invalid_reason"] == "never_injected_retry"


def test_a_row_fastloop_refused_to_grade_is_invalid(tmp_path):
    """The run's evidence gate stamps `evidence_incomplete` on a row whose transcripts were not copied (and prints
    `NOT GRADED`); the lane's record must not call such a row valid, or a plan counts a row nothing can grade."""
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def refused(argv, **_):
        run_id = list(argv)[list(argv).index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True)
        row = {**incident(run_id), "evidence_incomplete": "no verified evidence copy (evidence-copy.json)"}
        (env_dir / "results" / run_id / "incidents.jsonl").write_text(json.dumps(row) + "\n")
        return ""

    code, record = call(
        "run",
        "run",
        RUN_ARGS,
        lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x")),
        runner=workload.run_process,
        long_runner=refused,
    )
    assert code == 1
    assert record["valid"] is False
    assert record["invalid_reason"] == "evidence incomplete: no verified evidence copy (evidence-copy.json)"


def test_grade_rewrites_runs_jsonl_with_one_grade_per_operator(tmp_path):
    rows = [
        {
            "operator": "exec:a-r1",
            "valid": True,
            "fault": "p",
            "sregym": {"success": True, "time_to_fix_seconds": 30},
            "memory": {"seeded": False, "playbook_files_changed": ["x"]},
        },
        {"operator": "exec:b-r1", "valid": False, "invalid_reason": "no incident record"},
    ]
    (tmp_path / "runs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    code, result = dispatch(
        "grade",
        {"WORKLOAD_ACTION": "report", "WORKLOAD_ARGS": json.dumps([str(tmp_path)]), "WORKLOAD_RESULTS": str(tmp_path)},
    )
    assert (code, result) == (0, {"graded": 2})
    grades = [json.loads(line) for line in (tmp_path / "runs.jsonl").read_text().splitlines()]
    assert [g["operator"] for g in grades] == ["exec:a-r1", "exec:b-r1"]
    assert grades[0]["valid"] is True
    assert grades[0]["success"] is True
    assert grades[0]["playbooks_learned"] == 1
    assert all(isinstance(g["valid"], bool) for g in grades)


def test_main_prints_the_result_as_the_last_stdout_line(capsys, monkeypatch):
    monkeypatch.setenv("WORKLOAD_ACTION", "")
    assert workload.main(["footprint"]) == 64
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {"unsupported": True}


def test_subject_shim_describes_itself_to_the_platform():
    # the workload installs and drives SDO: no start-up period for the hooks to wait out, no model call of its own
    assert subject.dispatch("describe", {}) == (0, {"max_startup": "0s", "needs_credentials": False})


def test_subject_shim_prepares_with_the_pack_and_passes_the_seed(tmp_path):
    operator = tmp_path / "operator.json"
    operator.write_text(json.dumps({"seed": "/staged/seed"}))
    env = {"BENCH_PACK": "/pack", "BENCH_OPERATOR": str(operator)}
    assert subject.dispatch("prepare", env, version=lambda: "rev") == (
        0,
        {"pack": "/pack", "operator_version": "rev", "memory": "/staged/seed"},
    )
    assert subject.dispatch("idle", env)[0] == 0
    assert subject.dispatch("verify-artifact", env) == (64, {"unsupported": True})


class FakePageCluster:
    def __init__(self, own: bool) -> None:
        self.own = own
        self.sent: list[tuple[str, str, str]] = []

    def own_incident(self, lane, known):
        return self.own

    def send(self, lane, incident_id, message):
        self.sent.append((lane.info["namespace"], incident_id, message))


def paged_run(tmp_path: Path, cluster: FakePageCluster, ticks_at: list[float]) -> tuple[dict, Path]:
    """A run that injects at t=1000 and ticks at the given clock times, with the page cluster faked."""
    from benchmarks.sregym.fastloop.injection_marker import write_injection_marker

    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    clock = [0.0]

    def fast(argv, *, env=None, cwd=None, tick=None):
        assert tick is not None
        argv = list(argv)
        run_id = argv[argv.index("--run-id") + 1]
        results = env_dir / "results" / run_id
        results.mkdir(parents=True)
        tick()  # before injection: nothing to decide
        write_injection_marker(results, datetime.fromtimestamp(1000, UTC), frozenset({"old"}))
        for at in ticks_at:
            clock[0] = at
            tick()
        row = {**incident(run_id), "full_cleanup_verified": True}
        (results / "incidents.jsonl").write_text(json.dumps(row) + "\n")
        return ""

    exec_out = tmp_path / "exec"
    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(exec_out), BENCH_RUN="r1")
    code, record = dispatch(
        "run",
        {**env, "WORKLOAD_ACTION": "run", "WORKLOAD_ARGS": json.dumps(RUN_ARGS)},
        runner=workload.run_process,
        long_runner=fast,
        page_cluster=cluster,
        now=lambda: datetime.fromtimestamp(clock[0], UTC),
    )
    assert code == 0, record
    return record, exec_out


def test_with_no_incident_of_its_own_sdo_is_paged_once_after_the_fallback_delay(tmp_path):
    cluster = FakePageCluster(own=False)
    record, exec_out = paged_run(tmp_path, cluster, [1000 + 60, 1000 + 120, 1000 + 180])
    assert len(cluster.sent) == 1  # once, not before 120 s, never again
    namespace, _, message = cluster.sent[0]
    assert namespace == "hotel-reservation"
    assert "Namespace: hotel-reservation" in message
    assert record["sregym"]["page_source"] == "fallback"
    page = json.loads((exec_out / "page.json").read_text())
    assert page["source"] == "fallback"
    assert page["injected_at"] == 1000
    assert workload.grade_line(record)["page_source"] == "fallback"


def test_an_incident_of_its_own_means_no_page(tmp_path):
    cluster = FakePageCluster(own=True)
    record, exec_out = paged_run(tmp_path, cluster, [1000 + 130])
    assert cluster.sent == []
    assert record["sregym"]["page_source"] == "own"
    assert json.loads((exec_out / "page.json").read_text())["source"] == "own"


def test_no_decision_before_the_fallback_delay(tmp_path):
    cluster = FakePageCluster(own=False)
    record, exec_out = paged_run(tmp_path, cluster, [1000 + 30])
    assert cluster.sent == []
    assert record["sregym"]["page_source"] is None
    assert not (exec_out / "page.json").exists()


def test_the_run_uses_the_eval_runners_row_flags(tmp_path):
    seen: list[list[str]] = []
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def fast(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        run_id = list(argv)[list(argv).index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True)
        return ""

    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x"))
    call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast, page_cluster=FakePageCluster(True))
    for flag in ("--healthy-baseline", "--inject-before-resume", "--require-fault-tracking", "--require-full-cleanup"):
        assert flag in seen[0]
    assert seen[0][seen[0].index("--detection-timeout") + 1] == "300"


def eks_env(tmp_path: Path, **extra: str) -> dict[str, str]:
    return lane_env(
        tmp_path,
        BENCH_LANE_PROVIDER="eks",
        BENCH_IMAGE_REGISTRY="123456789012.dkr.ecr.us-west-2.amazonaws.com",
        BENCH_EVIDENCE_MODE="pull",
        BENCH_EVIDENCE_HOST="/ev",
        BENCH_ENTRY_HOST="203.0.113.7",
        BENCH_IMAGE_TAGS="Project=sdo,Owner=lanes",
        **extra,
    )


def test_eks_app_up_attaches_through_fastloop_and_never_calls_kind(tmp_path):
    # The provider owns the cluster: no kind, no cluster_up; fastloop attaches and pushes images to the registry.
    seen: list[list[str]] = []

    def up(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        write_environment(tmp_path, tmp_path / "ws")
        return ""

    runner = FakeRunner()
    code, result = call(
        "app-up",
        "up",
        ["--pack", str(PACK), "--namespace", "hotel-reservation"],
        eks_env(tmp_path, SREGYM_LANES_UP_ARGS="--controller-image c:1"),
        runner=runner,
        long_runner=up,
    )
    assert (code, result["reused"]) == (0, False)
    assert runner.calls == []
    (argv,) = seen
    assert argv[argv.index("--platform") + 1] == "eks"
    assert "--no-dev-mount" in argv
    assert argv[argv.index("--ecr-registry") + 1] == "123456789012.dkr.ecr.us-west-2.amazonaws.com"
    assert [argv[i + 1] for i, a in enumerate(argv) if a == "--ecr-tag"] == ["Project=sdo", "Owner=lanes"]
    assert argv[argv.index("--evidence-root") + 1] == "/ev"
    assert "--controller-image" in argv
    assert "--mirror-upstream" not in argv


def test_eks_app_up_reuses_a_lane_whose_fence_passes_and_rebuilds_one_whose_fence_fails(tmp_path):
    write_environment(tmp_path, tmp_path / "ws")
    ups: list[list[str]] = []

    def up(argv, **_):
        ups.append(list(argv))
        write_environment(tmp_path, tmp_path / "ws")
        return ""

    args = ["--pack", str(PACK), "--namespace", "hotel-reservation"]
    code, result = call(
        "app-up", "up", args, eks_env(tmp_path), runner=FakeRunner(), long_runner=up, eks_fence=lambda ctx, d: True
    )
    assert (code, result["reused"], ups) == (0, True, [])
    code, result = call(
        "app-up", "up", args, eks_env(tmp_path), runner=FakeRunner(), long_runner=up, eks_fence=lambda ctx, d: False
    )
    assert (code, result["reused"], len(ups)) == (0, False, 1)


def test_eks_fence_compares_the_recorded_cluster_with_the_kubeconfigs(tmp_path, monkeypatch):
    from benchmarks.lanes import eks as provider

    env_dir = write_environment(tmp_path, tmp_path / "ws")
    ctx = workload.Context("app-up", "up", {}, eks_env(tmp_path))
    assert workload.eks_fence(ctx, env_dir) is False  # a kind environment records no EKS cluster
    lane = {"registry": "123456789012.dkr.ecr.us-west-2.amazonaws.com", "server": "https://a", "cluster_uid": "u1"}
    doc = json.loads((env_dir / "fastloop.json").read_text())
    (env_dir / "fastloop.json").write_text(json.dumps({**doc, "eks": lane}))
    monkeypatch.setattr(provider, "cluster_identity", lambda kubeconfig, run=None: ("https://a", "u1"))
    assert workload.eks_fence(ctx, env_dir) is True
    monkeypatch.setattr(provider, "cluster_identity", lambda kubeconfig, run=None: ("https://a", "other"))
    assert workload.eks_fence(ctx, env_dir) is False


def test_every_app_pack_loads_and_names_a_namespace_fastloop_retires() -> None:
    from benchmarks.sregym.fastloop.app_retirement import APPLICATION_NAMESPACES

    packs = sorted((PACK.parent).iterdir())
    assert {p.name for p in packs} == {"astronomy_shop", "hotel_reservation", "social_network"}
    for pack in packs:
        loaded = workload.AppPack.load(pack)
        assert loaded.namespace in APPLICATION_NAMESPACES
        assert loaded.application == pack.name


def test_run_gives_the_runner_a_row_deadline_from_the_spec_cap(tmp_path):
    """The lanes step hands the spec's cap and ready_timeout to the adapter: past them plus a closeout margin the run
    is killed, whichever wait it is stuck in."""
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    fast, _ = fake_fastloop_run(tmp_path, env_dir, repo)
    seen: dict = {}

    def runner(argv, *, env=None, cwd=None, tick=None, **limits):
        seen.update(limits)
        seen["wait_file"] = env.get("SDO_CURRENT_WAIT_FILE")
        return fast(argv, env=env, cwd=cwd, tick=tick)

    env = lane_env(
        tmp_path,
        FUZZOPS_EXEC_OUT=str(tmp_path / "x"),
        BENCH_CAP_SECONDS="1800",
        BENCH_READY_TIMEOUT_SECONDS="2400",
    )
    code, _ = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=runner)
    assert code == 0
    assert seen["deadline_seconds"] == 1800 + 2400 + workload.ROW_CLOSEOUT_SECONDS
    assert seen["wait_file"] == str(tmp_path / "x" / "current-wait.json")


def test_run_bounds_the_incident_closure_by_the_spec_cap(tmp_path):
    """fastloop ends a row whose incident is still open at the cap, named: an alert that never clears never closes."""
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    fast, _ = fake_fastloop_run(tmp_path, env_dir, repo)
    commands: list[list[str]] = []

    def runner(argv, *, env=None, cwd=None, tick=None, **limits):
        commands.append(list(argv))
        return fast(argv, env=env, cwd=cwd, tick=tick)

    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x"), BENCH_CAP_SECONDS="1800")
    code, _ = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=runner)
    assert code == 0
    argv = commands[0]
    assert argv[argv.index("--closure-bound") + 1] == "1800.0"


def test_a_run_past_its_row_deadline_ends_invalid_with_the_named_wait(tmp_path):
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def stuck(argv, **_):
        raise workload.RowDeadlineError("row deadline 4200s exceeded while waiting for: outcome commit of incident i-7")

    env = lane_env(tmp_path, FUZZOPS_EXEC_OUT=str(tmp_path / "x"), BENCH_CAP_SECONDS="1800")
    code, record = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=stuck)
    assert code == 1
    assert record["valid"] is False
    assert record["invalid_reason"] == "row deadline exceeded"
    assert "outcome commit of incident i-7" in record["error"]
    # The fault may still be in place: the lane's verdict stays unknown (active) so the platform recovers it.
    assert workload.fault_active(env_dir, PROBLEM) is True
