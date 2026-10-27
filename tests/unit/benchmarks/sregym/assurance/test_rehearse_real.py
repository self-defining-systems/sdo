"""Real-app batch rehearsal: private lane environments, stage mapping from incident records, the cluster-object
diff, the program-reuse assertion, teardown verification and continue-past-failure ordering."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.assurance import rehearse_batch as batch
from benchmarks.sregym.assurance import rehearse_real as real

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

IMAGES = {"controller": "c:t", "responder": "r:t", "validator": "v:t"}


def _lane(tmp_path: Path) -> Path:
    source = tmp_path / "lane"
    (source / "application_workspace").mkdir(parents=True)
    (source / "cluster_baseline_state.json").write_text("{}", encoding="utf-8")
    (source / "fastloop.json").write_text(
        json.dumps(
            {
                "cluster": "luna-pilot-54",
                "kubeconfig": "/k/cfg",
                "namespace": "hotel-reservation",
                "run_dir": str(source),
                "workspace": str(source / "application_workspace"),
                "private_tmp": str(source / "tmp"),
                "images": {"controller": "old", "responder": "old", "validator": "old"},
                "worker_env": {"SREGYM_APP_SOURCE_DIR": str(source / "application_workspace"), "KEEP": "1"},
            }
        ),
        encoding="utf-8",
    )
    return source


def _record(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "injection": {"state": "injected"},
        "detected_at": "2026-10-27T00:00:10Z",
        "responder_finished_at": "2026-10-27T00:01:00Z",
        "resolved_at": "2026-10-27T00:01:30Z",
        "sdo_resolution": "sdo_mitigated",
        "recovery_oracle": {"success": True},
        "full_cleanup_verified": True,
    }
    record.update(overrides)
    return record


def _problem(slot: int = 1, app: str = "hotel_reservation") -> batch.Problem:
    return batch.Problem(slot, f"p{slot}", app, ("MissingConfigMap",), ("ConfigMap",))


def test_private_environment_never_writes_the_lane_directory(tmp_path: Path) -> None:
    source = _lane(tmp_path)
    before = sorted(p.relative_to(source) for p in source.rglob("*"))
    cloned: list[tuple[Path, Path]] = []
    path = real.private_environment(
        source, tmp_path / "private", images=IMAGES, clone=lambda a, b: (cloned.append((a, b)), b.mkdir())[0]
    )
    assert sorted(p.relative_to(source) for p in source.rglob("*")) == before
    env = json.loads(path.read_text(encoding="utf-8"))
    private = tmp_path / "private"
    assert (env["run_dir"], env["workspace"], env["private_tmp"]) == (
        str(private),
        str(private / "application_workspace"),
        str(private / "tmp"),
    )
    assert cloned == [(source / "application_workspace", private / "application_workspace")]
    assert env["images"] == IMAGES
    assert env["cluster"] == "luna-pilot-54"
    assert env["worker_env"]["SREGYM_APP_SOURCE_DIR"] == str(private / "application_workspace")
    assert env["worker_env"]["KEEP"] == "1"
    assert json.loads(env["worker_env"]["SREGYM_KIND_REQUIRED_IMAGES"]) == list(IMAGES.values())


def test_private_environment_requires_every_image_role(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="validator"):
        real.private_environment(_lane(tmp_path), tmp_path / "p", images={"controller": "c", "responder": "r"})


def test_a_clean_incident_passes_every_stage() -> None:
    stages = real.record_stages(_record())
    assert [s.name for s in stages] == ["admission", "inject", "detect", "responder", "closure", "recovery", "cleanup"]
    assert all(s.status == "pass" and not s.error_class for s in stages)


@pytest.mark.parametrize(
    ("record", "stage", "error_class"),
    [
        (
            _record(injection={"state": "not_injected", "stage": "injection"}, error="boom"),
            "inject",
            "injection-failed",
        ),
        (_record(detected_at=None), "detect", "no-detection"),
        (_record(error="scripted fastloop has no source repair for ResourceQuota"), "responder", "no-scripted-repair"),
        (_record(recovery_trigger="closure_failed", resolved_at=None), "closure", "closure-failed"),
        (_record(sdo_resolution="external_recovery"), "closure", "not-mitigated"),
        (_record(recovery_oracle={"success": False}), "recovery", "recovery-failed"),
        (
            _record(eval_notes=[{"kind": "app_namespace_inventory_drift", "not_in_baseline": ["cm/x"]}]),
            "cleanup",
            "fault-leftover",
        ),
        (_record(full_cleanup_verified=False), "cleanup", "fault-leftover"),
    ],
)
def test_each_broken_stage_names_its_error_class(record: dict[str, Any], stage: str, error_class: str) -> None:
    failed = [s for s in real.record_stages(record) if s.status == "fail"]
    assert [(s.name, s.error_class) for s in failed] == [(stage, error_class)]


def test_infra_error_before_injection_is_an_admission_failure() -> None:
    record = _record(infra_error="lane resources", injection={"state": "not_injected", "stage": "pre_injection"})
    assert [(s.name, s.error_class) for s in real.record_stages(record)] == [("admission", "infra-error")]


def test_missing_record_is_a_failure_not_a_pass() -> None:
    stages = real.record_stages(None)
    assert [(s.name, s.status, s.error_class) for s in stages] == [("admission", "fail", "no-record")]


def test_leftover_diff_names_added_and_missing_objects() -> None:
    before = {"ns/configmap/a", "ns/service/s"}
    after = {"ns/configmap/a", "ns/resourcequota/q", "validatingwebhookconfiguration/w"}
    assert real.leftovers(before, after) == (
        ["ns/resourcequota/q", "validatingwebhookconfiguration/w"],
        ["ns/service/s"],
    )


def test_inventory_reads_workloads_config_and_webhooks_and_never_secrets() -> None:
    seen: list[list[str]] = []

    def command(argv: Sequence[str]) -> tuple[int, str]:
        seen.append(list(argv))
        return 0, "x/n\n"

    names = real.inventory(command, "ns", real.Path("/k"))
    assert all("secret" not in " ".join(argv) for argv in seen)
    assert {"validatingwebhookconfiguration", "mutatingwebhookconfiguration", "resourcequota"} <= {
        a for argv in seen for a in argv
    }
    assert "ns/x/n" in names


def test_inventory_failure_is_raised_not_swallowed() -> None:
    with pytest.raises(RuntimeError, match="deployment"):
        real.inventory(lambda argv: (1, "forbidden"), "ns", real.Path("/k"))


def test_program_reuse_is_asserted_only_when_a_program_matched() -> None:
    assert real.program_reuse_note(_record(), earlier_learning=False).status == "skip"
    assert real.program_reuse_note(_record(), earlier_learning=True).status == "skip"
    matched = _record(program_verdicts={"Match": 1})
    failed = real.program_reuse_note(matched, earlier_learning=True)
    assert (failed.status, failed.error_class) == ("fail", "program-not-applied")
    ok = real.program_reuse_note(
        _record(program_verdicts={"Match": 1}, tool_commands={"apply": 1}), earlier_learning=True
    )
    assert ok.status == "pass"


def test_run_arguments_run_the_scripted_registrations_behind_the_lane_guard(tmp_path: Path) -> None:
    lane = real.RealLane("hotel_reservation", tmp_path, "luna-pilot-54", tmp_path / "cfg", "hotel", "le-p4b")
    argv = real.run_arguments(lane, "p1", timeout=100, time_scale=1.0)
    assert argv[argv.index("--cluster") + 1] == "luna-pilot-54"
    assert argv[argv.index("--lease") + 1] == "le-p4b"
    assert argv[argv.index("--agent-registrations") + 1] == str(real.SCRIPTED_REGISTRATIONS)
    assert real.SCRIPTED_REGISTRATIONS.joinpath("responder.json").is_file()
    assert "--require-full-cleanup" in argv
    assert "--continue-on-error" in argv


def test_the_batch_continues_past_a_failed_problem_and_resets_between(tmp_path: Path) -> None:
    source = _lane(tmp_path)
    calls: list[str] = []
    records = iter([_record(detected_at=None), _record()])
    inventories = iter([{"a"}, {"a"}, {"a"}, {"a", "ns/resourcequota/q"}])

    def execute(argv: Sequence[str], *, cwd: Path, timeout: float, log: Path) -> tuple[int, str]:
        text = " ".join(argv)
        calls.append("reset" if " reset " in f" {text} " else "run" if " run " in f" {text} " else "kubectl")
        if "get" in argv:
            return 0, "\n".join(sorted(next(inventories))) if argv[argv.index("get") + 1] == "deployment" else ""
        if calls[-1] == "run":
            (lane_run / "results/r").mkdir(parents=True, exist_ok=True)
            (lane_run / "results/r/incidents.jsonl").write_text(json.dumps(next(records)) + "\n", encoding="utf-8")
        return 0, ""

    lane_run = tmp_path / "out" / "environment"
    app = real.RealApp(
        real.RealLane.load("hotel_reservation", source, "le-p4b"),
        [_problem(1), _problem(2)],
        tmp_path,
        tmp_path / "out",
        IMAGES,
        execute=execute,
    )
    real.private_environment(source, lane_run, images=IMAGES, clone=lambda a, b: b.mkdir())
    lane = real.RealLane.load("hotel_reservation", lane_run, "le-p4b")
    first, learned = app.run_problem(lane, app.problems[0], False)
    second, _ = app.run_problem(lane, app.problems[1], learned)
    assert first.failure is not None
    assert first.failure.error_class == "no-detection"
    assert second.failure is not None
    assert second.failure.error_class == "fault-leftover"
    assert "resourcequota/q" in second.failure.detail
    assert calls.count("reset") == 2
    assert [s.name for s in first.stages][-1] == "lane-ready"
    assert set(app.seconds) == {"p1", "p2"}


def test_apps_without_a_lane_are_a_coverage_class_not_a_failure() -> None:
    static = [batch.Row(_problem(1, "social_network")), batch.Row(_problem(2))]
    rows = real.merge_rows(static, {}, {"hotel_reservation": (real.Path("/x"), "")})
    assert rows[0].failure is not None
    assert rows[0].failure.error_class == "no-lane"
    assert rows[1].findings == []
    summary = real.real_summary(rows, tag="t", code_root=real.Path("/nonexistent"), lanes={}, seconds=1)
    assert summary["failing"] == 0
    assert summary["coverage_gaps"] == 1


def test_lane_option_parses_app_dir_and_lease() -> None:
    assert real.parse_lanes(["social=/a/b:le-x"]) == {"social": (real.Path("/a/b"), "le-x")}
    with pytest.raises(ValueError, match="APP=ENV_DIR"):
        real.parse_lanes(["social"])


def test_teardown_deletes_and_reports_clusters_down_left_behind(monkeypatch: pytest.MonkeyPatch) -> None:
    deleted: list[str] = []
    monkeypatch.setattr(
        batch.subprocess,
        "run",
        lambda argv, **kw: deleted.append(argv[-1]) or batch.subprocess.CompletedProcess(argv, 0),
    )
    assert batch.verify_teardown("reht1", listing=lambda prefix: ["reht10-control-plane"]) == ["reht10-control-plane"]
    assert deleted == ["reht10-control-plane"]
    deleted.clear()
    assert batch.verify_teardown("reht1", listing=lambda prefix: []) == []
    assert deleted == []
