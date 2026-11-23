"""A row on a lane that already holds an SDO install of its identity runs the warm transaction (reseed), and says so.

The workload decides from what the lane was last installed with (``lane-install.json``), passes ``--warm-lane`` only
when that is this row's install identity, and records in the row what happened: reseeded, fell back (and why) or ran
the ordinary fresh install (and why). The decision is advisory: fastloop's own warm transaction proves the lane or
falls back.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.lanes import workload
from tests.unit.benchmarks.sregym.lanes.test_lanes_real_agent import operator_file, real_settings
from tests.unit.benchmarks.sregym.lanes.test_lanes_workload import (
    RUN_ARGS,
    FakePageCluster,
    accepted_repo,
    call,
    incident,
    lane_env,
    write_environment,
)

if TYPE_CHECKING:
    from pathlib import Path

RESEED = {"SREGYM_LANES_RESEED": "1"}


class Lane:
    """One kept lane: successive rows run through a fake ``fastloop run`` that writes its record and, when it was asked
    to try the warm transaction, a receipt."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.repo, self.base = accepted_repo(tmp_path)
        self.env_dir = write_environment(tmp_path, self.repo, seed_commit=self.base)
        self.calls: list[list[str]] = []
        self.receipt: dict | None = None
        self.rows = 0

    def fastloop(self, argv, *, env=None, cwd=None, tick=None, **_):
        argv = list(argv)
        self.calls.append(argv)
        run_id = argv[argv.index("--run-id") + 1]
        results = self.env_dir / "results" / run_id
        results.mkdir(parents=True)
        (results / "incidents.jsonl").write_text(json.dumps(incident(run_id)) + "\n")
        if "--warm-lane" in argv and self.receipt is not None:
            (results / "warm-transaction.json").write_text(json.dumps({"schema": "sdo.warm-lane/v1", **self.receipt}))
        return ""

    def row(self, **env_extra: str) -> tuple[int, dict]:
        self.rows += 1
        env = lane_env(
            self.tmp_path,
            FUZZOPS_EXEC_OUT=str(self.tmp_path / f"exec{self.rows}"),
            BENCH_RUN=f"r{self.rows}",
            **env_extra,
        )
        return call(
            "run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=self.fastloop,
            page_cluster=FakePageCluster(True),
        )  # fmt: skip


def test_the_first_row_of_a_lane_installs_fresh_and_names_why(tmp_path):
    lane = Lane(tmp_path)
    code, record = lane.row(**RESEED)
    assert code == 0, record
    assert "--warm-lane" not in lane.calls[0]
    assert record["reseed"]["attempted"] is False
    assert "no earlier install" in record["reseed"]["reason"]
    assert (lane.env_dir / "lane-install.json").is_file()


def test_a_next_row_on_the_same_install_runs_the_warm_transaction_and_drains_evidence_before_fastloop_exits(tmp_path):
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    lane.receipt = {"reused": True, "identity": None, "baseline_not_before": "t", "phase_seconds": {"reseed": 31.0}}
    code, record = lane.row(**RESEED)
    assert code == 0, record
    argv = lane.calls[1]
    assert "--warm-lane" in argv
    # The workload copies results out the moment fastloop returns: the sealed evidence must be there by then.
    assert "--deliver-evidence-before-exit" in argv
    assert record["reseed"] == {
        "attempted": True,
        "reused": True,
        "reason": "",
        "violations": [],
        "phase_seconds": {"reseed": 31.0},
    }


def test_a_fallback_names_the_boundary_and_the_violations(tmp_path):
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    lane.receipt = {
        "reused": False,
        "phase_seconds": {"prove": 2.0, "full_reset": 220.0},
        "fallback_reason": "prove:ResidentStartMismatch",
        "violations": ["onboarding differs from a fresh install's on the planned memory (or has no reference)"],
    }
    code, record = lane.row(**RESEED)
    assert code == 0, record
    assert record["reseed"]["attempted"] is True
    assert record["reseed"]["reused"] is False
    assert record["reseed"]["reason"] == "prove:ResidentStartMismatch"
    assert record["reseed"]["violations"] == lane.receipt["violations"]


def test_a_row_that_asked_for_the_warm_transaction_but_left_no_receipt_is_a_fallback_for_that_reason(tmp_path):
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    code, record = lane.row(**RESEED)
    assert code == 0, record
    assert record["reseed"]["attempted"] is True
    assert record["reseed"]["reused"] is False
    assert record["reseed"]["reason"] == "no warm-transaction receipt"


def test_a_lane_installed_for_another_application_is_not_reseeded(tmp_path):
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    identity = json.loads((lane.env_dir / "lane-install.json").read_text())
    identity["application"] = "social_network"
    identity["namespace"] = "social-network"
    (lane.env_dir / "lane-install.json").write_text(json.dumps(identity))
    code, record = lane.row(**RESEED)
    assert code == 0, record
    assert "--warm-lane" not in lane.calls[1]
    assert record["reseed"]["attempted"] is False
    assert "application" in record["reseed"]["reason"]
    assert "namespace" in record["reseed"]["reason"]


def test_another_agent_configuration_is_not_reseeded(tmp_path):
    lane = Lane(tmp_path)
    settings = real_settings(tmp_path)
    lane.row(**RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, settings)))
    changed = {**settings, "effort": "high"}
    code, record = lane.row(**RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, changed)))
    assert code == 0, record
    assert "--warm-lane" not in lane.calls[1]
    assert record["reseed"]["attempted"] is False
    assert "agent" in record["reseed"]["reason"]


def _with_learning(settings: dict, **learning: object) -> dict:
    return {**settings, "config": {**settings["config"], "learning": learning}}


def test_arms_that_differ_only_in_the_closing_turn_share_the_install(tmp_path):
    """Arms of one item run in turn on one lane (cold, p1, p2) and only p1 keeps its closing turn: the setting is
    applied by every row's own controller install, so it must not make the lane's install look like another one."""
    lane = Lane(tmp_path)
    settings = real_settings(tmp_path)
    lane.row(**RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, _with_learning(settings, closing_turn=False))))
    code, record = lane.row(**RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, settings)))
    assert code == 0, record
    assert "--warm-lane" in lane.calls[1], record["reseed"]
    code, record = lane.row(
        **RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, _with_learning(settings, closing_turn=False)))
    )
    assert code == 0, record
    assert "--warm-lane" in lane.calls[2], record["reseed"]
    # The row's own controller still gets the setting: its config overlay carries it.
    overlay = json.loads((tmp_path / "exec3" / "operator-config.yaml").read_text())
    assert overlay["learning"] == {"closing_turn": False}


def test_any_other_learning_setting_is_still_install_identity(tmp_path):
    lane = Lane(tmp_path)
    settings = real_settings(tmp_path)
    lane.row(**RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, settings)))
    code, record = lane.row(
        **RESEED, BENCH_OPERATOR=str(operator_file(tmp_path, _with_learning(settings, replay="off")))
    )
    assert code == 0, record
    assert "--warm-lane" not in lane.calls[1]
    assert "config_digest" in record["reseed"]["reason"]


def test_the_same_agent_configuration_is_reseeded(tmp_path):
    lane = Lane(tmp_path)
    settings = real_settings(tmp_path)
    operator = str(operator_file(tmp_path, settings))
    lane.row(**RESEED, BENCH_OPERATOR=operator)
    code, record = lane.row(**RESEED, BENCH_OPERATOR=operator)
    assert code == 0, record
    assert "--warm-lane" in lane.calls[1]


def test_without_the_host_switch_every_row_installs_fresh_and_the_record_says_so(tmp_path):
    lane = Lane(tmp_path)
    lane.row()
    code, record = lane.row()
    assert code == 0, record
    assert all("--warm-lane" not in argv for argv in lane.calls)
    assert record["reseed"]["attempted"] is False
    assert "not enabled" in record["reseed"]["reason"]


def test_the_grade_counts_reseeded_rows_and_fallbacks_with_their_reason(tmp_path):
    lane = Lane(tmp_path)
    _, fresh = lane.row(**RESEED)
    lane.receipt = {"reused": True, "phase_seconds": {}}
    _, reseeded = lane.row(**RESEED)
    lane.receipt = {"reused": False, "phase_seconds": {}, "fallback_reason": "reseed:SwapError"}
    _, fell_back = lane.row(**RESEED)
    grades = [workload.grade_line(r) for r in (reseeded, fell_back, fresh)]
    assert [g["reseed"] for g in grades] == ["reseeded", "fallback", "fresh"]
    assert grades[1]["reseed_reason"] == "reseed:SwapError"
    assert grades[2]["reseed_reason"]


def test_a_fallback_row_carries_the_lanes_own_refusal_text(tmp_path):
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    lane.receipt = {
        "reused": False,
        "phase_seconds": {},
        "fallback_reason": "inspect:WarmLaneRefusal",
        "detail": "previous attempt did not finish cleanly: previous row injected and not verified recovered: p",
    }
    _, record = lane.row(**RESEED)
    assert record["reseed"]["reason"] == (
        "inspect:WarmLaneRefusal: previous attempt did not finish cleanly: "
        "previous row injected and not verified recovered: p"
    )


def test_with_the_switch_on_every_row_recovers_at_its_end_the_first_row_of_a_lane_included(tmp_path):
    """The first row installs fresh (no warm flag) and would defer its recovery to the next row's Tier-2; a warm next
    row has none, so every row of a reseeding host recovers and verifies in the row."""
    lane = Lane(tmp_path)
    lane.row(**RESEED)
    lane.row(**RESEED)
    assert "--warm-lane" not in lane.calls[0]
    assert all("--end-of-row-recovery" in argv for argv in lane.calls)


def test_with_the_switch_off_a_row_keeps_the_old_deferral(tmp_path):
    lane = Lane(tmp_path)
    lane.row()
    assert "--end-of-row-recovery" not in lane.calls[0]
