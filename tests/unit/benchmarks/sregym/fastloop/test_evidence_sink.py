"""The fastloop lane's evidence sink: required for the SDO arm, copied out before the lane is deleted."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import cli, evidence_sink
from benchmarks.sregym.fastloop.environment import FastloopEnvironment

if TYPE_CHECKING:
    from pathlib import Path


def _lane(tmp_path: Path, *, sink_root: Path | None) -> FastloopEnvironment:
    lane = FastloopEnvironment(
        run_dir=tmp_path / "lane",
        cluster="eval-w0",
        kubeconfig=tmp_path / "config",
        namespace="toy",
        application="toy",
        workspace=tmp_path / "lane" / "application_workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        evidence_root=sink_root,
    )
    lane.workspace.mkdir(parents=True)
    lane.save()
    return lane


def _sink_run(lane: FastloopEnvironment, results_dir: Path, *, session: str = "s") -> Path:
    """The host sink run directory of ``results_dir``: one incident with a session file and an investigator's."""
    assert lane.evidence_root is not None
    run = lane.evidence_root / evidence_sink.lane_evidence_run_id(lane, results_dir)
    run.mkdir(parents=True)
    (run / "layout.json").write_text(json.dumps({"schema": "sdo.evidence/v1", "run_id": run.name, "created_at": "x"}))
    for name in ("a" * 32, "b" * 32):
        directory = run / "incidents" / name / "claude" / "projects" / "p"
        directory.mkdir(parents=True)
        (directory / f"{session}.jsonl").write_text("{}\n")
    return run


def test_the_sdo_arm_refuses_a_lane_without_a_sink_and_a_waiver_is_recorded(tmp_path: Path) -> None:
    lane = _lane(tmp_path, sink_root=None)
    results = lane.run_dir / "results" / "r1"
    with pytest.raises(evidence_sink.EvidenceSinkError, match="--evidence-root"):
        evidence_sink.require_evidence_sink(lane, results)
    assert not evidence_sink.waived(results)
    evidence_sink.require_evidence_sink(lane, results, waived=True)
    assert evidence_sink.waived(results)
    with_sink = _lane(tmp_path / "other", sink_root=tmp_path / "sink")
    evidence_sink.require_evidence_sink(with_sink, with_sink.run_dir / "results" / "r1")  # no raise


def test_run_refuses_the_sdo_arm_on_a_lane_without_a_sink(tmp_path: Path) -> None:
    lane = _lane(tmp_path, sink_root=None)
    with pytest.raises(SystemExit, match="evidence sink"):
        cli.main(["run", "--run-dir", str(lane.run_dir), "--agent", "sdo", "--run-id", "r1"])


def test_copy_out_copies_every_session_verifies_and_writes_a_receipt(tmp_path: Path) -> None:
    lane = _lane(tmp_path, sink_root=tmp_path / "sink")
    results = lane.run_dir / "results" / "r1"
    results.mkdir(parents=True)
    _sink_run(lane, results)
    copy = evidence_sink.copy_out_run(lane, results)
    assert (copy.incidents, copy.files) == (2, 3)
    assert sorted(p.name for p in (results / "evidence" / "incidents").iterdir()) == ["a" * 32, "b" * 32]
    receipt: dict[str, Any] = json.loads((results / "evidence-copy.json").read_text())
    assert receipt["schema"] == evidence_sink.RECEIPT_SCHEMA
    assert receipt["incidents"] == 2


def test_copy_out_fails_loudly_without_a_sink_or_its_run_directory(tmp_path: Path) -> None:
    no_sink = _lane(tmp_path / "a", sink_root=None)
    with pytest.raises(evidence_sink.EvidenceSinkError, match="no evidence sink"):
        evidence_sink.copy_out_run(no_sink, no_sink.run_dir / "results" / "r1")
    lane = _lane(tmp_path / "b", sink_root=tmp_path / "sink")
    results = lane.run_dir / "results" / "r1"
    results.mkdir(parents=True)
    with pytest.raises(evidence_sink.EvidenceSinkError, match="no sink run directory"):
        evidence_sink.copy_out_run(lane, results)


def test_settle_copies_unchanged_runs_once_and_reports_a_failed_copy(tmp_path: Path) -> None:
    lane = _lane(tmp_path, sink_root=tmp_path / "sink")
    results = lane.run_dir / "results" / "r1"
    results.mkdir(parents=True)
    run = _sink_run(lane, results)
    assert evidence_sink.settle_lane_evidence(lane) == []
    assert (results / "evidence-copy.json").is_file()
    first = (results / "evidence-copy.json").read_text()
    assert evidence_sink.settle_lane_evidence(lane) == []
    assert (results / "evidence-copy.json").read_text() == first, "an unchanged run is not copied again"
    late = run / "incidents" / ("a" * 32) / "claude" / "projects" / "p" / "late.jsonl"
    late.write_text("{}\n")
    future = late.stat().st_mtime + 60
    os.utime(late, (future, future))
    assert evidence_sink.settle_lane_evidence(lane) == []
    assert (results / "evidence" / "incidents" / ("a" * 32) / "claude" / "projects" / "p" / "late.jsonl").is_file()
    (run / "layout.json").write_text("{}")  # the sink becomes unreadable
    os.utime(run / "layout.json", (future + 60, future + 60))
    failures = evidence_sink.settle_lane_evidence(lane)
    assert len(failures) == 1
    assert failures[0].startswith("r1: ")


def test_a_lane_without_a_sink_has_nothing_to_settle(tmp_path: Path) -> None:
    assert evidence_sink.settle_lane_evidence(_lane(tmp_path, sink_root=None)) == []


def test_down_keeps_the_lane_when_the_sink_cannot_be_copied_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = _lane(tmp_path, sink_root=tmp_path / "sink")
    (lane.run_dir / "results" / "r1").mkdir(parents=True)
    _sink_run(lane, lane.run_dir / "results" / "r1")
    (
        lane.evidence_root / evidence_sink.lane_evidence_run_id(lane, lane.run_dir / "results" / "r1") / "layout.json"
    ).write_text("{}")  # type: ignore[operator]
    deleted: list[object] = []
    monkeypatch.setattr(cli.subprocess, "run", lambda argv, **_kw: deleted.append(argv))
    import benchmarks.sregym.adapter as adapter

    monkeypatch.setattr(adapter, "teardown", lambda *_a, **_kw: deleted.append("teardown") or [])
    assert cli.main(["down", "--run-dir", str(lane.run_dir), "--delete-cluster"]) == 1
    assert deleted == [], "nothing is deleted while the sink copy is unverified"


def test_down_deletes_the_cluster_only_after_the_final_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lane = _lane(tmp_path, sink_root=tmp_path / "sink")
    results = lane.run_dir / "results" / "r1"
    results.mkdir(parents=True)
    run = _sink_run(lane, results)
    order: list[str] = []
    import benchmarks.sregym.adapter as adapter
    from benchmarks.sregym.fastloop import image_delivery

    def teardown(*_a: object, **_kw: object) -> list[str]:
        # stopping the controller runs the sink's final pass: a transcript lands after the first copy
        order.append("teardown")
        late = run / "incidents" / ("a" * 32) / "claude" / "projects" / "p" / "final.jsonl"
        late.write_text("{}\n")
        later = late.stat().st_mtime + 120
        os.utime(late, (later, later))
        return []

    monkeypatch.setattr(adapter, "teardown", teardown)
    monkeypatch.setattr(image_delivery, "remove_registry", lambda _cluster: None)

    def run_command(argv: list[str], **_kw: object) -> None:
        order.append("delete cluster" if argv[:3] == ["kind", "delete", "cluster"] else str(argv))
        assert (results / "evidence" / "incidents" / ("a" * 32) / "claude" / "projects" / "p" / "final.jsonl").is_file()

    monkeypatch.setattr(cli.subprocess, "run", run_command)
    lane.state_path.write_text("{}")
    assert cli.main(["down", "--run-dir", str(lane.run_dir), "--delete-cluster"]) == 0
    assert order == ["teardown", "delete cluster"]


def test_copy_out_merges_run_level_directories_and_is_idempotent(tmp_path: Path) -> None:
    lane = _lane(tmp_path, sink_root=tmp_path / "sink")
    results = lane.run_dir / "results" / "r1"
    results.mkdir(parents=True)
    run = _sink_run(lane, results)
    (run / "telemetry").mkdir()
    (run / "telemetry" / "first.json").write_text("{}\n")
    evidence_sink.copy_out_run(lane, results)
    destination = results / "evidence"
    assert (destination / "telemetry" / "first.json").is_file()

    # The sink grew: a new file in the existing run-level directory, a grown file, and a new directory.
    (run / "telemetry" / "second.json").write_text('{"a": 1}\n')
    (run / "telemetry" / "first.json").write_text('{"grown": true}\n')
    (run / "extra").mkdir()
    (run / "extra" / "x.txt").write_text("x\n")
    (destination / "telemetry" / "destination-only.txt").write_text("keep\n")
    receipt = evidence_sink.copy_out_run(lane, results)

    assert (destination / "telemetry" / "second.json").read_text() == '{"a": 1}\n'
    assert (destination / "telemetry" / "first.json").read_text() == '{"grown": true}\n'
    assert (destination / "extra" / "x.txt").is_file()
    assert (destination / "telemetry" / "destination-only.txt").read_text() == "keep\n"
    again = evidence_sink.copy_out_run(lane, results)
    assert (again.files, again.bytes) == (receipt.files, receipt.bytes)
    assert evidence_sink.settle_lane_evidence(lane) == []
