"""The host evidence sink as fuzzops-bench reads it: layout, ``agents_ran``, transcript coverage, interval copy."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import sink

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
T1 = datetime(2026, 11, 12, 10, 30, tzinfo=UTC)


def _run(tmp: Path, schema: str = "sdo.evidence/v1") -> Path:
    run = tmp / "e-l-1"
    run.mkdir()
    (run / "layout.json").write_text(json.dumps({"schema": schema, "run_id": "e-l-1", "created_at": "x"}))
    return run


def _incident(run: Path, name: str, ledger: dict[str, Any] | None, rollouts: int = 0) -> Path:
    directory = run / "incidents" / name
    directory.mkdir(parents=True)
    if ledger is not None:
        (directory / "ledger.json").write_text(json.dumps(ledger))
    for i in range(rollouts):
        deep = directory / "claude" / "projects" / f"p{i}"
        deep.mkdir(parents=True)
        (deep / f"rollout-{i}.jsonl").write_text("{}\n")
    return directory


def _closure(**result: Any) -> dict[str, Any]:
    return {"detected_at": "2026-11-12T10:01:00.123456789Z", "result": result}


# ---- environment ---------------------------------------------------------------------------------------------


def test_install_flags_use_the_node_path_and_run_id() -> None:
    env = {"BENCH_EVIDENCE_NODE": "/bench-evidence", "BENCH_EVIDENCE_RUN": "exp-lane-7"}
    assert sink.install_flags(env) == [
        "--evidence-sink-host-path",
        "/bench-evidence",
        "--evidence-run-id",
        "exp-lane-7",
    ]


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"BENCH_EVIDENCE_RUN": "r"}, "BENCH_EVIDENCE_NODE"),
        ({"BENCH_EVIDENCE_NODE": "/n"}, "BENCH_EVIDENCE_RUN"),
        ({"BENCH_EVIDENCE_NODE": "rel", "BENCH_EVIDENCE_RUN": "r"}, "absolute"),
        ({"BENCH_EVIDENCE_NODE": "/n", "BENCH_EVIDENCE_RUN": "../x"}, "run ID"),
    ],
)
def test_install_flags_refuse_a_lane_without_a_sink(env: dict[str, str], message: str) -> None:
    with pytest.raises(sink.SinkError, match=message):
        sink.install_flags(env)


def test_host_run_dir_joins_the_host_directory_and_run(tmp_path: Path) -> None:
    env = {"BENCH_EVIDENCE_HOST": str(tmp_path), "BENCH_EVIDENCE_RUN": "r1"}
    assert sink.host_run_dir(env) == tmp_path / "r1"
    with pytest.raises(sink.SinkError, match="BENCH_EVIDENCE_HOST"):
        sink.host_run_dir({"BENCH_EVIDENCE_RUN": "r1"})


# ---- agents_ran ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ledger",
    [
        {"incident_id": "i"},  # no closure yet: an agent may be running
        {"closure": {"detected_at": "x"}},  # closure without result or dispatch error
        {"closure": _closure(responder_session_id="0199-abc")},
        {"closure": _closure(resolution_path="responder")},
        {"closure": _closure(usage={"llm_calls": 2})},
        {"closure": _closure(resolution_path="deterministic"), "outcome_document": {"usage": {"input_tokens": "9"}}},
        {"closure": _closure(resolution_path="deterministic"), "reflection_dispatch_at": "2026-11-12T10:09:00Z"},
        {"closure": _closure(resolution_path="deterministic"), "reflection_phase": "REFLECTION_PHASE_DISPATCH"},
        {
            "closure": _closure(resolution_path="deterministic"),
            "reflection_decision": {"learning_decision": "no_change"},
        },
        {"closure": _closure(resolution_path="deterministic"), "reflectionResults": [{"usage": {"outputTokens": 3}}]},
        {"closure": _closure(resolutionPath="responder")},
    ],
)
def test_agents_ran_unless_the_ledger_proves_otherwise(ledger: dict[str, Any]) -> None:
    assert sink.agents_ran(ledger)


@pytest.mark.parametrize(
    "ledger",
    [
        {"closure": _closure(resolution_path="deterministic", usage={})},
        {"closure": {"detected_at": "x", "dispatch_error": "no worker"}},
        {
            "closure": _closure(resolution_path="deterministic"),
            "outcome_document": {"resolution_path": "deterministic", "reflection_skip_reason": "no_session"},
            "reflection_phase": "REFLECTION_PHASE_DONE",
            "reflection_results": [{"learning_decision": "failed", "failure_reason": "session_unavailable"}],
        },
    ],
)
def test_no_agent_ran(ledger: dict[str, Any]) -> None:
    assert not sink.agents_ran(ledger)


# ---- transcripts verdict -------------------------------------------------------------------------------------


def test_verdict_needs_the_layout(tmp_path: Path) -> None:
    assert sink.transcripts_verdict(tmp_path / "absent").startswith("no sink: ")
    assert "not sdo.evidence/v1" in sink.transcripts_verdict(_run(tmp_path, schema="other"))


def test_verdict_checks_every_incident_with_a_ledger(tmp_path: Path) -> None:
    run = _run(tmp_path)
    responded = {"closure": _closure(responder_session_id="s"), "outcome_document": {"classification": "success"}}
    _incident(run, "a" * 32, responded, rollouts=2)
    _incident(run, "b" * 32, None)  # no ledger yet: not checked
    _incident(run, "c" * 32, {"closure": _closure(resolution_path="deterministic")})  # no agent ran
    assert sink.transcripts_verdict(run) == ""
    _incident(run, "d" * 32, responded)
    _incident(run, "e" * 32, {"incident_id": "open"})
    assert sink.transcripts_verdict(run) == f"transcripts missing: 2 ({'d' * 8}, {'e' * 8})"


# ---- interval copy -------------------------------------------------------------------------------------------


def _ledger(incident: str, detected: str, acknowledged: str | None = None) -> dict[str, Any]:
    ledger: dict[str, Any] = {
        "incident_id": incident,
        "closure": {"detected_at": detected, "result": {"responder_session_id": "s"}},
        "outcome_document": {"incident_id": incident},
    }
    if acknowledged:
        ledger["acknowledged_at"] = acknowledged
    return ledger


def _age(directory: Path, at: datetime) -> None:
    for path in [directory, *directory.rglob("*")]:
        os.utime(path, (at.timestamp(), at.timestamp()))


def test_copy_interval_takes_incidents_detected_in_or_open_during_the_interval(tmp_path: Path) -> None:
    run = _run(tmp_path)
    old = datetime(2026, 11, 11, tzinfo=UTC)
    inside = _incident(run, "1" * 32, _ledger("inside", "2026-11-12T10:05:00Z"), rollouts=1)
    spanning = _incident(run, "2" * 32, _ledger("spanning", "2026-11-12T09:00:00Z"), rollouts=1)
    before = _incident(run, "3" * 32, _ledger("before", "2026-11-12T09:00:00Z", "2026-11-12T09:30:00Z"), 1)
    after = _incident(run, "4" * 32, _ledger("after", "2026-11-12T11:00:00Z"), rollouts=1)
    missing = _incident(run, "5" * 32, _ledger("missing", "2026-11-12T10:10:00Z"))
    no_ledger_old = _incident(run, "6" * 32, None, rollouts=1)
    no_ledger_new = _incident(run, "7" * 32, None, rollouts=1)
    for d in (inside, spanning, before, after, missing, no_ledger_old):
        _age(d, old)
    _age(no_ledger_new, datetime(2026, 11, 12, 10, 15, tzinfo=UTC))
    task_in = run / "lifecycle" / "judge-1"
    task_out = run / "lifecycle" / "judge-0"
    task_in.mkdir(parents=True)
    task_out.mkdir(parents=True)
    (task_in / "result.json").write_text("{}")
    (task_out / "result.json").write_text("{}")
    _age(task_out, old)
    _age(task_in, old)
    records = [
        {"kind": "incident", "id": "inside", "path": "incidents/" + "1" * 32, "sealed_at": "2026-11-12T10:20:00Z"},
        {"kind": "incident", "id": "before", "path": "incidents/" + "3" * 32, "sealed_at": "2026-11-12T09:31:00Z"},
        {"kind": "task", "id": "judge-1", "path": "lifecycle/judge-1", "sealed_at": "2026-11-12T10:10:00Z"},
    ]
    (run / "index.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records) + '{"torn')
    dest = tmp_path / "out" / "evidence"
    result = sink.copy_interval(run, dest, T0, T1)
    copied = sorted(p.name[:1] for p in (dest / "incidents").iterdir())
    assert copied == ["1", "2", "5", "7"]
    assert (dest / "lifecycle" / "judge-1" / "result.json").is_file()
    assert not (dest / "lifecycle" / "judge-0").exists()
    assert (dest / "layout.json").is_file()
    kept = [json.loads(line)["id"] for line in (dest / "index.jsonl").read_text().splitlines()]
    assert kept == ["inside", "judge-1"]
    summary = result.summary()
    assert summary["rollouts"] == 3
    assert summary["missing"] == ["missing"]
    by_id = {i["id"]: i for i in summary["incidents"]}
    assert by_id["inside"] == {"id": "inside", "outcome": True, "agents_ran": True, "rollouts": 1}
    assert by_id["7" * 32]["outcome"] is False


def test_copy_interval_refuses_a_missing_sink(tmp_path: Path) -> None:
    with pytest.raises(sink.SinkError, match="no sink"):
        sink.copy_interval(tmp_path / "none", tmp_path / "dest", T0, T1)


def test_parse_time_accepts_protojson_nanoseconds() -> None:
    assert sink.parse_time("2026-11-12T10:01:00.123456789Z") == datetime(2026, 11, 12, 10, 1, 0, 123456, tzinfo=UTC)
    assert sink.parse_time("bad") is None
