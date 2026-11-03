"""A setup failure remains a canonical non-injected attempt."""

from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.records import load_records


def test_rehearsal_failure_records_every_requested_slot(tmp_path, monkeypatch):
    args = SimpleNamespace(
        run_dir=tmp_path,
        run_id="synthetic",
        agent="sdo",
        stream=None,
        problem=["first", "second"],
        incidents=3,
        slot_offset=4,
        model="synthetic",
        provider="codex",
        reasoning_effort="medium",
    )
    monkeypatch.setattr(cli.FastloopEnvironment, "load", lambda _: SimpleNamespace(run_dir=tmp_path))

    def fail(*_args, **_kwargs):
        raise ValueError("no-fault rehearsal: traffic failed")

    monkeypatch.setattr(cli, "_execute_owned", fail, raising=False)
    with pytest.raises(ValueError, match="traffic failed"):
        cli._run_owned(args)
    records = load_records(tmp_path / "results/synthetic/incidents.jsonl")
    assert [(r.index, r.problem_id) for r in records] == [(4, "first"), (5, "second"), (6, "first")]
    assert all(r.injection.stage == "pre_injection" and "traffic failed" in r.error for r in records)


def test_existing_incident_error_is_not_replaced_by_wrapper(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop.records import IncidentRecord, NotInjected, append_record

    args = SimpleNamespace(run_dir=tmp_path, run_id="synthetic", agent="sdo")
    path = tmp_path / "results/synthetic/incidents.jsonl"
    append_record(
        path,
        IncidentRecord(
            run_id="synthetic",
            index=0,
            agent="sdo",
            model="synthetic",
            problem_id="p",
            injection=NotInjected(stage="pre_injection"),
            error="original",
        ),
    )
    monkeypatch.setattr(cli.FastloopEnvironment, "load", lambda _: SimpleNamespace(run_dir=tmp_path))

    def fail(*_args, **_kwargs):
        raise RuntimeError("outer failure")

    monkeypatch.setattr(cli, "_execute_owned", fail)
    with pytest.raises(RuntimeError, match="outer failure"):
        cli._run_owned(args)
    assert [record.error for record in load_records(path)] == ["original"]
