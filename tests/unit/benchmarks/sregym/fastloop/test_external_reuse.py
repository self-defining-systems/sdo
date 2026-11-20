"""An external-agent row reuses the lane's app only when the previous row left it provably clean."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.records import IncidentRecord, NotInjected, OracleVerdict

if TYPE_CHECKING:
    from pathlib import Path


def _row(**overrides: object) -> IncidentRecord:
    fields: dict[str, object] = {
        "run_id": "prev",
        "index": 0,
        "agent": "external",
        "problem_id": "p",
        "model": "m",
        "injection": NotInjected(stage="pre_injection"),
        "full_cleanup_verified": True,
        "recovery_oracle": OracleVerdict(kind="health-check", success=True),
    }
    fields.update(overrides)
    return IncidentRecord.model_validate(fields)


def _write(run_dir: Path, record: IncidentRecord) -> None:
    path = run_dir / "results" / record.run_id / "incidents.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(record.model_dump_json() + "\n")
    os.utime(path, (1000, 1000))


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_change: object = None):
    monkeypatch.setattr(cli, "_deployed_source_change", lambda *_: source_change)
    monkeypatch.setattr(cli, "_recapture_application_baseline", lambda *_: None)
    monkeypatch.setattr(cli, "_record_deployed_source", lambda *_: None)
    environment = SimpleNamespace(run_dir=tmp_path, deploy_problem="fixture", baseline_path=tmp_path / "b")
    calls: list[str] = []
    worker = SimpleNamespace(request=lambda op, **kwargs: calls.append(op))
    decision = cli.deploy_with_setup_proof(
        environment, worker, tmp_path / "results" / "cur", allow_reuse=False, revision="r", external=True
    )
    return decision, calls


def test_reuses_when_previous_row_clean_and_source_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path, _row())
    decision, calls = _setup(tmp_path, monkeypatch)
    assert decision["reused"] is True
    assert decision["previous_run_id"] == "prev"
    assert "deploy" not in calls
    assert json.loads((tmp_path / "results" / "cur" / "setup-proof.json").read_text())["reused"] is True


@pytest.mark.parametrize(
    ("previous", "source_change", "reason"),
    [
        (None, None, "no_previous_row"),
        ({"recovery_error": "x"}, None, "previous_row_recovery_error"),
        ({"full_cleanup_verified": False}, None, "previous_row_full_cleanup_unverified"),
        ({"recovery_oracle": None}, None, "previous_row_recovery_oracle_not_passed"),
        (
            {"recovery_oracle": OracleVerdict(kind="health-check", success=False)},
            None,
            "previous_row_recovery_oracle_not_passed",
        ),
        ({"error": "boom"}, None, "previous_row_errored"),
        ({"agent": "sdo"}, None, "previous_row_was_sdo"),
        ({}, "other source", "deployed_source_differs"),
    ],
)
def test_redeploys_when_any_condition_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    previous: dict[str, object] | None,
    source_change: object,
    reason: str,
) -> None:
    if previous is not None:
        _write(tmp_path, _row(**previous))
    decision, calls = _setup(tmp_path, monkeypatch, source_change)
    assert decision["reused"] is False
    assert any(item.startswith(reason) for item in decision["reasons"])
    assert calls == ["deploy"]


def test_unknown_source_identity_redeploys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path, _row())

    def broken(*_: object) -> None:
        raise OSError("no git")

    _setup_calls: list[str] = []
    monkeypatch.setattr(cli, "_deployed_source_change", broken)
    monkeypatch.setattr(cli, "_recapture_application_baseline", lambda *_: None)
    monkeypatch.setattr(cli, "_record_deployed_source", lambda *_: None)
    environment = SimpleNamespace(run_dir=tmp_path, deploy_problem="fixture", baseline_path=tmp_path / "b")
    worker = SimpleNamespace(request=lambda op, **kwargs: _setup_calls.append(op))
    decision = cli.deploy_with_setup_proof(
        environment, worker, tmp_path / "results" / "cur", allow_reuse=False, revision="r", external=True
    )
    assert decision["reused"] is False
    assert _setup_calls == ["deploy"]
