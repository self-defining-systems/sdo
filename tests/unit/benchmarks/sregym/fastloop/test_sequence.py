"""A sequence may adopt durable completed rows, but never reinject an uncertain row."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, OracleVerdict, append_record
from benchmarks.sregym.fastloop.sequence import SequencePlan, SequenceRow, run_sequence

if TYPE_CHECKING:
    from pathlib import Path


def repository(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=t@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "fresh",
        ],
        check=True,
    )
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


@pytest.fixture
def plan(tmp_path: Path) -> SequencePlan:
    code = tmp_path / "code"
    sha = repository(code)
    memory = tmp_path / "memory"
    seed = repository(memory)
    environment = tmp_path / "environment"
    FastloopEnvironment(
        run_dir=environment,
        cluster="luna-pilot-51",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=memory,
        sregym_dir=code / "third_party/sregym",
        private_tmp=tmp_path / "private",
        seed_commit=seed,
    ).save()
    return SequencePlan(
        run_id="phase4-51",
        code_sha=sha,
        code_root=code,
        environment_dir=environment,
        journal_dir=tmp_path / "journal",
        lane="51",
        rows=[
            SequenceRow(slot=17, problem_id="problem", encounter="first"),
            SequenceRow(slot=17, problem_id="problem", encounter="repeat"),
        ],
    )


def finish(plan: SequencePlan, index: int, path: Path) -> None:
    now = datetime.now(timezone.utc)
    artifact = path.parent / "native"
    artifact.mkdir(parents=True, exist_ok=True)
    (artifact / "sdo_production_receipt_strict.json").write_text(
        json.dumps(
            {
                "incident_id": f"incident-{index}",
                "native_ledger": {
                    "incident_id": f"incident-{index}",
                    "acknowledged": True,
                    "outcome_commit": plan.code_sha,
                },
            }
        )
    )
    append_record(
        path,
        IncidentRecord(
            run_id=f"{plan.run_id}-{index:03d}",
            index=index,
            agent="sdo",
            model="gpt-6-luna",
            problem_id=plan.rows[index].problem_id,
            incident_id=f"incident-{index}",
            injection={"state": "injected", "started_at": now, "finished_at": now},
            oracle=OracleVerdict(kind="composition", success=True),
            recovery_oracle=OracleVerdict(kind="health-check", success=True),
            recovery_finished_at=now,
            full_cleanup_verified=True,
            cleanup_passes_verified=2,
            reflection_finished_at=now,
            artifacts_dir=str(artifact),
        ),
    )


def test_restart_adopts_row_completed_before_journal_write_and_skips_finished(plan: SequencePlan) -> None:
    calls = []

    def interrupted(p: SequencePlan, index: int, path: Path) -> None:
        calls.append(index)
        finish(p, index, path)
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_sequence(plan, execute=interrupted)
    assert calls == [0]

    def execute(p: SequencePlan, index: int, path: Path) -> None:
        calls.append(index)
        finish(p, index, path)

    run_sequence(plan, execute=execute)
    run_sequence(plan, execute=execute)
    assert calls == [0, 1]
    for index in range(2):
        row = json.loads((plan.journal_dir / f"{index:03d}" / "finished.json").read_text())
        assert row["code_sha"] == plan.code_sha
        assert row["accepted_before"] == row["accepted_after"]
        assert row["lane"] == "51"
        assert row["seed_commit"]


def test_restart_holds_incomplete_row_without_reinjection(plan: SequencePlan) -> None:
    def interrupted(_p: SequencePlan, _index: int, _path: Path) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_sequence(plan, execute=interrupted)
    with pytest.raises(RuntimeError, match="incomplete.*never reinject"):
        run_sequence(plan, execute=lambda *_args: pytest.fail("reinjected uncertain row"))


@pytest.mark.parametrize("updates", [{"code_sha": "b" * 40}, {"load_gate": 64}])
def test_changed_plan_cannot_adopt_old_evidence(plan: SequencePlan, updates: dict[str, object]) -> None:
    run_sequence(plan, execute=finish)
    changed = plan.model_copy(update=updates)
    with pytest.raises(ValueError, match="sequence identity"):
        run_sequence(changed, execute=finish)


@pytest.mark.parametrize("field", ["infra_error", "recovery_error"])
def test_terminal_infrastructure_failure_is_held(plan: SequencePlan, field: str) -> None:
    def execute(p: SequencePlan, index: int, path: Path) -> None:
        finish(p, index, path)
        record = json.loads(path.read_text())
        record[field] = "broken plumbing"
        path.write_text(json.dumps(record) + "\n")

    with pytest.raises(RuntimeError, match="recovery and learning"):
        run_sequence(plan, execute=execute)


def test_repeat_must_follow_its_own_first_on_the_lane(plan: SequencePlan) -> None:
    with pytest.raises(ValueError, match="first.*repeat"):
        SequencePlan.model_validate({**plan.model_dump(), "rows": list(reversed(plan.rows))})


def test_unacknowledged_native_memory_blocks_repeat(plan: SequencePlan) -> None:
    def execute(p: SequencePlan, index: int, path: Path) -> None:
        finish(p, index, path)
        receipt = path.parent / "native/sdo_production_receipt_strict.json"
        value = json.loads(receipt.read_text())
        value["native_ledger"]["acknowledged"] = False
        receipt.write_text(json.dumps(value))

    with pytest.raises(RuntimeError, match="acknowledged"):
        run_sequence(plan, execute=execute)


@pytest.mark.parametrize("updates", [{"full_cleanup_verified": False}, {"cleanup_passes_verified": 1}])
def test_partial_cleanup_cannot_advance_the_sequence(plan: SequencePlan, updates: dict) -> None:
    def execute(p: SequencePlan, index: int, path: Path) -> None:
        finish(p, index, path)
        record = json.loads(path.read_text())
        record.update(updates)
        path.write_text(json.dumps(record) + "\n")

    with pytest.raises(RuntimeError, match="recovery and learning"):
        run_sequence(plan, execute=execute)


def test_native_ledger_must_belong_to_the_row_incident(plan: SequencePlan) -> None:
    def execute(p: SequencePlan, index: int, path: Path) -> None:
        finish(p, index, path)
        receipt = path.parent / "native/sdo_production_receipt_strict.json"
        value = json.loads(receipt.read_text())
        value["native_ledger"]["incident_id"] = "another-incident"
        receipt.write_text(json.dumps(value))

    with pytest.raises(RuntimeError, match="acknowledged"):
        run_sequence(plan, execute=execute)


@pytest.mark.parametrize(("configured_gate", "expected_gate"), [(None, 52), (36, 36), (64, 64)])
def test_busy_host_waits_before_starting_the_guarded_native_runner(
    plan: SequencePlan, monkeypatch, configured_gate: int | None, expected_gate: int
) -> None:
    from benchmarks.sregym.fastloop import sequence
    from benchmarks.sregym.fastloop.cli import build_parser

    if configured_gate is not None:
        plan = SequencePlan.model_validate({**plan.model_dump(), "load_gate": configured_gate})
    loads = iter([(expected_gate + 0.5, 0, 0), (expected_gate, 0, 0), (expected_gate - 0.5, 0, 0)])
    sleeps = []
    commands = []
    monkeypatch.setattr(sequence.os, "getloadavg", lambda: next(loads))
    monkeypatch.setattr("time.sleep", sleeps.append)

    def run(command, **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(sequence.subprocess, "run", run)
    results = plan.environment_dir / "results" / "phase4-51-000" / "incidents.jsonl"
    sequence.execute_row(plan, 0, results)
    assert len(sleeps) == 2, "at-or-above admission must wait, below must proceed"
    assert [plan.coord, "gate", str(expected_gate), "--lease", plan.lease] in commands
    assert commands.index([plan.coord, "gate", str(expected_gate), "--lease", plan.lease]) < len(commands) - 1
    command = commands[-1]
    boundary = command.index("--")
    assert command[boundary + 1] == "benchmarks.sregym.fastloop"
    parsed = build_parser().parse_args(command[boundary + 2 :])
    assert parsed.require_fault_tracking
    assert parsed.require_full_cleanup
    assert parsed.cleanup_passes == 2
    assert parsed.provider == "codex"
    assert parsed.model == "gpt-6-luna"


@pytest.mark.parametrize("oracle_success", [False, True])
def test_row_start_tier2_does_not_gate_finished_rows_on_recovery(plan: SequencePlan, oracle_success: bool) -> None:
    def execute(p: SequencePlan, index: int, path: Path) -> None:
        finish(p, index, path)
        record = json.loads(path.read_text())
        record.update(
            oracle={"kind": "composition", "success": oracle_success},
            cleanup_skipped_reason="row_start_tier2",
            full_cleanup_verified=False,
            cleanup_passes_verified=0,
            recovery_finished_at=None,
            recovery_oracle=None,
        )
        path.write_text(json.dumps(record) + "\n")

    run_sequence(plan, execute=execute)
    for index in range(2):
        finished = json.loads((plan.journal_dir / f"{index:03d}" / "finished.json").read_text())
        assert finished["oracle"]["success"] is oracle_success
    assert (plan.journal_dir / "complete.json").is_file()


def test_pre_injection_observation_failure_is_retryable_not_a_hold(plan):
    from benchmarks.sregym.fastloop.records import NotInjected
    from benchmarks.sregym.fastloop.sequence import NeverInjectedRetry, _terminal

    path = plan.environment_dir / "results" / f"{plan.run_id}-000" / "incidents.jsonl"
    append_record(
        path,
        IncidentRecord(
            run_id=path.parent.name,
            index=0,
            problem_id="problem",
            agent="sdo",
            model="gpt-6-luna",
            injection=NotInjected(stage="pre_injection"),
            infra_error="never_injected_retry",
            error="ObservationUnavailable: temporary",
        ),
    )
    with pytest.raises(NeverInjectedRetry, match="never_injected_retry"):
        _terminal(plan, 0, path)


def test_sequence_restart_retries_only_proven_never_injected_attempt(plan):
    from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents
    from benchmarks.sregym.fastloop.runtime_observation import ObservationUnavailable
    from benchmarks.sregym.fastloop.sequence import NeverInjectedRetry

    calls = []

    class Agent:
        name = "sdo"
        model = "gpt-6-luna"

        def resolve(self, *_args):
            raise ObservationUnavailable("transient baseline")

        def close(self):
            pass

    class Driver:
        def inject(self, *_args):
            pytest.fail("injected during unavailable baseline")

        def recover(self):
            pytest.fail("recovered a never-injected row")

    def unavailable(p, index, path):
        calls.append(path)
        run_incidents(
            Agent(),
            Driver(),
            LoopConfig(
                run_id=path.parent.name,
                problems=(p.rows[index].problem_id,),
                incidents=1,
                results_path=path,
                slot_offset=index,
            ),
        )

    with pytest.raises(NeverInjectedRetry):
        run_sequence(plan, execute=unavailable)
    assert json.loads((plan.journal_dir / "000/never-injected-0.json").read_text())["status"] == "never_injected_retry"

    def execute(p, index, path):
        calls.append(path)
        finish(p, index, path)
        # Synthetic helper predates per-attempt identities.
        if "retry" in path.parent.name:
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]["run_id"] = path.parent.name
            path.write_text(json.dumps(rows[0]) + "\n")

    run_sequence(plan, execute=execute)
    assert calls[0] != calls[1]
    assert calls[0].exists()  # Preserve failed-attempt evidence.
    assert "retry-1" in str(calls[1])
    assert (plan.journal_dir / "complete.json").exists()


def test_interrupted_retry_attempt_still_holds_uncertain_work(plan):
    from benchmarks.sregym.fastloop.records import NotInjected
    from benchmarks.sregym.fastloop.sequence import NeverInjectedRetry

    def unavailable(p, index, path):
        append_record(
            path,
            IncidentRecord(
                run_id=path.parent.name,
                index=index,
                problem_id="problem",
                agent="sdo",
                model="gpt-6-luna",
                injection=NotInjected(stage="pre_injection"),
                infra_error="never_injected_retry",
            ),
        )

    with pytest.raises(NeverInjectedRetry):
        run_sequence(plan, execute=unavailable)

    def interrupted(*_args):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_sequence(plan, execute=interrupted)
    with pytest.raises(RuntimeError, match="incomplete.*never reinject"):
        run_sequence(plan, execute=lambda *_args: pytest.fail("reinjected uncertain retry attempt"))


@pytest.mark.parametrize("gate", [0, -1, 52.5, True])
def test_sequence_rejects_invalid_load_gate(plan: SequencePlan, gate: object) -> None:
    with pytest.raises(ValueError, match="load_gate"):
        SequencePlan.model_validate({**plan.model_dump(), "load_gate": gate})


def test_sequence_resumes_saved_plan_without_new_default_gate(plan: SequencePlan) -> None:
    run_sequence(plan, execute=finish)
    saved = plan.journal_dir / "sequence.json"
    identity = json.loads(saved.read_text())
    identity.pop("load_gate", None)
    saved.write_text(json.dumps(identity))
    run_sequence(plan, execute=lambda *_args: pytest.fail("re-executed finished row"))
