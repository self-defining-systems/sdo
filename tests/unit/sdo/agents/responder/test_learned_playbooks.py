"""The responder's first message names the learned playbooks that fit the incident, with their files."""

from __future__ import annotations

from google.protobuf import timestamp_pb2

from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.incident_opening import earlier_attempts_block, learned_playbooks_block
from sdo.contracts import EarlierAttempt, Finding, IncidentRequest, MemoryPlaybook, MemoryScript
from tests.unit.sdo.agents.prompts.cases import incident


def _readme(incident_id: str) -> str:
    return f"---\nschema_version: 1\nowner: responder\noriginating_incident: {incident_id}\n---\n# fixture\n"


def _request(*playbooks: MemoryPlaybook, findings: tuple[Finding, ...] = ()) -> IncidentRequest:
    request = incident(1)
    request.detected_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_700_000_000))
    request.ClearField("findings")
    request.memory_snapshot.ClearField("playbooks")
    request.memory_snapshot.playbooks.extend(playbooks)
    request.findings.extend(findings)
    return request


def _playbook(playbook_id: str, origin: str = "inc-old", scripts: tuple[str, ...] = ()) -> MemoryPlaybook:
    return MemoryPlaybook(
        id=playbook_id,
        markdown=_readme(origin),
        scripts=[MemoryScript(name=name, content="#!/bin/sh\n") for name in scripts],
    )


def _firing(detector_id: str, *playbooks: str) -> Finding:
    return Finding(
        detector_id=detector_id, rule_id="r", status="active", fingerprint=detector_id, playbook_ids=list(playbooks)
    )


def test_a_playbook_whose_detector_fires_is_named_with_its_readme_and_scripts() -> None:
    request = _request(
        _playbook("fixture-restore", scripts=("scripts/undo.sh", "scripts/backfill.sh")),
        findings=(_firing("fixture-restore", "fixture-restore"),),
    )

    block = learned_playbooks_block(request)

    assert "- fixture-restore: its detector is firing" in block
    assert ".sdo/playbooks/fixture-restore/README.md" in block
    assert "scripts/backfill.sh" in block
    assert "scripts/undo.sh" in block


def test_the_generic_health_objective_is_never_listed() -> None:
    request = _request(_playbook("health-objective"), findings=(_firing("traffic-probe", "health-objective"),))

    assert learned_playbooks_block(request) == ""


def test_the_playbook_an_earlier_attempt_produced_is_named_on_that_attempt_and_listed() -> None:
    request = _request(_playbook("fixture-restore", origin="inc-first"), _playbook("unrelated", origin="inc-other"))
    attempt = EarlierAttempt(incident_id="inc-first", outcome="success")
    attempt.completed_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_699_999_000))
    request.memory_snapshot.earlier_attempts.append(attempt)

    assert "learned playbook: fixture-restore" in earlier_attempts_block(request)
    block = learned_playbooks_block(request)
    assert "- fixture-restore: learned from attempt 1" in block
    assert "unrelated" not in block


def test_a_playbook_both_firing_and_learned_earlier_is_listed_once() -> None:
    request = _request(
        _playbook("fixture-restore", origin="inc-first"), findings=(_firing("fixture-restore", "fixture-restore"),)
    )
    attempt = EarlierAttempt(incident_id="inc-first", outcome="success")
    request.memory_snapshot.earlier_attempts.append(attempt)

    block = learned_playbooks_block(request)

    assert block.count("- fixture-restore:") == 1
    assert "its detector is firing; learned from attempt 1" in block


def test_nothing_is_listed_without_a_fitting_playbook() -> None:
    assert learned_playbooks_block(_request(_playbook("idle"))) == ""


def test_the_block_is_bounded_and_every_path_survives() -> None:
    playbooks = [_playbook(f"p{i}", scripts=tuple(f"scripts/{'s' * 200}{j}.sh" for j in range(20))) for i in range(8)]
    findings = tuple(_firing(f"p{i}", f"p{i}") for i in range(8))

    block = learned_playbooks_block(_request(*playbooks, findings=findings))

    assert len(block) <= 800
    assert block.count(".sdo/playbooks/p") == 3
    assert "more" in block


def test_the_first_message_carries_the_block() -> None:
    request = _request(_playbook("fixture-restore"), findings=(_firing("fixture-restore", "fixture-restore"),))

    assert learned_playbooks_block(request) in str(_responder_prompt(request))
