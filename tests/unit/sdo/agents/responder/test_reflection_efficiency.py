"""Reflection stays incident-scoped and reserves independent acceptance time."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from sdo.agents.responder import reflection
from sdo.agents.responder.memory_check import ValidationResult
from sdo.agents.responder.reflection_evidence import reflection_memory_view
from sdo.contracts import OperationalMemorySnapshot, OutcomeRecord
from tests.structured_turns import ScriptedAgent, reply


def test_incident_memory_finds_relevant_books_beyond_the_inventory_prefix() -> None:
    snapshot = OperationalMemorySnapshot(accepted_commit="accepted")
    for index in range(100):
        snapshot.playbooks.add(id=f"book-{index}", markdown=f"unique-{index}")
    snapshot.detectors.add(id="relevant", possible_playbook_ids=["book-99"])
    snapshot.detectors.add(id="unrelated", possible_playbook_ids=["book-0"])
    outcome = OutcomeRecord(incident_id="incident", applied_playbooks=["book-98"])
    outcome.findings.add(detector_id="relevant", fingerprint="finding")
    match = outcome.playbook_coverage.matches.add(playbook_id="book-99", program_revision="revision")
    match.explained_finding_fingerprints.append("finding")
    stage = outcome.prior_stages.add(stage="deterministic")
    stage.mitigations.add(playbook_id="book-98", program_revision="applied-revision", result="applied_cleared")
    view = reflection_memory_view(snapshot, outcome)
    assert [book["id"] for book in view["playbooks"]] == ["book-98", "book-99"]
    assert [item["id"] for item in view["detectors"]] == ["relevant"]
    assert view["omitted_playbooks"] == 98
    focus = view["incident_context"]
    assert focus["accepted_commit"] == "accepted"
    assert focus["explained_finding_fingerprints"] == ["finding"]
    assert focus["stage_mitigations"][0]["program_revision"] == "applied-revision"
    assert len(snapshot.playbooks) == 100  # projection never mutates accepted input


@pytest.mark.parametrize("backend_type", [reflection.CodexSessionBackend, reflection.ClaudeSessionBackend])
def test_agent_cannot_consume_the_final_check_reserve(tmp_path: Path, monkeypatch, backend_type) -> None:
    now = 10.0
    monkeypatch.setattr(reflection, "time", SimpleNamespace(monotonic=lambda: now))
    checked = []
    timeouts = []

    def respond(_request):
        nonlocal now
        now += timeouts[-1]  # provider consumes its whole allotted turn
        return reply(
            backend_type.provider,
            {
                "summary": "covered",
                "learning_decision": "no_change",
                "no_change_reason": "covered",
            },
            session_id="session",
            branch="reflection",
        )

    def check(_root, _decision, timeout):
        checked.append(timeout)
        return ValidationResult(0, False, "", "")

    agent = ScriptedAgent(respond)
    backend = backend_type(executor=agent.executor, timeout_seconds=900, memory_checker=check)
    run = backend._run

    def capture(**kwargs):
        timeouts.append(kwargs["timeout_seconds"])
        return run(**kwargs)

    monkeypatch.setattr(backend, "_run", capture)
    result = backend.fresh(worktree=tmp_path, prompt="reflect", idempotency_key="id")
    assert result.learning_decision == "no_change"
    assert timeouts == [600]
    assert checked == [300]


def test_unchanged_blocking_diagnostics_stop_correction_early(tmp_path: Path) -> None:
    agent = ScriptedAgent(
        lambda _: reply(
            "codex",
            {
                "summary": "covered",
                "learning_decision": "no_change",
                "no_change_reason": "covered",
            },
            session_id="session",
            branch="reflection",
        )
    )
    backend = reflection.CodexSessionBackend(
        executor=agent.executor,
        memory_checker=lambda *_: ValidationResult(
            1, False, '{"diagnostics":[{"rule":"program.compile","message":"same error"}]}', ""
        ),
    )
    with pytest.raises(reflection.ReflectionCorrectionStopped) as error:
        backend.fresh(worktree=tmp_path, prompt="reflect", idempotency_key="id")
    assert error.value.reason == "unchanged_validation_failure"
    assert len(agent.requests) == 2


def test_history_relevance_and_size_do_not_grow_with_unrelated_incidents() -> None:
    import json

    outcome = OutcomeRecord(incident_id="current")
    outcome.findings.add(detector_id="incident-detector", fingerprint="finding")
    history = []
    for index in range(100):
        record = OutcomeRecord(incident_id=f"unrelated-{index}")
        record.findings.add(detector_id="other-detector")
        history.append(record)
    for index in range(10):
        record = OutcomeRecord(incident_id=f"related-{index}")
        record.findings.add(detector_id="incident-detector")
        history.append(record)
    section = reflection._history_section(outcome, history)
    views = json.loads(section.split("\n", 1)[1].split("\nFull prior", 1)[0])
    assert [view["incident_id"] for view in views] == ["related-7", "related-8", "related-9"]
    assert "unrelated" not in section


@pytest.mark.parametrize(
    "relative", ["sdo/agents/responder/prompts/reflection.md", "sdo/skills/sdo-playbook-authoring/SKILL.md"]
)
def test_cleanly_applied_revisions_are_preserved(relative: str) -> None:
    root = Path(__file__).resolve().parents[5]
    text = (root / relative).read_text()
    assert (
        "Keep revisions that applied cleanly unless this incident shows failure or incompleteness; "
        "add or narrowly edit, never rewrite working steps."
    ) in text
