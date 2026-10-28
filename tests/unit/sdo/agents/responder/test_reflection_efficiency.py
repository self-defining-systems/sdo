"""Reflection keeps all accepted memory visible and reserves independent acceptance time."""

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
    assert [book["id"] for book in view["playbooks"]] == ["book-98", "book-99"] + [f"book-{i}" for i in range(98)]
    assert [item["id"] for item in view["detectors"]] == ["relevant", "unrelated"]
    assert view["omitted_playbooks"] == 0
    focus = view["incident_context"]
    assert focus["accepted_commit"] == "accepted"
    assert focus["explained_findings"] == 1
    assert focus["stage_mitigations"][0]["program_revision"] == "applied-revision"
    assert len(snapshot.playbooks) == 100  # projection never mutates accepted input


@pytest.mark.parametrize("link", ["finding", "disabled_detector", "applied_program", "skipped_program"])
def test_incident_memory_retains_explicit_links_and_owned_programs(link: str) -> None:
    from sdo.agents.responder.reflection_evidence import incident_memory_snapshot

    snapshot = OperationalMemorySnapshot(accepted_commit="accepted-revision")
    for index in range(40):
        snapshot.playbooks.add(id=f"unrelated-{index}", markdown="unrelated")
        snapshot.detectors.add(id=f"unrelated-{index}")
    book = snapshot.playbooks.add(id="linked-book", markdown="incident guidance")
    book.scripts.add(name="repair.go", content="owned program")
    outcome = OutcomeRecord(incident_id="incident")
    if link == "finding":
        outcome.findings.add(detector_id="absent-detector", playbook_ids=[book.id])
    elif link == "disabled_detector":
        snapshot.detectors.add(id="disabled", description="repair this detector", possible_playbook_ids=[book.id])
        outcome.disabled_setup_detectors.add(detector_id="disabled")
    else:
        stage = outcome.prior_stages.add(stage="deterministic")
        stage.mitigations.add(
            playbook_id=book.id,
            program_revision="exact-revision",
            result="skipped" if link == "skipped_program" else "applied_cleared",
        )

    scoped = incident_memory_snapshot(snapshot, outcome)
    assert scoped.accepted_commit == "accepted-revision"
    assert list(scoped.playbooks) == [book] + list(snapshot.playbooks[:-1])
    assert scoped.playbooks[0].scripts[0].content == "owned program"
    view = reflection_memory_view(snapshot, outcome)
    assert view["accepted_commit"] == "accepted-revision"
    assert [item["id"] for item in view["playbooks"]] == [book.id] + [f"unrelated-{i}" for i in range(40)]
    assert view["playbooks"][0]["script_names"] == ["repair.go"]
    assert [item["id"] for item in view["detectors"]] == (["disabled"] if link == "disabled_detector" else []) + [
        f"unrelated-{i}" for i in range(40)
    ]
    if link == "disabled_detector":
        assert view["detectors"][0]["description"] == "repair this detector"
    assert len(snapshot.playbooks) == 41


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


@pytest.mark.parametrize("mode", ["fresh", "resume"])
def test_reflection_sessions_show_all_memory_once_with_incident_entries_first(tmp_path: Path, mode: str) -> None:
    from tests.unit.sdo.agents.responder.test_reflection import _CapturingBackend, _fresh_closure, _outcome

    closure = _fresh_closure(tmp_path)
    snapshot = closure.request.memory_snapshot
    del snapshot.playbooks[:]
    del snapshot.detectors[:]
    for index in range(45):
        snapshot.playbooks.add(id=f"unrelated-{index}", markdown=f"advice-{index}")
        snapshot.detectors.add(id=f"other-{index}", description=f"signature-{index}")
    snapshot.playbooks.add(id="linked", markdown="already covered")
    snapshot.detectors.add(id="linked-detector", possible_playbook_ids=["linked"])
    outcome = _outcome()
    outcome.applied_playbooks[:] = ["linked"]
    backend = _CapturingBackend()
    reflection.SessionReflector(backend).resume(
        session_id="session",
        incident_id=outcome.incident_id,
        worktree=tmp_path,
        outcome=outcome,
        history=[],
        outcome_commit="outcome",
        session_mode=mode,
        closure=closure,
    )
    prompt = str(backend.calls[0]["prompt"])
    for index in range(45):
        assert prompt.count(f'advice-{index}"') == 1
        assert f"signature-{index}" in prompt
    assert prompt.index('"id": "linked"') < prompt.index('"id": "unrelated-0"')
    assert "Inspect only incident-relevant memory" not in prompt
    assert len(snapshot.playbooks) == 46
