"""The findings-hidden arm (``detection.hide_findings_from_agent``): Go clears every detector field from the agent's
task (``runtime/hide_findings.go hiddenFindingsRequest``). The responder still renders a complete first message from
what is left: the page, and none of the cleared evidence."""

from __future__ import annotations

from sdo.agents.responder.codex import _responder_prompt
from tests.unit.sdo.agents.responder.test_page_prompt_holds_no_finding import PAGE, _paged_with_clues

#: The IncidentRequest fields hiddenFindingsRequest clears (proto names).
CLEARED = (
    "opening_keys",
    "standing_keys",
    "joined_keys",
    "findings",
    "drift_events",
    "detector_history",
    "surfaced_playbooks",
    "relevant_outcomes",
    "passing_tests",
    "recent_changes",
    "state_changes",
    "initial_program_runs",
    "playbook_coverage",
    "prior_response_progress",
    "prior_stages",
)


def test_the_hidden_task_renders_the_page_alone() -> None:
    request = _paged_with_clues()
    names = [text for f in request.findings for text in (f.summary, f.fingerprint, f.detector_id, f.rule_id)]
    for field in CLEARED:
        request.ClearField(field)  # type: ignore[arg-type]
    prompt = str(_responder_prompt(request))
    assert PAGE in prompt
    for text in (*names, "synthetic-repair", "checkout", "not ready"):
        assert text not in prompt
