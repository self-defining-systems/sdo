"""Learning receives measured cause-detection gaps, never benchmark timing."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sdo.agents.responder.reflection_brief import incident_brief
from sdo.contracts import datetime_timestamp
from tests.unit.sdo.agents.responder.test_reflection import _fresh_closure


@pytest.mark.parametrize(
    ("first", "delay", "requested"),
    [
        ("workload-health", 62, True),
        ("workload-health", 9, False),
        ("workload-health", 10, True),
        ("drift.unexplained", 2, False),
        ("drift.unexplained", 62, False),
    ],
)
def test_detection_gap_in_learning_brief(tmp_path: Path, first: str, delay: int, requested: bool) -> None:
    closure = _fresh_closure(tmp_path)
    visible = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ref = closure.request.findings[0].primary_resource
    closure.result.confirmed_root_causes.add(summary="Unexpected init container blocks startup", resources=[ref])
    closure.request.drift_events.add(
        ref=ref,
        observed_at=datetime_timestamp(visible),
        classification="unexplained",
        fields=[
            {"path": "/spec/template/spec/initContainers", "old": "[]", "new": '[{"name":"synthetic-dependency-wait"}]'}
        ],
    )
    closure.detector_timeline.add(
        detector_id=first,
        detector_class="incident" if first.startswith("drift.") else "health",
        first_activated_at=datetime_timestamp(visible + timedelta(seconds=delay)),
    )
    text = incident_brief(closure, worktree=tmp_path)
    assert first in text
    assert '"activation_delay_seconds": ' + str(delay) in text
    assert ("Write or refine an incident detector" in text) is requested
    assert '"existing_cause_detectors": []' in text


def test_unknown_visibility_never_invents_latency(tmp_path: Path) -> None:
    closure = _fresh_closure(tmp_path)
    closure.detector_timeline.add(
        detector_id="workload-health", detector_class="health", first_activated_at=closure.detected_at
    )
    text = incident_brief(closure, worktree=tmp_path)
    assert '"cause_observed_at": null' in text
    assert '"activation_delay_seconds": null' in text
    assert "Write or refine an incident detector" not in text


@pytest.mark.parametrize("mode", ["resume", "fresh"])
def test_both_learning_sessions_receive_detection_facts(tmp_path: Path, mode: str) -> None:
    from sdo.agents.responder.reflection import SessionReflector
    from tests.unit.sdo.agents.responder.test_reflection import _CapturingBackend, _outcome

    closure = _fresh_closure(tmp_path)
    closure.detector_timeline.add(
        detector_id="workload-health", detector_class="health", first_activated_at=closure.detected_at
    )
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=closure,
        session_mode=mode,
    )  # type: ignore[arg-type]
    assert '"first_detectors": [' in str(backend.calls[0]["prompt"])
    assert '"cause_observed_at": null' in str(backend.calls[0]["prompt"])


def test_existing_incident_detector_is_a_candidate_not_a_claim_of_equivalence(tmp_path: Path) -> None:
    closure = _fresh_closure(tmp_path)
    finding = closure.request.findings[0]
    finding.playbook_ids.append("init-repair")
    closure.result.confirmed_root_causes.add(summary="synthetic init wait", resources=[finding.primary_resource])
    closure.request.memory_snapshot.detectors.add(
        id="init-wait", detector_class="incident", possible_playbook_ids=["init-repair"]
    )
    closure.request.memory_snapshot.detectors.add(
        id="unrelated", detector_class="incident", possible_playbook_ids=["other"]
    )
    closure.detector_timeline.add(
        detector_id="workload-health", detector_class="health", first_activated_at=closure.detected_at
    )
    text = incident_brief(closure, worktree=tmp_path, include_memory=False)
    assert '"existing_cause_detectors": [\n    "init-wait"\n  ]' in text
    assert "linked candidates; compare predicates" in text
    assert "unrelated" not in text
