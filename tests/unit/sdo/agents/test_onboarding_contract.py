"""Onboarding metadata describes authored detectors, without provenance echoes."""

from sdo.agents.lifecycle.agents import HealthJudgeDraft


def test_judge_accepts_ordinary_detectors_without_goal_binding() -> None:
    draft = HealthJudgeDraft(failure_patterns=["invalid application configuration"], detector_ids=["app-config"])
    assert draft.detector_ids == ["app-config"]
    assert set(HealthJudgeDraft.model_fields) == {"failure_patterns", "detector_ids"}
