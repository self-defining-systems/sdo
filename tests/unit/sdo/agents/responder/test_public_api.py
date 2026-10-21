"""The responder package exposes only available worker entry points."""

from sdo.agents import responder


def test_declared_responder_exports_resolve_after_investigator_removal() -> None:
    exported = {name: getattr(responder, name) for name in responder.__all__}
    assert exported["run_worker_task"] is responder.run_worker_task
    assert exported["run_reflection_task"] is responder.run_reflection_task
    assert "run_drift_task" not in exported
