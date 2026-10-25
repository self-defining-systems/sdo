"""Lean responder data preserves the facts needed to act and bounds inline context."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sdo.agents.responder.incident_context import incident_data_view, prepared_incident_data
from sdo.contracts import IncidentRequest, OperationalMemorySnapshot, Verdict


def _request() -> IncidentRequest:
    request = IncidentRequest(
        application="toy",
        namespace="toy",
        incident_id="incident",
        repository_base_commit="accepted",
        memory_snapshot=OperationalMemorySnapshot(
            accepted_commit="accepted", goal_markdown="Stay available.", architecture_markdown="API calls store."
        ),
    )
    for index in range(5):
        finding = request.findings.add(
            detector_id=f"detector-{index}", fingerprint=f"finding-{index}", evidence="live fact"
        )
        finding.primary_resource.kind = "Service"
        finding.primary_resource.name = f"service-{index}"
        request.detector_history.add(detector_id=f"detector-{index}", status="firing")
    return request


def test_lean_view_retains_findings_intent_and_latest_detector_evaluations() -> None:
    request = _request()
    request.memory_snapshot.playbooks.add(id="repair", markdown="large unrelated content" * 2000)
    request.detector_history.add(detector_id="detector-0", status="clear")
    before = request.SerializeToString()

    document = incident_data_view(request)

    assert [item["fingerprint"] for item in document["findings"]] == [f"finding-{index}" for index in range(5)]
    assert document["intent"]["goal"] == "Stay available."
    assert document["intent"]["architecture"] == "API calls store."
    assert document["available_playbook_ids"] == ["repair"]
    assert (
        next(item for item in document["latest_detector_evaluations"] if item["detector_id"] == "detector-0")["status"]
        == "clear"
    )
    assert "large unrelated content" not in json.dumps(document)
    assert "detector_history" not in document
    assert "memory_snapshot" not in document
    assert request.SerializeToString() == before


def test_lean_context_for_five_findings_is_below_typical_budget() -> None:
    with prepared_incident_data(_request()) as rendered:
        assert len(rendered) <= 6000
        document = json.loads(rendered.split("Incident request:\n", 1)[1])
        assert len(document["findings"]) == 5


def test_oversized_incident_context_keeps_complete_facts_in_a_private_turn_file() -> None:
    request = _request()
    request.findings[-1].evidence = "start " + "large live evidence 界" * 1000 + " end"
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= 12000
        assert "Read the full incident facts before acting" in rendered
        details = Path(rendered.split("Full incident facts: ", 1)[1].splitlines()[0])
        document = json.loads(details.read_text(encoding="utf-8"))
        assert document["findings"][-1]["evidence"] == request.findings[-1].evidence
        assert details.stat().st_mode & 0o777 == 0o600
    assert not details.exists()


def test_quiet_program_sweeps_are_omitted_without_losing_actionable_evidence() -> None:
    request = _request()
    for index in range(20):
        run = request.initial_program_runs.add(program_id=f"quiet-{index}")
        run.verdict.result = Verdict.RESULT_NO_MATCH
    run = request.initial_program_runs.add(program_id="repair", can_mitigate=True)
    run.verdict.result = Verdict.RESULT_MATCH
    run.verdict.evidence.add(observed="wrong port")

    document = incident_data_view(request)

    assert [item["program_id"] for item in document["initial_program_runs"]] == ["repair"]
    assert document["initial_program_runs"][0]["verdict"]["evidence"][0]["observed"] == "wrong port"
    assert document["omitted_no_match_program_runs"] == 20


@pytest.mark.skipif(
    "playbook_coverage" not in IncidentRequest.DESCRIPTOR.fields_by_name,
    reason="D1 typed coverage contract is not on the production base yet",
)
def test_coverage_keeps_diagnosis_only_flags_counts_and_resource_bindings() -> None:
    request = _request()
    coverage_field = request.DESCRIPTOR.fields_by_name["playbook_coverage"]
    coverage = getattr(request, coverage_field.name)
    match = coverage.matches.add(playbook_id="diagnosis-only", can_mitigate=False)
    match.explained_finding_fingerprints.append("finding-0")
    binding = match.bindings.add(name="service")
    binding.object.kind = "Service"
    binding.object.name = "service-0"
    coverage.unexplained_finding_fingerprints.append("finding-1")

    view = incident_data_view(request)["playbook_coverage"]
    selected = view["matches"][0]
    assert selected["can_mitigate"] is False
    assert selected["verified_successes"] == 0
    assert selected["verified_failures"] == 0
    assert selected["bindings"][0]["object"]["name"] == "service-0"
    assert selected["explained_finding_fingerprints"] == ["finding-0"]
    assert view["unexplained_finding_fingerprints"] == ["finding-1"]
