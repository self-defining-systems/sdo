"""Lean responder data preserves the facts needed to act and bounds inline context."""

from __future__ import annotations

import json
from pathlib import Path

from sdo.agents.responder.incident_context import MAX_INCIDENT_SUMMARY_CHARS, incident_data_view, prepared_incident_data
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


def test_coverage_keeps_diagnosis_only_flags_counts_and_resource_bindings() -> None:
    request = _request()
    coverage = request.playbook_coverage
    match = coverage.matches.add(playbook_id="diagnosis-only", can_mitigate=False)
    match.explained_finding_fingerprints.append("finding-0")
    binding = match.bindings.add(name="service")
    binding.object.kind = "Service"
    binding.object.name = "service-0"
    coverage.unexplained_finding_fingerprints.append("finding-1")

    view = incident_data_view(request)["playbook_coverage"]
    selected = view["matches"][0]
    assert selected["can_mitigate"] is False
    assert selected["successful_applications"] == 0
    assert selected["failed_applications"] == 0
    assert selected["bindings"][0]["object"]["name"] == "service-0"
    assert selected["explained_finding_fingerprints"] == ["finding-0"]
    assert view["unexplained_finding_fingerprints"] == ["finding-1"]


def test_absent_coverage_keeps_every_finding_unexplained() -> None:
    request = _request()
    assert incident_data_view(request)["playbook_coverage"] == {
        "matches": [],
        "unexplained_finding_fingerprints": [item.fingerprint for item in request.findings],
    }


def test_bulky_intent_does_not_hide_actionable_findings_and_coverage() -> None:
    request = _request()
    request.memory_snapshot.architecture_markdown = "Supplemental architecture. " * 1000
    match = request.playbook_coverage.matches.add(playbook_id="repair", mitigation_selection="repair-option")
    match.explained_finding_fingerprints.append("finding-0")

    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= 6000
        summary = json.loads(rendered.split("Incident request summary:\n", 1)[1].splitlines()[0])
        assert [item["fingerprint"] for item in summary["findings"]] == [item.fingerprint for item in request.findings]
        assert all(item["primary_resource"]["name"] == f"service-{i}" for i, item in enumerate(summary["findings"]))
        assert summary["playbook_coverage"] == incident_data_view(request)["playbook_coverage"]
        details = Path(rendered.split("Full incident facts: ", 1)[1].splitlines()[0])
        full = json.loads(details.read_text())
        assert full["intent"]["architecture"] == request.memory_snapshot.architecture_markdown
        assert "intent" in summary["supplemental_fields"]
        assert full["findings"] == incident_data_view(request)["findings"]
    assert not details.exists()


def test_accepted_playbook_documents_are_available_without_searching_examples() -> None:
    request = _request()
    request.memory_snapshot.playbooks.add(id="repair", markdown="# Accepted repair\nCheck live preconditions.")
    request.memory_snapshot.playbooks.add(id="other", markdown="# Another accepted playbook")
    before = request.SerializeToString()

    with prepared_incident_data(request) as rendered:
        document = json.loads(rendered.split("Incident request:\n", 1)[1])
        references = document["playbook_documents"]
        assert set(references) == {"repair", "other"}
        paths = [Path(value) for value in references.values()]
        assert Path(references["repair"]).read_text() == request.memory_snapshot.playbooks[0].markdown
        assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths)
        assert document["memory_accepted_commit"] == "accepted"
        assert "Check live preconditions" not in rendered
        assert request.SerializeToString() == before
    assert all(not path.exists() for path in paths)


def test_incident_carries_the_current_cli_surface_without_tool_discovery() -> None:
    with prepared_incident_data(_request()) as rendered:
        document = json.loads(rendered.split("Incident request:\n", 1)[1])
        tools = document["incident_tools"]
        assert "--finding" in tools["commands"]["mitigate"]
        assert "--resource" in tools["commands"]["act"]
        assert "--detector" in tools["commands"]["check"]
        assert "Full JSON:" in tools["output"]
        assert "--json" in tools["output"]


def test_verbose_findings_retain_identities_coverage_and_commands_in_summary() -> None:
    request = _request()
    for finding in request.findings:
        finding.evidence = "verbose observation " * 2000
    request.playbook_coverage.matches.add(playbook_id="repair", can_mitigate=False)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= 6000
        summary = json.loads(rendered.split("Incident request summary:\n", 1)[1].splitlines()[0])
        assert len(summary["findings"]) == 5
        assert summary["playbook_coverage"]["matches"][0]["can_mitigate"] is False
        assert "mitigate" in summary["incident_tools"]["commands"]
        details = Path(rendered.split("Full incident facts: ", 1)[1].splitlines()[0])
        assert len(json.loads(details.read_text())["findings"][0]["evidence"]) > 30000


def test_many_finding_summary_keeps_unexplained_counts_and_complete_facts() -> None:
    from tests.unit.sdo.agents.prompts.cases import incident

    request = incident(100, oversized=True)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        summary = json.loads(rendered.split("Incident request summary:\n", 1)[1].splitlines()[0])
        assert summary["finding_count"] == 100
        assert len(summary["findings"]) + summary["findings_omitted"] == 100
        coverage = summary["playbook_coverage"]
        assert len(coverage["unexplained_finding_fingerprints"]) + coverage["unexplained_findings_omitted"] == 100
        details = Path(rendered.split("Full incident facts: ", 1)[1].splitlines()[0])
        assert len(json.loads(details.read_text())["findings"]) == 100
