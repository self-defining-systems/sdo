"""Lean responder data preserves the facts needed to act and bounds inline context."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from sdo.agents.responder.incident_context import (
    MAX_INCIDENT_SUMMARY_CHARS,
    MAX_INLINE_FINDINGS,
    incident_data_view,
    prepared_incident_data,
)
from sdo.agents.responder.tools_cli import command_surface
from sdo.contracts import IncidentRequest, OperationalMemorySnapshot, Verdict
from tests.context_helpers import incident_document, incident_facts_path


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
        document = incident_document(rendered, full=True)
        assert len(document["findings"]) == 5


def test_oversized_incident_context_keeps_complete_facts_in_a_private_turn_file() -> None:
    request = _request()
    request.findings[-1].evidence = "start " + "large live evidence 界" * 1000 + " end"
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        assert "large live evidence" not in rendered
        details = incident_facts_path(rendered)
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
    assert selected["verified_successes"] == 0
    assert selected["verified_failures"] == 0
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
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        summary = incident_document(rendered)
        assert [line.split(":", 1)[0] for line in summary["findings"]] == [
            item.fingerprint for item in request.findings
        ]
        assert all(line.endswith(f"[Service/service-{i}]") for i, line in enumerate(summary["findings"]))
        coverage = summary["playbook_coverage"]
        assert coverage["matches"][0]["explained_finding_fingerprints"] == ["finding-0"]
        assert coverage["unexplained_findings"] == 4
        details = incident_facts_path(rendered)
        full = json.loads(details.read_text())
        assert full["intent"]["architecture"] == request.memory_snapshot.architecture_markdown
        assert "Supplemental architecture" not in rendered
        assert full["findings"] == incident_data_view(request)["findings"]
    assert not details.exists()


def test_accepted_playbook_documents_are_available_without_searching_examples() -> None:
    request = _request()
    request.memory_snapshot.playbooks.add(id="repair", markdown="# Accepted repair\nCheck live preconditions.")
    request.memory_snapshot.playbooks.add(id="other", markdown="# Another accepted playbook")
    before = request.SerializeToString()

    with prepared_incident_data(request) as rendered:
        document = incident_document(rendered, full=True)
        references = document["playbook_documents"]
        assert set(references) == {"repair", "other"}
        paths = [Path(value) for value in references.values()]
        assert Path(references["repair"]).read_text() == request.memory_snapshot.playbooks[0].markdown
        assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths)
        assert document["playbook_index"] == {"repair": "Accepted repair", "other": "Another accepted playbook"}
        assert json.loads(incident_facts_path(rendered).read_text())["memory_accepted_commit"] == "accepted"
        assert "Check live preconditions" not in rendered
        assert request.SerializeToString() == before
    assert all(not path.exists() for path in paths)


def test_incident_carries_the_current_cli_surface_without_tool_discovery() -> None:
    with prepared_incident_data(_request()) as rendered:
        tools = incident_document(rendered)["incident_tools"]
        assert "independent options" in tools["commands"]["mitigate"]
        assert set(tools["commands"]) == set(command_surface()["commands"])
        assert "--help" in tools["help"]
        assert "Full JSON:" in tools["output"]
        assert "--json" in tools["output"]
        full = json.loads(incident_facts_path(rendered).read_text())
        assert "sdo act --help" in full["incident_tools"]["commands"]["act"]


def test_verbose_findings_retain_identities_coverage_and_commands_in_summary() -> None:
    request = _request()
    for finding in request.findings:
        finding.evidence = "verbose observation " * 2000
    request.playbook_coverage.matches.add(
        playbook_id="repair", can_mitigate=False
    ).explained_finding_fingerprints.append("finding-0")
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= 6000
        summary = incident_document(rendered)
        assert len(summary["findings"]) == 5
        assert "verbose observation" not in rendered
        assert summary["playbook_coverage"]["matches"][0]["can_mitigate"] is False
        assert "mitigate" in summary["incident_tools"]["commands"]
        details = incident_facts_path(rendered)
        assert len(json.loads(details.read_text())["findings"][0]["evidence"]) > 30000


def test_many_finding_summary_keeps_unexplained_counts_and_complete_facts() -> None:
    from tests.unit.sdo.agents.prompts.cases import incident

    request = incident(100, oversized=True)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        summary = incident_document(rendered)
        assert len(summary["findings"]) + summary["findings_omitted"] == 100
        assert summary["playbook_coverage"]["unexplained_findings"] == 100
        details = incident_facts_path(rendered)
        assert len(json.loads(details.read_text())["findings"]) == 100


def test_many_stage_attempts_cannot_displace_bounded_first_prompt() -> None:
    from tests.unit.sdo.agents.prompts.cases import incident

    request = incident(100, oversized=True)
    report = request.prior_stages.add(stage="deterministic", remaining_finding_fingerprints=["f" * 5000] * 20)
    for index in range(20):
        report.mitigations.add(
            playbook_id=f"repair-{index}-" + "p" * 500,
            program_revision="revision",
            result="apply_error",
            error="e" * 5000,
        )
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        summary = incident_document(rendered)
        actions = summary["stage_actions"]
        assert "-> apply_error; error: eee" in actions[0]
        assert len(actions) + summary["stage_actions_omitted"] == 20
        assert summary["findings"]
        assert "mitigate" in summary["incident_tools"]["commands"]
        details = incident_facts_path(rendered)
        assert len(json.loads(details.read_text())["prior_stages"][0]["mitigations"]) == 20


def test_failed_episode_progress_stays_inline_with_full_results_in_facts() -> None:
    request = _request()
    request.findings[0].evidence = "large fact " * 4000
    prior = request.prior_response_progress.add(incident_id="failed-episode")
    prior.successful_actions.add(
        action_id="source-fixed", success=True, summary="source reconciled", details="write read back " * 4000
    )
    prior.blockers.add(reason_code="admission_blocked", reason="validate.example.test denied Pods")
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        assert "source-fixed" in rendered
        assert "admission_blocked" in rendered
        details = incident_facts_path(rendered)
        evidence = json.loads(details.read_text())["prior_response_progress"][0]
        assert evidence["successful_actions"][0]["details"] == "write read back " * 4000
        assert evidence["blockers"][0]["reason_code"] == "admission_blocked"


def test_many_prior_episodes_keep_latest_progress_within_first_prompt_budget() -> None:
    from tests.unit.sdo.agents.prompts.cases import incident

    request = incident(100, oversized=True)
    for index in range(10):
        episode = request.prior_response_progress.add(incident_id=f"episode-{index}", error="blocker " * 200)
        for action in range(8):
            episode.successful_actions.add(action_id=f"done-{index}-{action}", success=True, summary="repaired " * 100)
        episode.blockers.add(reason_code="admission_blocked", reason="webhook denial " * 200)
    for index in range(2):
        report = request.prior_stages.add(stage=f"stage-{index}", remaining_finding_fingerprints=["f" * 5000] * 20)
        for attempt in range(20):
            report.mitigations.add(playbook_id=f"repair-{attempt}-" + "p" * 500, result="apply_error", error="e" * 5000)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        assert "done-9-0" in rendered
        assert "admission_blocked" in rendered
        details = incident_facts_path(rendered)
        assert len(json.loads(details.read_text())["prior_response_progress"]) == 10


def test_prior_progress_summary_preserves_transport_omissions() -> None:
    from sdo.agents.responder.incident_context import prior_response_summary

    request = _request()
    request.prior_response_progress.add(incident_id="newest", truncated=True, omitted_episodes=23)
    summary = prior_response_summary(request)
    assert summary["episodes_omitted"] == 23
    assert summary["episodes"][0]["truncated"] is True


def _staged_request(findings: int, *, oversized: bool = False) -> IncidentRequest:
    """The handoff shape: findings are what the stage left; stage receipts name what it cleared."""
    request = _request()
    del request.findings[:]
    for index in range(findings):
        latent = index % 3 != 0
        finding = request.findings.add(
            detector_id="drift.unexplained" if latent else "health-objective",
            rule_id="unexplained" if latent else "deployment-unavailable",
            severity="latent" if latent else "critical",
            summary=("unexplained ConfigMap/extra-" if latent else "Deployment unavailable: api-")
            + str(index)
            + (": absent from accepted source" if latent else "")
            + (" detail" * 400 if oversized else ""),
            evidence="observed " * (4000 if oversized else 3),
        )
        finding.primary_resource.kind = "ConfigMap" if latent else "Deployment"
        finding.primary_resource.namespace = "toy"
        finding.primary_resource.name = f"extra-{index}" if latent else f"api-{index}"
        if latent:
            finding.fingerprint = f"drift.unexplained/unexplained/toy/ConfigMap/extra-{index}/" + "a" * 64
    request.memory_snapshot.architecture_markdown = "Architecture. " * (2000 if oversized else 5)
    for index in range(40 if oversized else 2):
        request.namespace_onboarding.objects.add().name = f"onboarded-{index}"
    stage = request.prior_stages.add(stage="deterministic", cleared_finding_fingerprints=["port", "drift-port"])
    cleared = stage.mitigations.add(
        playbook_id="service-source-drift",
        result="applied_cleared",
        plan_action_count=1,
        cleared_finding_fingerprints=["port", "drift-port"],
    )
    binding = cleared.bindings.add(name="service")
    binding.object.kind = "Service"
    binding.object.namespace = "toy"
    binding.object.name = "profile"
    cleared.apply_receipt.applied = True
    cleared.apply_receipt.plan.rationale = "full receipt evidence " * 4000
    failed = stage.mitigations.add(
        playbook_id="deployment-source-drift",
        result="rollout_failed",
        error="source_reconcile_partial: 2 objects retain source differences " + "x" * 4000,
    )
    failed.apply_receipt.applied = True
    for finding in request.findings:
        stage.remaining_finding_fingerprints.append(finding.fingerprint or finding.detector_id)
    return request


def _brief(rendered: str) -> dict[str, Any]:
    assert rendered.startswith("Incident request:\n"), rendered[:80]
    return json.loads(rendered.split("\n", 1)[1])


def test_first_message_lists_uncleared_findings_and_one_line_per_stage_action() -> None:
    request = _staged_request(4)
    with prepared_incident_data(request) as rendered:
        brief = _brief(rendered)
        assert brief["findings"][0].startswith("health-objective/deployment-unavailable/toy/Deployment/api-0: ")
        assert len(brief["findings"]) == 4
        assert all(isinstance(line, str) and "\n" not in line for line in brief["findings"])
        actions = brief["stage_actions"]
        assert actions[0] == (
            "deterministic: service-source-drift on Service/toy/profile -> applied_cleared; 1 write; cleared 2"
        )
        assert actions[1].startswith(
            "deterministic: deployment-source-drift -> rollout_failed; error: source_reconcile_partial"
        )
        assert all(len(line) <= 300 for line in actions)
        assert brief["stage_cleared_findings"] == 2
        assert "full receipt evidence" not in rendered
        assert "prior_stages" not in brief
        assert "intent" not in brief
        full = json.loads(Path(brief["details_file"]).read_text())
        assert full["prior_stages"][0]["mitigations"][0]["apply_receipt"]["plan"]["rationale"].startswith(
            "full receipt evidence"
        )
        assert full["findings"][0]["evidence"] == request.findings[0].evidence


@pytest.mark.parametrize("oversized", [False, True])
@pytest.mark.parametrize("count", [1, 5, 13, 25, 100])
def test_first_message_always_lists_findings_within_budget(count: int, oversized: bool) -> None:
    """Class guard: the first message never degrades to a field skeleton without findings."""
    request = _staged_request(count, oversized=oversized)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= MAX_INCIDENT_SUMMARY_CHARS
        assert "Summary (partial)" not in rendered
        assert "Full:" not in rendered
        brief = _brief(rendered)
        listed = brief["findings"]
        assert len(listed) + brief.get("findings_omitted", 0) == count
        assert len(listed) == min(count, MAX_INLINE_FINDINGS)
        critical = [line for line in listed if line.startswith("health-objective/")]
        assert critical == listed[: len(critical)], "critical findings come first"
        assert brief["stage_actions"]
        assert "incident_tools" in brief


def test_attempts_settled_by_one_application_are_one_stage_action() -> None:
    request = _staged_request(2)
    stage = request.prior_stages[0]
    del stage.mitigations[:]
    for name in ("geo", "rate", "profile", "user"):
        attempt = stage.mitigations.add(
            playbook_id="deployment-source-repair",
            result="rollout_failed",
            plan_action_count=1,
            error="source_reconcile_partial: 8 objects retain source differences",
            cleared_finding_fingerprints=[f"drift-{name}"],
        )
        attempt.apply_receipt.application_id = "apply-1"
        binding = attempt.bindings.add(name="deployment")
        binding.object.kind = "Deployment"
        binding.object.namespace = "toy"
        binding.object.name = name
    with prepared_incident_data(request) as rendered:
        assert _brief(rendered)["stage_actions"] == [
            "deterministic: deployment-source-repair on Deployment/toy/geo,Deployment/toy/rate,Deployment/toy/profile"
            " +1 -> rollout_failed; 4 writes; cleared 4; error: source_reconcile_partial: 8 objects retain source"
            " differences"
        ]


def test_coverage_brief_keeps_only_matches_that_explain_an_open_finding() -> None:
    request = _request()
    request.playbook_coverage.matches.add(playbook_id="settled").explained_finding_fingerprints.append("cleared")
    request.playbook_coverage.matches.add(playbook_id="open").explained_finding_fingerprints.extend(
        ["finding-1", "cleared"]
    )
    with prepared_incident_data(request) as rendered:
        coverage = incident_document(rendered)["playbook_coverage"]
        assert [match["playbook_id"] for match in coverage["matches"]] == ["open"]
        assert coverage["matches"][0]["explained_finding_fingerprints"] == ["finding-1"]
        assert coverage["unexplained_findings"] == 4
        full = json.loads(incident_facts_path(rendered).read_text())
        assert len(full["playbook_coverage"]["matches"]) == 2
