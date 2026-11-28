"""A page-opened incident's first message is the page: the clues riding in the request (failing user tests, Kubernetes
conditions, learned detectors naming playbooks) never reach it. The detector-ablation arm (``detection.pages_only``)
relies on this: its pages carry SDO's detector findings as clues for the automatic stage only."""

from __future__ import annotations

from sdo.agents.responder.codex import _responder_prompt
from tests.unit.sdo.agents.prompts.cases import incident

PAGE = "Application: demo\nNamespace: demo\n\nA synthetic demo application."


def _paged_with_clues():
    request = incident(3)
    request.page = PAGE
    del request.opening_keys[:]
    kinds = [
        ("traffic-health", "scenario-slo.checkout", "checkout fails 9/20"),
        ("kubernetes-workload-conditions", "workload-container-not-ready", "worker not ready"),
        ("synthetic-learned-detector", "synthetic-learned-detector", "synthetic ConfigMap missing"),
    ]
    for finding, (detector, rule, summary) in zip(request.findings, kinds, strict=True):
        finding.detector_id, finding.rule_id, finding.summary = detector, rule, summary
        finding.metadata["first_failure_at"] = "2023-11-14T22:13:00+00:00"
    request.findings[2].playbook_ids.append("synthetic-repair")
    request.standing_keys.extend(f"{f.detector_id}\x00{f.fingerprint}" for f in request.findings)
    return request


def test_a_paged_first_message_is_the_page_and_names_no_clue() -> None:
    request = _paged_with_clues()
    prompt = str(_responder_prompt(request))

    assert f"Opened on a page at 22:13:20 UTC; return when it is fixed:\n{PAGE}" in prompt
    for finding in request.findings:
        for text in (finding.summary, finding.fingerprint, finding.detector_id, finding.rule_id):
            assert text not in prompt
    for text in ("synthetic-repair", "failing user test", "checkout", "not ready"):
        assert text not in prompt


def test_the_same_clues_without_the_page_are_the_first_message() -> None:
    request = _paged_with_clues()
    request.page = ""
    request.opening_keys.append(request.standing_keys[0])

    assert "checkout" in str(_responder_prompt(request))
