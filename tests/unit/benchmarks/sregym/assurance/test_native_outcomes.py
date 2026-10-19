"""Assurance reads the native, incident-keyed outcome format through the memory CLI."""

from __future__ import annotations

from benchmarks.sregym.assurance.harness import _evidenced_outcome, _outcome_from_cli


def test_picks_the_requested_incident_from_cli_output() -> None:
    text = (
        '{"outcomes": [{"schema_version": 1, "incident_id": "incident-1", "classification": "success"},'
        ' {"schema_version": 1, "incident_id": "incident-2", "classification": "failed"}]}'
    )
    assert _outcome_from_cli(text, "incident-1").classification == "success"
    assert _outcome_from_cli(text, "incident-3") is None


def test_rejects_absent_or_malformed_cli_output() -> None:
    assert _outcome_from_cli(None, "incident-1") is None
    assert _outcome_from_cli('{"outcomes": [', "incident-1") is None
    assert _outcome_from_cli('{"unexpected": 1}', "incident-1") is None


def test_prefers_correlated_go_evidence_when_host_checkout_has_not_synced(tmp_path) -> None:
    evidence = {
        "native_ledger": {
            "outcome_document": {"schema_version": 1, "incident_id": "incident-1", "classification": "success"}
        }
    }
    assert _evidenced_outcome(evidence, tmp_path, "incident-1").classification == "success"


def test_ledger_comes_from_the_receipt_and_playbooks_are_logical_ids() -> None:
    from benchmarks.sregym.assurance.harness import _applied_playbook_ids, _evidenced_ledger

    ledger = {
        "incident_id": "incident-1",
        "closure": {"result": {"applied_playbooks": [{"playbook_id": "fix-cache", "script_names": ["repair.sh"]}]}},
    }
    evidence = {"native_ledger": ledger}

    assert _evidenced_ledger(evidence, "incident-1") == ledger
    assert _evidenced_ledger(evidence, "incident-2") is None
    assert _evidenced_ledger({}, "incident-1") is None
    assert _applied_playbook_ids(ledger) == ["fix-cache"]


def test_reflection_attempts_are_counted_from_scripted_reflection_turns() -> None:
    from benchmarks.sregym.assurance.harness import _reflection_attempts

    turns = [{"kind": "responder"}, {"kind": "reflection-fresh"}, {"kind": "reflection-retry"}]
    assert _reflection_attempts(turns) == 2
    assert _reflection_attempts([{"kind": "responder-warm"}]) == 0
