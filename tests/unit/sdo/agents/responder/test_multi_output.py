from copy import deepcopy

from sdo.agents.responder.multi_output import option_action_results


def test_per_option_formatter_input_retains_receipts_and_pending_state():
    reports = [
        {
            "option_id": "first",
            "application_id": "one",
            "status": "STATUS_SUCCESS",
            "rounds": [{"actions": [{"action_id": "receipt-one"}]}],
        },
        {
            "option_id": "second",
            "application_id": "two",
            "status": "STATUS_UNRESOLVED",
            "reason_code": "rollout_pending",
            "rounds": [{"actions": [{"action_id": "receipt-two"}]}],
        },
    ]
    executions = [
        {"option_id": "first", "application_id": "one", "pending": False, "action_results": [{"status": "applied"}]},
        {
            "option_id": "second",
            "application_id": "two",
            "pending": True,
            "plan": {"actions": [{"scale_workload": {"ref": "synthetic"}}]},
        },
    ]
    response = {
        "command": "mitigate",
        "option_reports": reports,
        "report": {"execution": executions},
        "rollout_pending": {"reason": "settling"},
        "controller_findings": [{"fingerprint": "peer"}],
    }
    original = deepcopy(response)
    actions = option_action_results(response)
    assert len(actions) == 2
    assert actions[0]["application_id"] == "one"
    assert actions[0]["report"]["rounds"] == reports[0]["rounds"]
    assert actions[0]["report"]["execution"] == [executions[0]]
    assert "rollout_pending" not in actions[0]
    assert actions[1]["pending"] is True
    assert actions[1]["report"]["execution"] == [executions[1]]
    assert actions[1]["controller_findings"] == response["controller_findings"]
    assert response == original


def test_batch_peer_failure_does_not_mark_successful_option_failed():
    response = {
        "command": "mitigate",
        "error": "second failed",
        "option_reports": [
            {"option_id": "first", "status": "STATUS_SUCCESS"},
            {"option_id": "second", "status": "STATUS_EXECUTION_ERROR", "reason_code": "execution_failed"},
        ],
        "report": {"execution": [{"option_id": "first"}, {"option_id": "second", "error": "denied"}]},
    }
    actions = option_action_results(response)
    assert "error" not in actions[0]
    assert actions[1]["error"] == "denied"


def test_single_action_retains_its_complete_output():
    response = {"command": "apply", "summary": "done", "program_runs": [{"program_id": "repair"}]}
    assert option_action_results(response) == [response]


def test_combined_summary_uses_shared_progress_and_retains_complete_aggregate(tmp_path, monkeypatch):
    import json

    from sdo.agents.responder.multi_output import format_selection_response

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    before = [{"fingerprint": "fixed", "evidence": "old"}]
    response = {
        "command": "mitigate",
        "summary": "batch",
        "option_reports": [
            {"option_id": "one", "application_id": "applied-one", "status": "STATUS_SUCCESS"},
            {"option_id": "two", "application_id": "pending-two", "status": "STATUS_UNRESOLVED"},
        ],
        "report": {"attempt_id": "combined", "execution": [{"pending": False}, {"pending": True}]},
    }
    text, path = format_selection_response(before, [], response)
    assert "1 succeeded; 1 pending; 0 failed" in text
    assert "option: one success application=applied-one" in text
    assert "option: two pending application=pending-two" in text
    assert "fixed by/since this action: 1" in text
    assert "Incident: NOT verified" in text
    assert len(text.encode()) <= 2048
    report = json.loads(path.read_text())
    assert report["actions"] == [response]
    assert report["option_reports"] == response["option_reports"]


def test_many_options_keep_full_reports_and_explicit_omissions_within_budget(tmp_path, monkeypatch):
    import json

    from sdo.agents.responder.multi_output import format_selection_response

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    response = {
        "command": "mitigate",
        "summary": "batch",
        "option_reports": [
            {
                "option_id": f"option-{i}-" + "x" * 100,
                "application_id": "application-" + "y" * 100,
                "status": "STATUS_SUCCESS",
            }
            for i in range(32)
        ],
        "report": {"execution": [{"pending": False} for _ in range(32)]},
    }
    findings = [
        {
            "fingerprint": str(i),
            "summary": "still broken " * 20,
            "primary_resource": {"kind": "Service", "namespace": "app", "name": str(i)},
        }
        for i in range(12)
    ]
    text, path = format_selection_response(findings, findings, response)
    assert len((text + "\n").encode()) <= 2048
    assert "Options omitted:" in text
    assert "still broken, unchanged: 12" in text
    assert "Schema (top-level keys):" in text
    document = json.loads(path.read_text())
    assert document["actions"] == [response]
    assert len(document["option_reports"]) == 32
