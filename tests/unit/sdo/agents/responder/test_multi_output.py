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
