import json
from pathlib import Path

import pytest

from sdo.agents.responder import tools_cli


@pytest.mark.parametrize("full_json", [False, True])
@pytest.mark.parametrize("pending", [False, True])
def test_rec_final_selection_stdout_budget_and_controller_progress(tmp_path, monkeypatch, capsys, full_json, pending):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    finding = {"fingerprint": "new", "summary": "peer fault", "evidence": "changed"}
    response = {
        "command": "mitigate",
        "before_findings": [],
        "controller_findings": [finding],
        "progress": {"fixed": [finding], "new": [], "unchanged": []},
        "summary": "batch",
        "application_id": '💥"\\' * 100,
        "rollout_pending": {"reason": "settling"},
    }
    if not pending:
        response["option_reports"] = [
            {"option_id": '界"\\' * 100, "application_id": '💥"\\' * 100, "status": "STATUS_SUCCESS"} for _ in range(32)
        ]
        response["report"] = {"execution": [{"pending": False} for _ in range(32)]}
    tools_cli._print_response(response, full_json=full_json)
    stdout = capsys.readouterr().out
    assert len(stdout.encode("utf-8")) <= 2048
    envelope = json.loads(stdout) if full_json else None
    text = envelope["summary"] if envelope else stdout
    assert "fixed by/since this action: 1" in text
    assert "new since this action: 0" in text
    assert "Schema (top-level keys):" in text
    if pending:
        assert "rollout pending" in text
    else:
        assert "32 succeeded" in text
        assert "Options omitted:" in text
    path = Path(envelope["details_file"]) if envelope else Path(text.split("Full JSON: ")[1].splitlines()[0])
    report = json.loads(path.read_text())
    assert report["actions"] == [response]
    assert report["fixed"] == [finding]
