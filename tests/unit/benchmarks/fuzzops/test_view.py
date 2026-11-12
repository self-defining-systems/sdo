"""The fuzzops-bench dashboard view: contract limits, events, transcripts from rollouts and redaction."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import view

if TYPE_CHECKING:
    from pathlib import Path

INJECT = "2026-11-11T08:00:00Z"
ACCOUNT = "205d60f1-2e84-4617-980b-aea896d9da8c"
USER = "user-Re0kdZFWbPOj6qBfwedCB24T"
INCIDENT = "inc-1"
DIR = "ab" * 16


def _ledger() -> dict[str, Any]:
    finding = {
        "detector_id": "workload-health",
        "fingerprint": "workload-health/not-ready/ns/Deployment/user",
        "rule_id": "not-ready",
        "summary": "Deployment user: not ready",
        "primary_resource": {"kind": "Deployment", "name": "user", "namespace": "ns"},
    }
    return {
        "incident_id": INCIDENT,
        "acknowledged_at": "2026-11-11T08:03:00Z",
        "closure": {
            "detected_at": "2026-11-11T08:01:00Z",
            "dispatched_at": "2026-11-11T08:01:01Z",
            "verified_at": "2026-11-11T08:02:30Z",
            "detector_timeline": [
                {
                    "detector_id": "workload-health",
                    "rule_id": "not-ready",
                    "fingerprint": finding["fingerprint"],
                    "first_activated_at": "2026-11-11T08:00:59Z",
                },
                {"detector_id": "workload-health", "first_activated_at": "2026-11-11T08:01:30Z"},
            ],
        },
        "closure_phase_timestamps": {
            "closure_ack": "2026-11-11T08:03:00Z",
            "reflection_dispatch": "2026-11-11T08:03:05Z",
            "reflection_committed": "2026-11-11T08:09:00Z",
        },
        "reflection_decision": {
            "learning_decision": "updated",
            "playbook_ids": ["probe-drift"],
            "usage": {"input_tokens": "900"},
        },
        "reflection_paths": [
            ".sdo/playbooks/probe-drift/diagnose.go",
            ".sdo/diagnostics/detectors/incidents/probe-detector/detector.go",
            ".sdo/corpus/case/open.json",
        ],
        "outcome_document": {
            "incident_id": INCIDENT,
            "classification": "success",
            "resolution_path": "responder",
            "findings": [finding],
            "confirmed_root_causes": [{"cause_class": "config", "reach": "workload"}],
            "prior_stages": [
                {
                    "stage": "deterministic",
                    "started_at": "2026-11-11T08:01:02Z",
                    "recovery_wait": {"exit_reason": "EXIT_REASON_UNCOVERED"},
                }
            ],
            "repair_actions": [
                {
                    "kind": "kubectl-patch",
                    "target": "Deployment/ns/user",
                    "summary": "restore probe",
                    "success": True,
                    "started_at": "2026-11-11T08:02:00Z",
                    "completed_at": "2026-11-11T08:02:05Z",
                    "resources": [{"kind": "Deployment", "name": "user", "namespace": "ns"}],
                }
            ],
            "timestamps": {
                "detected_at": "2026-11-11T08:01:00Z",
                "verified_at": "2026-11-11T08:02:30Z",
                "completed_at": "2026-11-11T08:02:30Z",
            },
            "usage": {"input_tokens": "5000"},
        },
    }


def _msg(role: str, text: str, kind: str = "input_text") -> dict[str, Any]:
    return {
        "type": "response_item",
        "timestamp": "2026-11-11T08:01:05Z",
        "payload": {"type": "message", "role": role, "content": [{"type": kind, "text": text}]},
    }


def _rollout() -> list[dict[str, Any]]:
    js = (
        'const r = await tools.exec_command({cmd:"kubectl get pods -n ns \\"x\\"\\nls",max_output_tokens:6000});\n'
        "text(r.output);"
    )
    item = {"type": "item_completed", "item": {"type": "UserMessage"}}
    return [
        {"type": "session_meta", "payload": {"creator_user_id": USER, "creator_account_id": ACCOUNT}},
        {"type": "event_msg", "payload": {"type": "task_started"}},
        _msg("developer", "permissions text"),
        _msg("user", "SDO responder: inc-1. Fix it."),
        {"type": "event_msg", "payload": item},
        _msg("assistant", "I will look.", "output_text"),
        {"type": "response_item", "payload": {"type": "reasoning", "summary": [], "encrypted_content": "gAAA"}},
        {"type": "response_item", "payload": {"type": "custom_tool_call", "name": "exec", "input": js}},
        {"type": "response_item", "payload": {"type": "custom_tool_call", "name": "exec", "input": "text(1)"}},
        {"type": "response_item", "payload": {"type": "function_call", "name": "wait", "arguments": '{"cell_id":"1"}'}},
        {
            "type": "response_item",
            "payload": {
                "type": "custom_tool_call_output",
                "output": [
                    {"type": "input_text", "text": "Script completed"},
                    {"type": "input_text", "text": "x" * 5000},
                ],
            },
        },
        {"type": "response_item", "payload": {"type": "function_call_output", "output": "plain output"}},
        {"type": "event_msg", "payload": {"type": "task_complete", "last_agent_message": '{"incident_result":{}}'}},
        {"type": "event_msg", "payload": {"type": "task_started"}},
        _msg(
            "user", f"Idempotency key: reflection:{INCIDENT}\nThe controller has committed the outcome. token: hunter2x"
        ),
        _msg("assistant", f"Learned. account {ACCOUNT}", "output_text"),
        {"type": "event_msg", "payload": {"type": "task_complete", "last_agent_message": '{"learning_decision":"x"}'}},
    ]


def _collected(tmp: Path, *, run_json: bool = True, rollout: bool = True, memory: bool = True) -> Path:
    dest = tmp / "run" / "operator"
    incident = dest / "evidence" / "incidents" / DIR
    incident.mkdir(parents=True)
    (dest / "evidence" / "layout.json").write_text(json.dumps({"schema": "sdo.evidence/v1"}))
    (incident / "ledger.json").write_text(json.dumps(_ledger()))
    if rollout:
        path = incident / "codex" / "sessions" / "2026" / "11" / "11" / "rollout-x.jsonl"
        path.parent.mkdir(parents=True)
        path.write_text("".join(json.dumps(r) + "\n" for r in _rollout()))
    if memory:
        book = dest / "memory" / ".sdo" / "playbooks" / "probe-drift"
        book.mkdir(parents=True)
        (book / "README.md").write_text("# probe drift\n")
    if run_json:
        (dest.parent / "run.json").write_text(
            json.dumps({"fault": "f", "inject_at": INJECT, "settle_end": "2026-11-11T08:30:00Z", "status": "done"})
        )
    return dest


def _read(dest: Path) -> dict[str, Any]:
    return json.loads((dest / "view.json").read_text())


def _transcript(dest: Path, name: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (dest / "view" / "transcripts" / name).read_text().splitlines()]


def _assert_valid(dest: Path, doc: dict[str, Any]) -> view.View:
    """Rebuild the document through the contract dataclasses (limits re-checked) and check the files."""
    assert doc["schema"] == view.SCHEMA
    panels: list[view.Panel] = []
    for p in doc["panels"]:
        if p["type"] == "table":
            rows = [[view.FileCell(**c) if isinstance(c, dict) else c for c in row] for row in p["rows"]]
            panels.append(view.TablePanel(p["title"], p["columns"], rows))
        else:
            panels.append(view.TranscriptPanel(p["title"], p["file"]))
    parsed = view.View(doc["summary"], doc["hide"], [view.Event(**e) for e in doc["events"]], panels)
    parsed.check_files(dest)
    return parsed


# ---- the view ----------------------------------------------------------------------------------------------


def test_write_builds_a_valid_view_with_the_sdo_summary(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    doc = view.write(dest)
    assert doc == _read(dest)
    _assert_valid(dest, doc)
    assert doc["summary"] == {
        "incidents": 1,
        "outcome": "success",
        "cause_class": "config",
        "reach": "workload",
        "auto_applied": "no",
        "responder_tokens": 5000,
        "reflection": "updated",
        "learned": 2,
    }
    assert doc["hide"] == [
        "evidence/**/rollout-*.jsonl",
        "evidence/**/claude/projects/**",
        "evidence/**/model_io/**",
        "controller.log",
    ]


def test_events_are_ordered_on_the_five_lanes(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    events = view.write(dest)["events"]
    assert [e["at"] for e in events] == sorted(e["at"] for e in events)
    assert {e["lane"] for e in events} == {"detect", "stage", "act", "close", "learn"}
    assert [e["lane"] for e in events] == [
        "detect", "detect", "stage", "stage", "act", "close", "close", "learn", "learn",
    ]  # fmt: skip
    act = next(e for e in events if e["lane"] == "act")
    assert act["end"] > act["at"]
    assert act["component"] == "Deployment/user"
    assert act["file"] == f"evidence/incidents/{DIR}/ledger.json"
    detectors = [e for e in events if e["text"].startswith("detector ")]
    assert len(detectors) == 1  # first activation per detector
    assert detectors[0]["component"] == "Deployment/user"
    stage = next(e for e in events if e["lane"] == "stage" and "deterministic" in e["text"])
    assert "no mitigation applied" in stage["text"]
    assert "uncovered" in stage["text"]
    assert events[-1]["text"] == "reflection committed (updated)"


def test_incident_table_counts_seconds_after_the_inject(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    panels = {p["title"]: p for p in view.write(dest)["panels"] if p["type"] == "table"}
    incidents = panels["Incidents"]
    assert incidents["columns"][:4] == ["incident", "detected s", "verified s", "closed s"]
    row = incidents["rows"][0]
    assert row[:4] == [INCIDENT, 60.0, 150.0, 150.0]
    assert row[9:11] == [5000, 900]
    assert row[11] == {"text": "ledger.json", "file": f"evidence/incidents/{DIR}/ledger.json"}
    assert panels["Repair actions"]["rows"] == [[120.0, "kubectl-patch", "Deployment/ns/user", True, "restore probe"]]
    learned = panels["Learned in this run"]["rows"]
    assert learned[0] == [
        "playbook",
        "probe-drift",
        {"text": "README.md", "file": "memory/.sdo/playbooks/probe-drift/README.md"},
    ]
    assert ["detector", "probe-detector", None] in learned
    assert ["file", ".sdo/corpus/case/open.json", None] in learned


def test_times_stay_absolute_without_a_run_record(tmp_path: Path) -> None:
    dest = _collected(tmp_path, run_json=False)
    incidents = next(p for p in view.write(dest)["panels"] if p["title"] == "Incidents")
    assert incidents["columns"][1] == "detected"
    assert incidents["rows"][0][1] == "2026-11-11T08:01:00Z"


def test_inject_comes_from_the_record_and_picks_the_runs_incidents(tmp_path: Path) -> None:
    dest = _collected(tmp_path, run_json=False)
    (dest.parent / "record.json").write_text(
        json.dumps({"lifecycle": {"inject_at": "2026-11-11T09:00:00Z"}, "fault": "f", "operator": "o"})
    )
    doc = view.write(dest)
    assert doc["summary"]["incidents"] == 0  # the only incident was detected before this run's window
    (dest.parent / "record.json").write_text(json.dumps({"lifecycle": {"inject_at": INJECT}}))
    assert view.write(dest)["summary"]["incidents"] == 1


def test_camel_case_ledgers_are_read(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    ledger = dest / "evidence" / "incidents" / DIR / "ledger.json"

    def camel(v: Any) -> Any:
        if isinstance(v, dict):
            out = {}
            for k, x in v.items():
                head, *rest = k.split("_")
                out[head + "".join(p.title() for p in rest)] = camel(x)
            return out
        return [camel(x) for x in v] if isinstance(v, list) else v

    ledger.write_text(json.dumps(camel(_ledger())))
    doc = view.write(dest)
    assert doc["summary"]["outcome"] == "success"
    assert any(e["lane"] == "act" for e in doc["events"])


# ---- transcripts -------------------------------------------------------------------------------------------


def test_rollout_splits_into_responder_and_reflection(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    doc = view.write(dest)
    short = DIR[:12]
    transcripts = [p for p in doc["panels"] if p["type"] == "transcript"]
    assert [p["file"] for p in transcripts] == [
        f"view/transcripts/{short}-responder.jsonl",
        f"view/transcripts/{short}-reflection.jsonl",
    ]
    responder = _transcript(dest, f"{short}-responder.jsonl")
    assert responder[0]["schema"] == "fuzzops.transcript/v1"
    assert responder[0]["source"].startswith(f"evidence/incidents/{DIR}/codex/sessions/")
    entries = responder[1:]
    assert [e["role"] for e in entries] == ["user", "assistant", "note", "tool", "tool", "tool", "output", "output"]
    assert entries[0]["text"].startswith("SDO responder")  # the developer message is skipped
    assert sum(e["text"].startswith("I will look") for e in entries) == 1  # item_completed copies are skipped
    assert next(e for e in entries if e["role"] == "note")["text"] == "reasoning (not summarized)"
    reflection = _transcript(dest, f"{short}-reflection.jsonl")[1:]
    assert reflection[0]["role"] == "user"
    assert "reflection" in reflection[0]["text"]
    assert all("hunter2x" not in e["text"] and ACCOUNT not in e["text"] for e in reflection)


def test_claude_session_files_make_responder_panels(tmp_path: Path) -> None:
    dest = _collected(tmp_path, rollout=False)
    session = dest / "evidence" / "incidents" / DIR / "claude" / "projects" / "-checkout" / "d50edf54.jsonl"
    session.parent.mkdir(parents=True)
    rows = [
        {"type": "queue-operation", "timestamp": "2026-11-11T08:10:00Z"},
        {
            "type": "user",
            "timestamp": "2026-11-11T08:10:01Z",
            "message": {"role": "user", "content": "SDO responder: inc-1."},
        },
        {
            "type": "assistant",
            "timestamp": "2026-11-11T08:10:02Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Checking pods."},
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "kubectl get pods"}},
                ],
            },
        },
        {
            "type": "user",
            "timestamp": "2026-11-11T08:10:03Z",
            "message": {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "pod-a Running"},
                ],
            },
        },
    ]
    session.write_text("".join(json.dumps(r) + "\n" for r in rows))
    doc = view.write(dest)
    short = DIR[:12]
    assert [p["file"] for p in doc["panels"] if p["type"] == "transcript"] == [
        f"view/transcripts/{short}-responder.jsonl"
    ]
    entries = _transcript(dest, f"{short}-responder.jsonl")[1:]
    assert [e["role"] for e in entries] == ["user", "assistant", "tool", "output"]
    assert entries[2]["text"] == "kubectl get pods"


def test_a_rollout_with_reasoning_summaries_exports_them_to_the_transcript(tmp_path: Path) -> None:
    dest = _collected(tmp_path, rollout=False)
    path = dest / "evidence" / "incidents" / DIR / "codex" / "sessions" / "2026" / "11" / "11" / "rollout-x.jsonl"
    path.parent.mkdir(parents=True)
    summary = [
        {"type": "summary_text", "text": "**Checking probes**"},
        {"type": "summary_text", "text": "Then rollout"},
    ]
    records = [r for r in _rollout() if r.get("payload", {}).get("type") != "reasoning"]
    records.insert(
        3, {"type": "response_item", "payload": {"type": "reasoning", "summary": summary, "encrypted_content": "gAAA"}}
    )
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    view.write(dest)
    entries = _transcript(dest, f"{DIR[:12]}-responder.jsonl")[1:]
    reasoning = [e for e in entries if e["role"] == "reasoning"]
    assert [e["text"] for e in reasoning] == ["**Checking probes**\nThen rollout"]
    assert all(e["text"] != "reasoning (not summarized)" for e in entries)


def test_exec_cmd_is_extracted_from_javascript(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    view.write(dest)
    entries = _transcript(dest, f"{DIR[:12]}-responder.jsonl")[1:]
    tools = [e for e in entries if e["role"] == "tool"]
    assert tools[0] == {"role": "tool", "title": "exec_command", "text": 'kubectl get pods -n ns "x"\nls'}
    assert (tools[1]["title"], tools[1]["text"]) == ("exec", "text(1)")  # no exec_command: the JS itself
    assert (tools[2]["title"], tools[2]["text"]) == ("wait", '{"cell_id":"1"}')


def test_exec_commands_handles_quotes_and_several_calls() -> None:
    js = (
        "const a = await tools.exec_command({cmd: 'echo \\'hi\\'', yield_time_ms: 1});\n"
        'await tools.exec_command({cmd:"ls"});'
    )
    assert view.exec_commands(js) == ["echo 'hi'", "ls"]
    assert view.exec_commands("text(1)") == []
    assert view.exec_commands('tools.exec_command({cmd:"unterminated') == []


def test_outputs_are_truncated_with_a_note(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    view.write(dest)
    entries = _transcript(dest, f"{DIR[:12]}-responder.jsonl")[1:]
    outputs = [e for e in entries if e["role"] == "output"]
    assert outputs[0]["text"].startswith("Script completed\n")
    assert "[output truncated: " in outputs[0]["text"]
    assert len(outputs[0]["text"]) < 4200
    assert outputs[1]["text"] == "plain output"


def test_reasoning_summaries_are_shown_when_present(tmp_path: Path) -> None:
    item = {"type": "reasoning", "summary": [{"type": "summary_text", "text": "Checking probes"}]}
    path = tmp_path / "rollout-a.jsonl"
    path.write_text(json.dumps({"type": "response_item", "payload": item}) + "\n")
    [turn] = view.parse_rollout(path).turns
    assert turn.entries == [view.Entry("reasoning", "Checking probes")]


def test_a_separate_reflection_session_is_its_own_transcript(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    second = dest / "evidence" / "incidents" / DIR / "codex" / "sessions" / "rollout-z.jsonl"
    rows = [
        {"type": "event_msg", "payload": {"type": "task_started"}},
        _msg("user", "Reflect on the incident"),
        _msg("assistant", "done", "output_text"),
    ]
    second.write_text("".join(json.dumps(r) + "\n" for r in rows))
    files = [p["file"] for p in view.write(dest)["panels"] if p["type"] == "transcript"]
    assert any(f.endswith("-reflection-2.jsonl") for f in files)


# ---- redaction ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "secret"),
    [
        ('{"token": "abc123secretvalue"}', "abc123secretvalue"),
        ('{"api_key":"k-9f8e7d6c5b4a"}', "k-9f8e7d6c5b4a"),
        ('{\\"password\\":\\"p4ss w0rd\\"}', "p4ss w0rd"),
        ("password: hunter2hunter2", "hunter2hunter2"),
        ("DB_PASSWD=s3cr3tpw", "s3cr3tpw"),
        ("client_secret = abcdefgh12345", "abcdefgh12345"),
        ("Authorization: Bearer abcDEF123456789", "abcDEF123456789"),
        ("curl -H 'Authorization: Basic dXNlcjpwYXNzd29yZA=='", "dXNlcjpwYXNzd29yZA"),
        ("Cookie: session=abcdef1234567890", "abcdef1234567890"),
        ("got Bearer Zm9vYmFyMTIzNDU2", "Zm9vYmFyMTIzNDU2"),
        ("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ.c2lnbmF0dXJl end", "eyJhbGciOiJIUzI1NiJ9"),
        ("key sk-proj-abcdefghijklmnop1234", "sk-proj-abcdefghijklmnop1234"),
        ("gh ghp_abcdefghijklmnopqrstuvwx", "ghp_abcdefghijklmnopqrstuvwx"),
        ("gh github_pat_11ABCDEFG0abcdefghijkl_mnop", "github_pat_11ABCDEFG0abcdefghijkl_mnop"),
        ("slack xoxb-1234567890-abcdefghij", "xoxb-1234567890-abcdefghij"),
        ("aws AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"),
        ("-----BEGIN RSA PRIVATE KEY-----\nMIIEow\nIBAAKCAQEA\n-----END RSA PRIVATE KEY-----", "MIIEow"),
        ("client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQo", "LS0tLS1CRUdJTiBSU0Eg"),
        ("  data: QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5", "QUJDREVGR0hJSktM"),
    ],
)
def test_redact_removes_credentials(text: str, secret: str) -> None:
    out = view.redact(text)
    assert secret not in out
    assert view.REDACTED in out


def test_redact_removes_the_sessions_identities_everywhere() -> None:
    out = view.redact(f"user {USER} acct {ACCOUNT} again {ACCOUNT}", {USER, ACCOUNT})
    assert USER not in out
    assert ACCOUNT not in out
    assert out.count(view.REDACTED) == 3


def test_redact_keeps_ordinary_text() -> None:
    for text in (
        "Basic configuration of the service",
        "max_output_tokens:6000",
        '"output_tokens": 125',
        "kubectl get secret -n ns",
        "kind: Secret\nmetadata:\n  name: db-creds\ndata:\n  secretKeyRef:",
        "Deployment user: container running but not ready",
    ):
        assert view.redact(text) == text


def test_written_files_hold_no_credentials(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    ledger = _ledger()
    ledger["outcome_document"]["repair_actions"][0]["summary"] = f"token: s3cretvalue {ACCOUNT}"
    (dest / "evidence" / "incidents" / DIR / "ledger.json").write_text(json.dumps(ledger))
    view.write(dest)
    text = (dest / "view.json").read_text() + "".join(p.read_text() for p in (dest / "view").rglob("*.jsonl"))
    assert "s3cretvalue" not in text
    assert ACCOUNT not in text
    assert USER not in text


# ---- limits and missing pieces -------------------------------------------------------------------------------------


def test_missing_evidence_and_memory_still_make_a_valid_view(tmp_path: Path) -> None:
    dest = tmp_path / "run" / "operator"
    dest.mkdir(parents=True)
    doc = view.write(dest)
    _assert_valid(dest, doc)
    assert doc["summary"]["incidents"] == 0
    assert doc["summary"]["auto_applied"] == "n/a"
    assert doc["events"] == []
    assert [p["title"] for p in doc["panels"]] == ["Incidents", "Repair actions", "Learned in this run"]


def test_a_ledger_without_a_rollout_or_memory_is_valid(tmp_path: Path) -> None:
    dest = _collected(tmp_path, rollout=False, memory=False)
    doc = view.write(dest)
    _assert_valid(dest, doc)
    assert not (dest / "view" / "transcripts").exists()


def test_the_view_enforces_the_contract_limits() -> None:
    with pytest.raises(view.ViewError):
        view.View(summary={f"k{i}": i for i in range(21)})
    with pytest.raises(view.ViewError):
        view.View(summary={"Bad-Key": 1})
    with pytest.raises(view.ViewError):
        view.View(summary={"k": "x" * 201})
    with pytest.raises(view.ViewError):
        view.View(hide=["x"] * 51)
    with pytest.raises(view.ViewError):
        view.View(events=[view.Event("2026-11-11T08:00:00Z", f"lane{i}", "t") for i in range(7)])
    with pytest.raises(view.ViewError):
        view.Event("not a time", "detect", "t")
    with pytest.raises(view.ViewError):
        view.Event("2026-11-11T08:00:00Z", "x" * 25, "t")
    with pytest.raises(view.ViewError):
        view.Event("2026-11-11T08:00:00Z", "detect", "t" * 501)
    with pytest.raises(view.ViewError):
        view.TablePanel("t", ["a", "b"], [["only one"]])
    with pytest.raises(view.ViewError):
        view.TablePanel("t", [str(i) for i in range(21)], [])
    for bad in ("../x", "/etc/passwd", "a/../b", ""):
        with pytest.raises(view.ViewError):
            view.TranscriptPanel("t", bad)
        with pytest.raises(view.ViewError):
            view.FileCell("t", bad)


def test_check_files_names_a_missing_file(tmp_path: Path) -> None:
    doc = view.View(panels=[view.TranscriptPanel("t", "view/transcripts/none.jsonl")])
    with pytest.raises(view.ViewError, match="none.jsonl"):
        doc.check_files(tmp_path)


def test_long_texts_and_many_events_are_clipped_when_written(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    ledger = _ledger()
    ledger["outcome_document"]["repair_actions"] = [
        {
            "kind": "k",
            "target": "Deployment/ns/user",
            "summary": "s" * 900,
            "started_at": "2026-11-11T08:02:00Z",
        }
        for _ in range(2100)
    ]
    (dest / "evidence" / "incidents" / DIR / "ledger.json").write_text(json.dumps(ledger))
    doc = view.write(dest)
    _assert_valid(dest, doc)
    assert len(doc["events"]) == 2000
    assert all(len(e["text"]) <= 500 for e in doc["events"])


# ---- the CLI ------------------------------------------------------------


def _snapshot(run: Path) -> dict[str, bytes]:
    """Every file of the run directory except the view outputs."""
    return {
        p.relative_to(run).as_posix(): p.read_bytes()
        for p in run.rglob("*")
        if p.is_file()
        and p.relative_to(run).as_posix() != "operator/view.json"
        and "operator/view/" not in p.as_posix()
    }


def test_cli_rewrites_only_the_view_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    dest = _collected(tmp_path)
    run = dest.parent
    (dest / "view").mkdir()
    (dest / "view" / "stale.txt").write_text("old")
    (dest / "view.json").write_text("{}")
    (dest / "collect.json").write_text('{"parts": {}}')
    (run / "run.log").write_text("keep")
    before = _snapshot(run)
    assert view.main([str(run)]) == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 1
    assert "1 incidents" in out[0]
    assert "2 transcripts" in out[0]
    assert _snapshot(run) == before
    assert not (dest / "view" / "stale.txt").exists()
    _assert_valid(dest, _read(dest))


def test_cli_reports_a_run_without_operator_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    good = _collected(tmp_path).parent
    bad = tmp_path / "empty"
    bad.mkdir()
    assert view.main([str(bad), str(good)]) == 1
    lines = capsys.readouterr().out.splitlines()
    assert "no operator directory" in lines[0]
    assert "1 incidents" in lines[1]


def test_redaction_is_linear_in_the_text() -> None:
    """Long command output (a word of 200 kB) is redacted in well under a second: no pattern rescans a word from each
    of its positions."""
    import time

    text = "x" * 200_000 + " password=hunter2"
    start = time.monotonic()
    out = view.redact(text)
    assert time.monotonic() - start < 1.0
    assert out.endswith("password=" + view.REDACTED)


# ---- the agent's recorded calls ------------------------------------------------------------------------------

NS = {"version": "v1", "namespace": "ns"}


def _call(request: str, at: str, end: str | None = None, **fields: Any) -> list[dict[str, Any]]:
    """A recorded request: its start and, unless ``end`` is None, its end, as the sink mirrors the recording."""
    start = {"request_id": request, "phase": "start", "started": at, **NS, **fields}
    if end is None:
        return [start]
    outcome = {k: v for k, v in fields.items() if k in ("status", "omission", "changed_paths", "session_end")}
    return [start, {**start, "phase": "end", "finished": end, "status": 200, **outcome}]


def _recording(dest: Path, *events: dict[str, Any]) -> None:
    history = dest / "evidence" / "incidents" / DIR / "api-history"
    history.mkdir(parents=True)
    (history / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    (history / "manifest.json").write_text(json.dumps({"incident": INCIDENT, "write_gaps": 0}))


def _writes() -> list[dict[str, Any]]:
    patch = {"verb": "patch", "group": "apps", "resource": "deployments", "name": "user"}
    image = ["spec.template.spec.containers[api].image"]
    pod = {"resource": "pods", "name": "db-0"}
    config = {"verb": "patch", "resource": "configmaps", "name": "cfg"}
    create = {"verb": "post", "resource": "pods", "name": "x"}
    return [
        *_call("r1", "2026-11-11T08:01:30Z", "2026-11-11T08:01:31Z", **patch, changed_paths=image),
        *_call("r2", "2026-11-11T08:01:40Z", "2026-11-11T08:01:41Z", **config, status=403),
        *_call("r3", "2026-11-11T08:01:42Z", "2026-11-11T08:01:42Z", **create, status=403, omission="scope_read_only"),
        *_call(
            "r4", "2026-11-11T08:01:45Z", "2026-11-11T08:01:45Z", verb="get", **pod, subresource="exec", container="db",
            command=["psql", "<withheld>", "<withheld>"], status=101, session_end="2026-11-11T08:02:15Z",
        ),
        *_call(
            "r5", "2026-11-11T08:01:50Z", "2026-11-11T08:01:50Z", verb="post", **pod, subresource="portforward",
            ports=["5432"], status=101, session_end="2026-11-11T08:01:58Z",
        ),
        *_call("r9", "2026-11-11T08:02:20Z", verb="delete", resource="pods", name="gone"),
    ]  # fmt: skip


def _reads() -> list[dict[str, Any]]:
    return [
        *_call("r6", "2026-11-11T08:01:05Z", "2026-11-11T08:01:05Z", verb="get", resource="pods"),
        *_call("r7", "2026-11-11T08:01:20Z", "2026-11-11T08:01:20Z", verb="get", resource="pods"),
        *_call("r8", "2026-11-11T08:01:25Z", "2026-11-11T08:01:25Z", verb="list", resource="events"),
    ]


def test_recorded_writes_execs_and_reads_fill_the_act_and_read_lanes(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    _recording(dest, *_writes(), *_reads())
    doc = view.write(dest)
    _assert_valid(dest, doc)
    link = f"evidence/incidents/{DIR}/api-history/events.jsonl"
    acts = [e for e in doc["events"] if e["lane"] == "act" and e.get("file") == link]
    assert [e["text"] for e in acts] == [
        "api-patch Deployment/user: spec.template.spec.containers[api].image",
        "api-patch ConfigMap/cfg (failed 403)",
        "api-create Pod/x (refused: scope_read_only)",
        "exec Pod/db-0 [db]: psql <withheld> <withheld>",
        "port-forward Pod/db-0: 5432",
        "api-delete Pod/gone (open)",
    ]
    assert acts[0]["component"] == "Deployment/user"
    assert acts[3]["end"] == "2026-11-11T08:02:15Z"  # an exec spans its session
    reads = [(e["at"], e["text"]) for e in doc["events"] if e["lane"] == "read"]
    assert reads == [("2026-11-11T08:01:00Z", "3 reads: get pods \u00d72, list events \u00d71")]


def test_the_recording_draws_agent_writes_once(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    ledger = dest / "evidence" / "incidents" / DIR / "ledger.json"
    document = json.loads(ledger.read_text())
    plan = {"kind": "program-plan", "target": "plan", "summary": "applied", "success": True}
    document["outcome_document"]["repair_actions"].append({**plan, "started_at": "2026-11-11T08:01:20Z"})
    ledger.write_text(json.dumps(document))
    _recording(dest, *_writes())
    doc = view.write(dest)
    acts = [e["text"] for e in doc["events"] if e["lane"] == "act"]
    assert not any(t.startswith("kubectl-patch") for t in acts)  # a kubectl receipt is the same write
    assert any(t.startswith("program-plan") for t in acts)  # Go's own writes do not pass the proxy
    assert any(t.startswith("api-patch Deployment/user") for t in acts)
    assert len({e["lane"] for e in doc["events"]}) <= view.MAX_LANES


def test_without_a_recording_the_repair_actions_still_draw_the_act_lane(tmp_path: Path) -> None:
    dest = _collected(tmp_path)
    doc = view.write(dest)
    acts = [e["text"] for e in doc["events"] if e["lane"] == "act"]
    assert acts == ["kubectl-patch Deployment/ns/user: restore probe"]
    assert not [e for e in doc["events"] if e["lane"] == "read"]
