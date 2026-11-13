from pathlib import Path

import pytest
from google.protobuf.any_pb2 import Any

import sdo.agents.responder.worker as worker
from sdo.agents.code_paths import skills_for_role
from sdo.agents.responder import tools_cli
from sdo.agents.responder.codex import _incident_result_schema
from sdo.agents.responder.investigation import investigation_prompt
from sdo.agents.responder.worker import run_worker_task, task_role
from sdo.contracts import AgentTask, IncidentRequest, IncidentResult, parse_json, to_canonical_json

FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"
BRIEF = "Node: db\nHypothesis: the failure is caused by X at node db."


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: tuple[str, str, str] | None = None

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        self.published = (name, key, payload)


def _write_task(path: Path, brief: str) -> None:
    request = parse_json((FIXTURES / "incident_request.json").read_text(), IncidentRequest())
    request.memory_snapshot.accepted_commit = request.repository_base_commit
    request.memory_snapshot.goal_markdown = "Example objective"
    request.memory_snapshot.architecture_markdown = "Example architecture"
    for finding in request.findings:
        finding.playbook_ids[:] = ["missing-configmap"]
    for playbook in request.surfaced_playbooks:
        playbook.playbook_id = "missing-configmap"
    request.investigator_brief = brief
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="task-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": request.repository_worktree},
    )
    path.write_text(to_canonical_json(task), encoding="utf-8")


def test_a_task_with_a_brief_runs_the_investigator_role_and_returns_only_report_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "agent-task.json"
    _write_task(path, "")
    assert task_role(path) == "responder"
    _write_task(path, BRIEF)
    assert task_role(path) == "investigator"
    seen: list[str] = []

    def investigate(request: IncidentRequest) -> IncidentResult:
        seen.append(request.investigator_brief)
        return IncidentResult(
            schema_version="sdo.dev/v1alpha1",
            incident_id=request.incident_id,
            status="completed",
            investigation_report="Verdict: supported - X\nHow to diagnose: select.",
        )

    monkeypatch.setattr(worker, "execute_investigation", investigate)
    envelope = run_worker_task(
        path, "result", RecordingPublisher(), executor=lambda _r: pytest.fail("not the responder")
    )
    assert seen == [BRIEF]
    result = IncidentResult()
    envelope.response.Unpack(result)
    assert result.investigation_report.startswith("Verdict: supported")
    assert not result.confirmed_root_causes
    assert not result.repair_actions


def test_the_investigator_prompt_is_the_go_brief_plus_the_skill_path() -> None:
    prompt = investigation_prompt(IncidentRequest(investigator_brief="Node: db\nRead sdo-investigator/SKILL.md first."))
    assert prompt.startswith("Node: db")
    assert "sdo-investigator/SKILL.md" in prompt.splitlines()[-1]


def test_the_investigator_role_ships_only_its_skill_and_the_responder_schema_omits_the_report() -> None:
    assert skills_for_role("investigator") == ("sdo-investigator",)
    assert "investigation_report" not in _incident_result_schema()["properties"]  # type: ignore[operator]


def test_investigations_prints_the_go_text_and_changes_nothing(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    seen: list[dict[str, object]] = []

    def exchange(request: dict[str, object], _directory: object) -> dict[str, object]:
        seen.append(request)
        return {"command": "investigations", "text": "investigators: 1 started\n== node db: done\nVerdict: supported\n"}

    monkeypatch.setattr(tools_cli, "_exchange", exchange)
    assert tools_cli.main(["investigations"]) == 0
    assert seen == [{"command": "investigations"}]
    assert "Verdict: supported" in capsys.readouterr().out
    assert "investigations" in tools_cli.RESPONSE_FIELDS
    assert "investigations" not in tools_cli.MUTATION_COMMANDS
