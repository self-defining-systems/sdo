"""The scripted program loop: SDO's real responder runs ``sdo`` tools and ``sdo act`` under the scripted Codex CLI.

Stub ``sdo`` read tools and ``kubectl`` stand in for the controller and cluster; ``sdo act`` is the real CLI, so its
receipts reach the result through SDO's own merge. The scripted Codex records what it was launched with.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as ProtoAny

from benchmarks.sregym.assurance.scripted_codex.cli import run
from benchmarks.sregym.assurance.scripted_codex.directive import Directive
from benchmarks.sregym.assurance.scripted_codex.store import DirectoryStore
from sdo.agents.responder.codex import execute_incident
from sdo.contracts import AgentTask, IncidentRequest, to_canonical_json

REPO_ROOT = Path(__file__).resolve().parents[5]
SEED = REPO_ROOT / "tests" / "fixtures" / "sdo" / "contracts" / "incident_request.json"
NAMESPACE = "toy"
PROGRAM = "service-targetport-mismatch"

_SDO = f"""#!/bin/sh
state="$SCRIPTED_TEST_STATE"
echo "$*" >> "$state/sdo-calls"
case "$1" in
  act) exec "{sys.executable}" -m sdo.agents.responder.tools_cli "$@" ;;
  diagnose) cat "$state/diagnose.json" ;;
  plan) echo '{{"plan_id": "plan-1", "summary": "planned"}}' ;;
  apply) echo '{{"application_id": "app-1", "summary": "applied"}}' ;;
  source) echo '{{"source": {{"spec": {{"ports": [{{"port": 80, "targetPort": 8080}}]}}}}, "summary": "source"}}' ;;
  verify) echo '{{"healthy": true, "summary": "healthy"}}' ;;
  *) echo "unexpected sdo $*" >&2; exit 1 ;;
esac
"""


def _executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@pytest.fixture
def toy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _executable(
        bin_dir / "codex",
        f'#!/bin/sh\nPYTHONPATH="{REPO_ROOT}" exec "{sys.executable}" '
        '-m benchmarks.sregym.assurance.scripted_codex "$@"\n',
    )
    _executable(bin_dir / "sdo", _SDO)
    _executable(bin_dir / "kubectl", '#!/bin/sh\necho "$*" >> "$SCRIPTED_TEST_STATE/kubectl-calls"\necho patched\n')
    state, store, scratch = tmp_path / "cluster", tmp_path / "store", tmp_path / "scratch"
    for directory in (state, store, scratch):
        directory.mkdir()
    (state / "diagnose.json").write_text(json.dumps({"program_runs": []}), encoding="utf-8")
    (store / "directive.json").write_text(
        Directive(scenario="e1", fault="service_target_port", target="toy", usage_seed=1).to_json(), encoding="utf-8"
    )
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("SCRIPTED_TEST_STATE", str(state))
    monkeypatch.setenv("SDO_SCRIPTED_STATE_DIR", str(store))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(scratch))
    monkeypatch.setenv("SDO_SCRIPTED_STATUS_COMMAND", "true")
    monkeypatch.setenv("SDO_SCRIPTED_STATUS_POLL_SECONDS", "0")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex-home"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    return {"state": state, "store": store, "tmp": tmp_path}


def _request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> IncidentRequest:
    payload = json.loads(SEED.read_text(encoding="utf-8"))
    payload.update(
        {
            "incident_id": "toy-1",
            "namespace": NAMESPACE,
            "repository_worktree": str(tmp_path),
            "surfaced_playbooks": [],
            "memory_snapshot": {
                "accepted_commit": payload["repository_base_commit"],
                "goal_markdown": "The toy frontend must serve requests.",
                "architecture_markdown": "Frontend calls backend.",
            },
            "state_changes": {
                "changes": [{"kind": "Service", "name": "backend", "change": "modified", "fields": []}],
            },
            "findings": [
                {
                    **payload["findings"][0],
                    "detector_id": "traffic-health",
                    "rule_id": "scenario-slo.toy-journey",
                    "summary": "toy journey fails",
                    "fingerprint": "traffic-health/toy-journey",
                    "primary_resource": {
                        "api_version": "v1",
                        "kind": "Service",
                        "namespace": NAMESPACE,
                        "name": "backend",
                    },
                }
            ],
        }
    )
    request = json_format.ParseDict(payload, IncidentRequest())
    packed = ProtoAny()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="task-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed,
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    monkeypatch.setenv("SDO_REQUEST_PATH", str(path))
    return request


def _turns(store: Path) -> list[dict[str, object]]:
    return sorted(
        (json.loads(path.read_text(encoding="utf-8")) for path in (store / "turns").glob("*.json")),
        key=lambda record: str(record["started_at"]),
    )


def _calls(state: Path) -> list[str]:
    return (state / "sdo-calls").read_text(encoding="utf-8").splitlines()


def test_without_an_installed_program_the_repair_is_one_recorded_sdo_act(
    toy: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(toy["tmp"], monkeypatch)

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status == "completed"
    (action,) = result.repair_actions
    assert action.success is True
    assert [(ref.kind, ref.name) for ref in action.resources] == [("Service", "backend")]
    (cause,) = result.confirmed_root_causes
    assert {(item.kind, item.source) for item in cause.evidence} >= {("state-change", "Service/backend")}
    assert not result.applied_playbooks
    assert [call.split()[0] for call in _calls(toy["state"])] == ["diagnose", "source", "act", "verify"]
    assert "patch service backend" in (toy["state"] / "kubectl-calls").read_text(encoding="utf-8")


def test_a_matching_program_is_planned_and_applied_not_repaired_by_hand(
    toy: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(toy["tmp"], monkeypatch)
    run_ = {
        "program_id": PROGRAM,
        "bindings": {"service": {"kind": "Service", "namespace": NAMESPACE, "name": "backend"}},
        "verdict": {"result": "Match"},
    }
    (toy["state"] / "diagnose.json").write_text(json.dumps({"program_runs": [run_]}), encoding="utf-8")

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status == "completed"
    assert [item.playbook_id for item in result.applied_playbooks] == [PROGRAM]
    assert not result.repair_actions
    assert [call.split()[0] for call in _calls(toy["state"])] == ["diagnose", "plan", "apply", "verify"]
    assert "--bind service=Service/toy/backend" in _calls(toy["state"])[1]
    assert not (toy["state"] / "kubectl-calls").exists()


def test_every_turn_records_its_argv_output_schema_digest_and_mcp_definitions(
    toy: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(toy["tmp"], monkeypatch)
    execute_incident(request, model="gpt-6-luna", provider="codex")

    (turn,) = _turns(toy["store"])

    assert turn["argv"][0] == "exec"
    assert len(str(turn["output_schema_sha256"])) == 64
    assert turn["output_schema_bytes"] > 0
    assert any("sdo_reflection" in token for token in turn["mcp_config"])  # type: ignore[union-attr]


def test_a_refine_prompt_is_confirmed_without_editing_memory(
    toy: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = toy["tmp"] / "checkout"
    worktree.mkdir()
    monkeypatch.chdir(worktree)
    import subprocess

    subprocess.run(["git", "init", "-q"], check=True)
    prompt = (
        "This is a confirm-or-refine reflection, not a full one.\nIdempotency key: reflection:toy-2:" + "a" * 40 + "\n"
    )

    assert run(["exec", "--json", "-"], stdin=prompt, store=DirectoryStore(toy["store"])) == 0

    (turn,) = _turns(toy["store"])
    assert turn["outcome"] == "no_change"
    assert [item["command"] for item in turn["commands"]] == ["git status --short"]  # type: ignore[union-attr]


def test_responder_and_resumed_reflection_launch_codex_with_identical_schema_and_mcp_definitions(
    toy: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    from sdo.agents.responder.reflection import CodexSessionBackend

    request = _request(toy["tmp"], monkeypatch)
    result = execute_incident(request, model="gpt-6-luna", provider="codex")
    worktree = toy["tmp"] / "checkout"
    worktree.mkdir()
    refine = (
        "This is a confirm-or-refine reflection, not a full one. Programs: "
        f"{PROGRAM}.\nIdempotency key: reflection:{request.incident_id}:{'a' * 40}\n"
    )

    turn = CodexSessionBackend(model="gpt-6-luna").resume(
        session_id=result.responder_session_id,
        worktree=worktree,
        prompt=refine,
        idempotency_key=f"reflection:{request.incident_id}:{'a' * 40}",
    )

    assert turn.learning_decision == "no_change"
    responder, reflection = _turns(toy["store"])
    assert reflection["resumed"] is True
    assert reflection["session_id"] == responder["session_id"]
    assert reflection["output_schema_sha256"] == responder["output_schema_sha256"]
    assert reflection["output_schema_bytes"] == responder["output_schema_bytes"]

    # The server's --root follows each turn's checkout; the tool set the model sees must not.
    def definitions(turn: dict[str, Any]) -> list[str]:
        return [
            token.replace(str(worktree), "<checkout>").replace(str(toy["tmp"]), "<checkout>")
            for token in turn["mcp_config"]
        ]

    assert definitions(reflection) == definitions(responder)
    assert reflection["mcp_config"]
