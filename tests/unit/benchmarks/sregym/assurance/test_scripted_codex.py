"""The scripted Codex CLI replaces only the model: SDO's real responder, reflection and accounting run above it.

These tests put the scripted ``codex`` first on PATH and drive SDO's real
``execute_incident`` and ``CodexSessionBackend`` through agentshim's host
executor, with a stub ``kubectl`` standing in for the cluster.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as ProtoAny

from benchmarks.sregym.assurance.scripted_codex.directive import Directive
from benchmarks.sregym.assurance.scripted_codex.usage import request_usage
from sdo.agents.responder.codex import execute_incident
from sdo.agents.responder.reflection import CodexSessionBackend
from sdo.contracts import AgentTask, IncidentRequest, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parents[5]
SEED = REPO_ROOT / "tests" / "fixtures" / "sdo" / "contracts" / "incident_request.json"
NAMESPACE = "hotel-reservation"
POLICY = "deny-all-recommendation"

_KUBECTL = r"""#!/bin/sh
state="$SCRIPTED_TEST_STATE"
case "$*" in
  *"get networkpolicy -o json"*)
    if [ -f "$state/deleted" ]; then echo '{"items": []}'; else cat "$state/policies.json"; fi ;;
  *"delete networkpolicy"*) touch "$state/deleted"; echo "networkpolicy deleted" ;;
  *"rollout restart"*) echo "deployment restarted" ;;
  *"rollout status"*) echo "successfully rolled out" ;;
  *"get pods"*) echo "recommendation-1 1/1 Running" ;;
  *) echo "unexpected kubectl $*" >&2; exit 1 ;;
esac
"""


def _executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _memory_worktree(tmp_path: Path) -> Path:
    """A proposal checkout copied from the committed detector fixture."""

    worktree = tmp_path / "worktree"
    shutil.copytree(REPO_ROOT / "tests" / "fixtures" / "sdo" / "missing_configmap_app", worktree)
    subprocess.run(["git", "init", "-q", str(worktree)], check=True)
    subprocess.run(["git", "-C", str(worktree), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(worktree), "-c", "user.name=t", "-c", "user.email=t@e", "commit", "-qm", "seed"],
        check=True,
    )
    for name, body in {
        "goal.md": "---\nschema_version: 1\nowner: human\n---\nKeep healthy.\n",
        "arch.md": "---\nschema_version: 1\nowner: health_judge\n---\nOne service.\n",
        "playbooks/README.md": "# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n",
    }.items():
        (worktree / ".sdo" / name).write_text(body)
    existing = worktree / ".sdo/playbooks/missing-configmap/README.md"
    existing.write_text(
        "---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\n"
        "originating_incident: fixture\n---\n\n" + existing.read_text().split("---\n", 2)[-1]
    )
    return worktree


@pytest.fixture
def scripted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, memory_binary: Path) -> dict[str, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _executable(
        bin_dir / "codex",
        f'#!/bin/sh\nPYTHONPATH="{REPO_ROOT}" exec "{sys.executable}" '
        '-m benchmarks.sregym.assurance.scripted_codex "$@"\n',
    )
    _executable(bin_dir / "kubectl", _KUBECTL)
    (bin_dir / "sdo-operator").symlink_to(memory_binary)
    state = tmp_path / "cluster"
    state.mkdir()
    policy = {
        "metadata": {"name": POLICY},
        "spec": {
            "podSelector": {"matchLabels": {"io.kompose.service": "recommendation"}},
            "policyTypes": ["Ingress", "Egress"],
        },
    }
    (state / "policies.json").write_text(json.dumps({"items": [policy]}), encoding="utf-8")
    store = tmp_path / "store"
    store.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("SCRIPTED_TEST_STATE", str(state))
    monkeypatch.setenv("SDO_SCRIPTED_STATE_DIR", str(store))
    monkeypatch.setenv("SDO_SCRIPTED_STATUS_COMMAND", f'test -f "{state}/deleted"')
    monkeypatch.setenv("SDO_SCRIPTED_STATUS_POLL_SECONDS", "0")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex-home"))
    monkeypatch.setenv("SDO_TURN_USAGE_LOG", str(tmp_path / "turns.jsonl"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    return {"store": store, "state": state, "turns": tmp_path / "turns.jsonl", "tmp": tmp_path}


def _request(worktree: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> IncidentRequest:
    payload = json.loads(SEED.read_text(encoding="utf-8"))
    payload.update(
        {
            "incident_id": "Hotel Reservation-42",
            "namespace": NAMESPACE,
            "repository_worktree": str(worktree),
            "surfaced_playbooks": [],
            "memory_snapshot": {
                "accepted_commit": payload["repository_base_commit"],
                "goal_markdown": "The hotel service must serve recommendation requests.",
                "architecture_markdown": "Recommendation is a user-facing service.",
            },
            "findings": [
                {
                    **payload["findings"][0],
                    "detector_id": "traffic-health",
                    "rule_id": "scenario-slo.hotel-recommendations",
                    "summary": "hotel-recommendations fails its SLO",
                    "fingerprint": "traffic-health/hotel-recommendations",
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


def _directive(store: Path, **overrides: object) -> Directive:
    directive = Directive(scenario="unit", fault="network_policy_block", target="recommendation", usage_seed=3)
    directive = Directive(**{**directive.__dict__, **overrides})
    (store / "directive.json").write_text(directive.to_json(), encoding="utf-8")
    return directive


def _turn_records(store: Path) -> list[dict[str, object]]:
    return sorted(
        (json.loads(path.read_text(encoding="utf-8")) for path in (store / "turns").glob("*.json")),
        key=lambda record: str(record["started_at"]),
    )


def _expected_usage(seed: int, kind: str, requests: int) -> dict[str, int]:
    usages = [request_usage(seed, kind, index) for index in range(requests)]
    return {
        "input_tokens": sum(u.input_tokens for u in usages),
        "cached_input_tokens": sum(u.cached_input_tokens for u in usages),
        "output_tokens": sum(u.output_tokens for u in usages),
        "reasoning_output_tokens": sum(u.reasoning_output_tokens for u in usages),
    }


def test_scripted_responder_repairs_through_the_real_structured_turn(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch, prepare_reflection_memory
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"])

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status == "completed"
    (cause,) = result.confirmed_root_causes
    assert {(ref.kind, ref.name) for ref in cause.resources} >= {("NetworkPolicy", POLICY)}
    assert cause.explained_detectors == ["traffic-health"]
    assert [action.success for action in result.repair_actions] == [True]
    # The repair names what it mutated, so SDO can attribute the confirmed cause to it.
    assert ("NetworkPolicy", POLICY) in {(ref.kind, ref.name) for ref in result.repair_actions[0].resources}
    assert result.verification_evidence[0].name == "sdo-incident-status"
    assert result.verification_evidence[0].passed is True
    (turn,) = _turn_records(scripted["store"])
    assert turn["session_id"] == result.responder_session_id
    expected = _expected_usage(3, "responder", int(turn["requests"]))
    assert turn["usage"] == expected
    assert result.usage.input_tokens == expected["input_tokens"]
    assert result.usage.cache_read_input_tokens == expected["cached_input_tokens"]
    assert result.usage.output_tokens == expected["output_tokens"]
    assert result.usage.reasoning_output_tokens == expected["reasoning_output_tokens"]
    assert result.usage.model_requests == turn["requests"]
    (logged,) = [json.loads(line) for line in scripted["turns"].read_text(encoding="utf-8").splitlines()]
    assert logged["model"] == "gpt-6-luna"
    assert logged["provider"] == "codex"
    assert logged["usage"]["input_tokens"] == expected["input_tokens"]


def test_wrong_then_correct_reports_the_failed_attempt_and_the_repair(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch, prepare_reflection_memory
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], mitigation="wrong_then_correct")

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status == "completed"
    assert [(action.action_id, action.success) for action in result.repair_actions] == [
        ("wrong-repair", False),
        ("repair-1", True),
    ]


def test_wrong_only_honest_fails_and_claimed_lies_about_verification(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch, prepare_reflection_memory
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], mitigation="wrong_only_honest")
    honest = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert honest.status == "failed"
    assert honest.confirmed_root_causes == []

    shutil.rmtree(scripted["store"] / "bindings")
    _directive(scripted["store"], mitigation="wrong_only_claimed")
    claimed = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert claimed.status == "completed"
    assert claimed.verification_evidence[0].passed is True
    assert not (scripted["state"] / "deleted").exists()
    # The wrong repair truthfully records that it restarted frontend, not the NetworkPolicy.
    (wrong,) = claimed.repair_actions
    assert [(ref.kind, ref.name) for ref in wrong.resources] == [("Deployment", "frontend")]


def _prepare_policy_reflection(
    worktree: Path,
    incident_id: str,
    prepare_reflection_memory: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Controller-owned opening, repair and fresh-clear evidence for the toy policy."""
    resource = {"api_version": "networking.k8s.io/v1", "kind": "NetworkPolicy", "namespace": NAMESPACE, "name": POLICY}
    policy = {
        "metadata": {"name": POLICY, "namespace": NAMESPACE},
        "spec": {
            "podSelector": {"matchLabels": {"io.kompose.service": "recommendation"}},
            "policyTypes": ["Ingress", "Egress"],
        },
    }
    peer = {
        **policy,
        "metadata": {"name": "allow-recommendation", "namespace": NAMESPACE},
        "spec": {**policy["spec"], "ingress": [{}]},
    }
    opened = json.dumps({"namespace": NAMESPACE, "networkPolicies": [policy, peer]}).encode()
    fixed = json.dumps({"namespace": NAMESPACE, "networkPolicies": [peer]}).encode()
    # memory.CaptureRepairReceipts copies the original hand-repair receipt into
    # captured recordings; the positive case links it to the learned program.
    receipt = {
        "id": "delete-policy",
        "incident_id": incident_id,
        "success": True,
        "started_at": "2026-01-01T00:00:01Z",
        "completed_at": "2026-01-01T00:00:02Z",
        "resources": [resource],
    }
    recordings = json.dumps([{"method": "RepairReceipt", "arguments": [], "result": receipt}]).encode()
    corpus = worktree / ".sdo/corpus/deny-all-networkpolicy-isolation"
    corpus.mkdir(parents=True)
    for name, content in {
        "open.json": opened,
        "fixed.json": fixed,
        "healthy.json": fixed,
        "recordings.json": recordings,
    }.items():
        (corpus / name).write_bytes(content)
    (corpus / "labels.json").write_text(
        json.dumps(
            {
                "root_cause_program_ids": ["deny-all-networkpolicy-isolation"],
                "bindings": {"policy": resource},
                "peer_bindings": {"policy": {**resource, "name": "allow-recommendation"}},
                "provenance": {
                    "incident_id": incident_id,
                    "open_at": "detect",
                    "open_sha256": hashlib.sha256(opened).hexdigest(),
                    "fixed_sha256": hashlib.sha256(fixed).hexdigest(),
                },
            }
        )
    )
    capture = worktree.parent / "policy-capture"
    (capture / "healthy").mkdir(parents=True)
    (capture / "healthy/healthy.json").write_bytes(fixed)
    (capture / "open@detect.json").write_bytes(opened)
    (capture / "fixed.json").write_bytes(fixed)
    (capture / "open@detect-recordings.json").write_bytes(recordings)
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    return prepare_reflection_memory(
        worktree,
        incident_id,
        outcome_record={
            "schema_version": 1,
            "incident_id": incident_id,
            "classification": "success",
            "confirmed_root_causes": [{"summary": "deny-all policy isolates recommendation", "resources": [resource]}],
            "findings": [
                {
                    "detector_id": "deny-all-networkpolicy-isolation",
                    "rule_id": "total-isolation",
                    "status": "active",
                    "severity": "critical",
                    "summary": "policy isolates recommendation",
                    "evidence": "deny-all rules select workload",
                    "primary_resource": resource,
                    "fingerprint": "policy-isolation",
                }
            ],
            "repair_actions": [
                {
                    "action_id": "delete-policy",
                    "kind": "kubectl",
                    "target": "NetworkPolicy/" + POLICY,
                    "summary": "remove isolation",
                    "details": "policy deleted and absence observed",
                    "success": True,
                    "resources": [resource],
                    "started_at": "2026-01-01T00:00:01Z",
                    "completed_at": "2026-01-01T00:00:02Z",
                }
            ],
            "detector_history": [
                {
                    "detector_id": "deny-all-networkpolicy-isolation",
                    "status": "clear",
                    "evaluated_at": "2026-01-01T00:00:03Z",
                }
            ],
        },
    )


def test_resumed_reflection_accounts_its_tokens_and_edits_final_files(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch, prepare_reflection_memory
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"])
    result = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert result.responder_session_id

    commit = _prepare_policy_reflection(worktree, request.incident_id, prepare_reflection_memory, monkeypatch)
    turn = CodexSessionBackend(model="gpt-6-luna").resume(
        session_id=result.responder_session_id,
        worktree=worktree,
        prompt="The controller has independently verified incident closure.",
        idempotency_key=f"reflection:{request.incident_id}:{commit}",
    )

    assert turn.learning_decision == "updated"
    responder, reflection = _turn_records(scripted["store"])
    assert reflection["session_id"] == responder["session_id"]
    assert reflection["resumed"] is True
    own = _expected_usage(3, "reflection-resume", int(reflection["requests"]))
    # `codex exec resume` reports the session's cumulative usage; SDO must count only the reflection's own delta.
    assert reflection["session_totals"]["input_tokens"] > own["input_tokens"]
    assert turn.usage["input_tokens"] == own["input_tokens"]
    assert turn.usage["output_tokens"] == own["output_tokens"]
    assert turn.usage["model_requests"] == reflection["requests"]
    assert turn.playbook_ids == ["deny-all-networkpolicy-isolation"]
    manifest = (
        worktree / ".sdo/diagnostics/detectors/incidents/deny-all-networkpolicy-isolation/manifest.yaml"
    ).read_text()
    assert f'originatingIncident: "{request.incident_id}"' in manifest
    assert (worktree / ".sdo/playbooks/deny-all-networkpolicy-isolation/README.md").exists()
    assert any(item["command"].startswith("sdo-operator memory check") for item in reflection["commands"])
    assert subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain"], capture_output=True, text=True, check=True
    ).stdout


def test_an_unplanned_turn_is_refused_loudly(scripted: dict[str, Path]) -> None:
    completed = subprocess.run(
        [str(scripted["tmp"] / "bin" / "codex"), "exec", "--json", "-"],
        input="You are SDO's health judge. Author detectors.",
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 3
    assert "no scripted plan" in completed.stderr


def test_missing_configmap_rebuilds_a_script_configmap_that_has_no_manifest(tmp_path: Path) -> None:
    """hotel_reservation creates ``mongo-geo-script`` from the root ``k8s-geo-mongo.sh``; no manifest defines it."""

    from benchmarks.sregym.assurance.scripted_codex.faults import MissingConfigMap

    worktree = tmp_path / "app"
    (worktree / "kubernetes" / "geo").mkdir(parents=True)
    (worktree / "kubernetes" / "geo" / "mongodb-geo-deployment.yaml").write_text(
        "kind: Deployment\nspec:\n  volumes:\n    - configMap:\n        name: mongo-geo-script\n", encoding="utf-8"
    )
    (worktree / "k8s-geo-mongo.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (worktree / "k8s-rate-mongo.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    deployment = {"spec": {"template": {"spec": {"volumes": [{"configMap": {"name": "mongo-geo-script"}}]}}}}

    def run(command: str, *, timeout: float = 60) -> tuple[int, str]:
        if "get deployment" in command:
            return 0, json.dumps(deployment)
        if "get configmap" in command:
            return 1, "NotFound"
        return 0, ""

    plan = MissingConfigMap()
    facts = plan.diagnose(run, NAMESPACE, "mongodb-geo", {}, worktree)
    (restore, _), _ = plan.correct_repair(facts)

    assert restore == f"kubectl -n {NAMESPACE} create configmap mongo-geo-script --from-file=k8s-geo-mongo.sh"
    assert plan.root_cause(facts, {})["static_context"] == ["k8s-geo-mongo.sh"]


def test_a_failed_plan_records_why_it_stopped(scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    _request(worktree, scripted["tmp"], monkeypatch)
    (scripted["state"] / "policies.json").write_text(json.dumps({"items": []}), encoding="utf-8")
    _directive(scripted["store"])
    request = _request(worktree, scripted["tmp"], monkeypatch)

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status == "failed"
    (turn,) = _turn_records(scripted["store"])
    assert turn["outcome"] == "failed"
    assert "scripted plan stopped" in str(turn["plan_error"])


def test_healed_noop_claims_completed_with_no_action_once_health_is_back(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The production shape: the finding healed without the responder, which truthfully reports doing nothing."""

    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], mitigation="healed_noop", status_attempts=2)
    unhealed = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert unhealed.status == "failed"
    assert unhealed.repair_actions == []

    shutil.rmtree(scripted["store"] / "bindings")
    (scripted["state"] / "deleted").write_text("", encoding="utf-8")  # someone else healed it
    _directive(scripted["store"], mitigation="healed_noop", status_attempts=2)
    healed = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert healed.status == "completed"
    assert healed.repair_actions == []
    assert healed.confirmed_root_causes == []
    assert healed.verification_evidence[0].passed is True


def test_scripted_reflection_fixes_local_diagnostics_in_the_same_checkout(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch, prepare_reflection_memory
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], reflection="invalid_then_learn")
    responder = execute_incident(request, model="gpt-6-luna", provider="codex")
    _prepare_policy_reflection(worktree, request.incident_id, prepare_reflection_memory, monkeypatch)
    turn = CodexSessionBackend(model="gpt-6-luna").resume(
        session_id=responder.responder_session_id,
        worktree=worktree,
        prompt="Reflect on the verified closure",
        idempotency_key=f"reflection:{request.incident_id}:{'a' * 40}",
    )
    assert turn.learning_decision == "updated"
    reflection = _turn_records(scripted["store"])[-1]
    checks = [
        command for command in reflection["commands"] if command["command"].startswith("sdo-operator memory check")
    ]
    assert [command["exit_code"] for command in checks] == [1, 0]
    assert (
        "malformed README" not in (worktree / ".sdo/playbooks/deny-all-networkpolicy-isolation/README.md").read_text()
    )
