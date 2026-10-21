"""The scripted Codex CLI replaces only the model: SDO's real responder, reflection and accounting run above it.

These tests put the scripted ``codex`` first on PATH and drive SDO's real
``execute_incident`` and ``CodexSessionBackend`` through agentshim's host
executor, with a stub ``kubectl`` standing in for the cluster.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.sregym.assurance.scripted_codex.directive import Directive
from benchmarks.sregym.assurance.scripted_codex.usage import request_usage
from sdo.agents.responder.codex import execute_incident
from sdo.agents.responder.reflection import CodexSessionBackend
from sdo.agents.views import (
    IncidentRequest,
    OperationalArtifactDraft,
)

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
    memory = worktree / ".sdo"
    (memory / "schema-version").write_text("1\n", encoding="utf-8")
    (memory / "goal.md").write_text(
        "---\nschema_version: 1\nowner: human\n---\n\nKeep the app healthy.\n", encoding="utf-8"
    )
    (memory / "arch.md").write_text(
        "---\nschema_version: 1\nowner: health_judge\n---\n\nOne service.\n", encoding="utf-8"
    )
    (memory / "playbooks" / "README.md").write_text(
        "# Playbooks\n\n- [missing-configmap](missing-configmap/README.md)\n", encoding="utf-8"
    )
    incidents = memory / "diagnostics" / "detectors" / "incidents"
    incidents.mkdir()
    (incidents.parent / "missing_configmap").rename(incidents / "missing_configmap")
    manifest = memory / "diagnostics" / "manifest.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            "./detectors/missing_configmap", "./detectors/incidents/missing_configmap", 1
        ),
        encoding="utf-8",
    )
    playbook = memory / "playbooks" / "missing-configmap" / "README.md"
    playbook.write_text(
        "---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\n"
        "originating_incident: fixture\n---\n\n" + playbook.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", str(worktree)], check=True)
    subprocess.run(["git", "-C", str(worktree), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(worktree), "-c", "user.name=t", "-c", "user.email=t@e", "commit", "-qm", "seed"],
        check=True,
    )
    return worktree


@pytest.fixture
def scripted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _executable(
        bin_dir / "codex",
        f'#!/bin/sh\nPYTHONPATH="{REPO_ROOT}" exec "{sys.executable}" '
        '-m benchmarks.sregym.assurance.scripted_codex "$@"\n',
    )
    _executable(bin_dir / "kubectl", _KUBECTL)
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
                    "playbooks": [],
                    "fingerprint": "traffic-health/hotel-recommendations",
                }
            ],
        }
    )
    request = IncidentRequest.model_validate(payload)
    path = tmp_path / "incident-request.json"
    path.write_text(request.model_dump_json(), encoding="utf-8")
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
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"])

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status.value == "completed"
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
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], mitigation="wrong_then_correct")

    result = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert result.status.value == "completed"
    assert [(action.action_id, action.success) for action in result.repair_actions] == [
        ("wrong-repair", False),
        ("repair-1", True),
    ]


def test_wrong_only_honest_fails_and_claimed_lies_about_verification(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"], mitigation="wrong_only_honest")
    honest = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert honest.status.value == "failed"
    assert honest.confirmed_root_causes == []

    shutil.rmtree(scripted["store"] / "bindings")
    _directive(scripted["store"], mitigation="wrong_only_claimed")
    claimed = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert claimed.status.value == "completed"
    assert claimed.verification_evidence[0].passed is True
    assert not (scripted["state"] / "deleted").exists()
    # The wrong repair truthfully records that it restarted frontend, not the NetworkPolicy.
    (wrong,) = claimed.repair_actions
    assert [(ref.kind, ref.name) for ref in wrong.resources] == [("Deployment", "frontend")]


def test_resumed_reflection_accounts_only_its_own_tokens_and_returns_logical_artifacts(
    scripted: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree = _memory_worktree(scripted["tmp"])
    request = _request(worktree, scripted["tmp"], monkeypatch)
    _directive(scripted["store"])
    result = execute_incident(request, model="gpt-6-luna", provider="codex")
    assert result.responder_session_id

    commit = "a" * 40
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
    proposal = turn.artifact_proposal
    assert proposal is not None
    assert proposal.detectors[0].id == "deny-all-networkpolicy-isolation"
    assert proposal.detectors[0].originating_incident == request.incident_id
    assert proposal.playbooks[0].id == "deny-all-networkpolicy-isolation"
    assert not subprocess.run(
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


@pytest.mark.parametrize("fault", ["network_policy_block", "missing_configmap"])
@pytest.mark.parametrize("claimed", [False, True], ids=["confirmed-cause", "claimed-cause"])
def test_every_scripted_memory_proposal_passes_the_detector_gateway(
    tmp_path: Path, fault: str, claimed: bool, operator_binary: Path
) -> None:
    from google.protobuf import json_format

    from benchmarks.sregym.assurance.scripted_codex.faults import PLANS, Facts
    from sdo.contracts._gen.sdodev.contracts.v1alpha1.messages_pb2 import OperationalArtifactProposal

    worktree = _memory_worktree(tmp_path)
    plan = PLANS[fault]
    build = plan.claimed_memory if claimed else plan.memory
    proposal = build(Facts(namespace="", target="x"), incident_id="Hotel Reservation-1", outcome_commit="a" * 40)
    draft = OperationalArtifactDraft.model_validate(proposal.artifact_proposal)
    assert draft.artifact_ids() == [proposal.detector_id, proposal.playbook_id]
    proto = json_format.ParseDict(proposal.artifact_proposal, OperationalArtifactProposal())
    proposal_path = tmp_path / "proposal.json"
    proposal_path.write_text(json_format.MessageToJson(proto), encoding="utf-8")
    gateway = subprocess.run(
        [
            str(operator_binary),
            "draft-test",
            "--app",
            str(worktree),
            "--proposal-json",
            str(proposal_path),
            "--operator-root",
            str(REPO_ROOT / "sdo" / "operator"),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT / "sdo" / "operator",
    )
    assert gateway.returncode == 0, gateway.stdout + gateway.stderr
    assert not subprocess.run(
        ["git", "-C", str(worktree), "status", "--porcelain"], capture_output=True, text=True, check=True
    ).stdout


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

    assert result.status.value == "failed"
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
    assert unhealed.status.value == "failed"
    assert unhealed.repair_actions == []

    shutil.rmtree(scripted["store"] / "bindings")
    (scripted["state"] / "deleted").write_text("", encoding="utf-8")  # someone else healed it
    _directive(scripted["store"], mitigation="healed_noop", status_attempts=2)
    healed = execute_incident(request, model="gpt-6-luna", provider="codex")

    assert healed.status.value == "completed"
    assert healed.repair_actions == []
    assert healed.confirmed_root_causes == []
    assert healed.verification_evidence[0].passed is True
