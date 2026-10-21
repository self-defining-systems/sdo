from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.replay import CheckpointError, CheckpointStore, capture_checkpoints, replay_broker
from sdo.contracts import (
    AgentResult,
    AgentTask,
    ProtoIncidentRequest,
    ProtoIncidentResult,
    ReflectionRequest,
    ReflectionResult,
)
from sdo.contracts.proto import parse_json, to_canonical_json


def _task_and_result() -> tuple[str, str]:
    from google.protobuf.any_pb2 import Any as ProtoAny

    request = ProtoIncidentRequest(schema_version="sdo.dev/v1alpha1", incident_id="incident-1")
    response = ProtoIncidentResult(schema_version="sdo.dev/v1alpha1", incident_id="incident-1")
    packed_request, packed_result = ProtoAny(), ProtoAny()
    packed_request.Pack(request)
    packed_result.Pack(response)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="incident-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed_request,
        workspace={"proposal_id": "incident-1", "checkout_path": "/workspace/incident"},
    )
    result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id="incident-1",
        proposal_id="incident-1",
        status="completed",
        response=packed_result,
    )
    return to_canonical_json(task), to_canonical_json(result)


def test_checkpoint_round_trips_exact_protojson_and_digest_capture(tmp_path: Path) -> None:
    request, result = _task_and_result()
    store = CheckpointStore(tmp_path)
    capture = b'{"namespace_name":"hotel","services":[]}'

    store.save(
        0, "responder", request=request, result=result, captures={"open.json": capture}, broker={"accepted": True}
    )
    restored = store.load(0, "responder")

    assert restored.request == request
    assert restored.result == result
    assert restored.captures == {"open.json": capture}
    assert restored.broker == {"accepted": True}
    assert not (tmp_path / "checkpoints" / "000" / "responder" / "captures" / "open.json").exists()
    assert (tmp_path / "checkpoints").stat().st_mode & 0o777 == 0o700


def test_captured_kubernetes_api_serves_accepted_source_for_repair() -> None:
    from benchmarks.sregym.fastloop.replay_worker import CapturedKubernetesAPI

    opened = {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"port": 80, "targetPort": 9999}]}}]}
    fixed = {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"port": 80, "targetPort": 8080}]}}]}
    api = CapturedKubernetesAPI(
        {"open@pre-apply.json": json.dumps(opened).encode(), "fixed.json": json.dumps(fixed).encode()}
    )

    assert api.tool("source", "Service/hotel/front")["source"]["spec"]["ports"][0]["targetPort"] == 8080
    with pytest.raises(CheckpointError, match="uncaptured source"):
        api.tool("source", "Service/hotel/missing")


def test_checkpoint_rejects_tampered_capture(tmp_path: Path) -> None:
    request, result = _task_and_result()
    store = CheckpointStore(tmp_path)
    store.save(0, "responder", request=request, result=result, captures={"open.json": b"{}"})
    (tmp_path / "checkpoints" / "blobs" / store.load(0, "responder").capture_digests["open.json"]).write_bytes(b"bad")

    with pytest.raises(CheckpointError, match="digest"):
        store.load(0, "responder")


def test_checkpoint_retains_oversized_reflection_result_for_broker_replay(tmp_path: Path) -> None:
    request, result = _task_and_result()
    envelope = parse_json(result, AgentResult())
    envelope.response.Pack(ReflectionResult(summary="x" * (700 << 10), learning_decision="no_change"))
    store = CheckpointStore(tmp_path)
    store.save(0, "broker", request=request, result=to_canonical_json(envelope))

    assert store.load(0, "broker").result is not None
    with pytest.raises(CheckpointError, match="transport limit"):
        replay_broker(store, 0, source=tmp_path, operator_root=Path(__file__).resolve().parents[5] / "sdo/operator")


def test_checkpoint_accepts_camel_case_protojson_result(tmp_path: Path) -> None:
    request, result = _task_and_result()
    camel = (
        json.dumps(json.loads(result), separators=(",", ":"))
        .replace("task_id", "taskId")
        .replace("proposal_id", "proposalId")
    )
    store = CheckpointStore(tmp_path)

    store.save(0, "responder", request=request, result=camel)

    assert store.load(0, "responder").result == camel


def test_checkpoint_rejects_inconsistent_task_result(tmp_path: Path) -> None:
    request, result = _task_and_result()
    wrong = json.loads(result)
    wrong["task_id"] = "another-task"

    with pytest.raises(CheckpointError, match="task ID"):
        CheckpointStore(tmp_path).save(0, "responder", request=request, result=json.dumps(wrong))


def test_capture_selects_only_incident_worker_configmaps_and_broker_ledger(tmp_path: Path) -> None:
    from google.protobuf.any_pb2 import Any as ProtoAny

    request, result = _task_and_result()
    another = json.loads(request)
    another["task_id"] = "another"
    another["request"]["incident_id"] = "another-incident"
    other_request = json.dumps(another)
    reflection = ReflectionRequest()
    reflection.outcome.incident_id = "incident-1"
    reflection.closure.request.incident_id = "incident-1"
    packed_reflection = ProtoAny()
    packed_reflection.Pack(reflection)
    reflection_task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection",
        request=packed_reflection,
        workspace={"proposal_id": "reflection-1"},
    )
    packed_result = ProtoAny()
    packed_result.Pack(ReflectionResult(learning_decision="no_change", no_change_reason="fixture"))
    reflection_result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        proposal_id="reflection-1",
        status="completed",
        response=packed_result,
    )
    maps = [
        {"metadata": {"name": "sdo-task-request"}, "data": {"agent-task.json": request}},
        {"metadata": {"name": "sdo-task-result"}, "data": {"agent-result.json": result}},
        {
            "metadata": {"name": "sdo-reflection-request"},
            "data": {"agent-task.json": to_canonical_json(reflection_task)},
        },
        {
            "metadata": {"name": "sdo-reflection-result"},
            "data": {"agent-result.json": to_canonical_json(reflection_result)},
        },
        {"metadata": {"name": "sdo-other-request"}, "data": {"agent-task.json": other_request}},
    ]
    receipt_dir = tmp_path / "incident"
    receipt_dir.mkdir()
    (receipt_dir / "sdo_production_receipt_strict.json").write_text(
        json.dumps({"native_ledger": {"incident_id": "incident-1", "receipt": {"outcome_commit": "abc"}}}),
        encoding="utf-8",
    )

    capture_checkpoints(CheckpointStore(tmp_path), 0, "incident-1", receipt_dir, maps)

    assert CheckpointStore(tmp_path).load(0, "responder").result == result
    assert CheckpointStore(tmp_path).load(0, "broker").result == to_canonical_json(reflection_result)
    assert CheckpointStore(tmp_path).load(0, "broker").broker == {
        "incident_id": "incident-1",
        "receipt": {"outcome_commit": "abc"},
    }
    assert not (tmp_path / "checkpoints" / "000" / "drift").exists()


def test_capture_does_not_offer_broker_replay_without_reflection_result(tmp_path: Path) -> None:
    request, result = _task_and_result()
    receipt_dir = tmp_path / "incident"
    receipt_dir.mkdir()
    (receipt_dir / "sdo_production_receipt_strict.json").write_text(
        json.dumps({"native_ledger": {"incident_id": "incident-1"}}), encoding="utf-8"
    )

    capture_checkpoints(
        CheckpointStore(tmp_path),
        0,
        "incident-1",
        receipt_dir,
        [{"data": {"agent-task.json": request}}, {"data": {"agent-result.json": result}}],
    )

    assert not (tmp_path / "checkpoints" / "000" / "broker").exists()


def test_capture_uses_latest_dispatch_attempt_for_a_stage(tmp_path: Path) -> None:
    request, result = _task_and_result()
    later_task = json.loads(request)
    later_task["task_id"] = "incident-1-retry"
    later_task["workspace"]["proposal_id"] = "incident-1-retry"
    later_result = json.loads(result)
    later_result["task_id"] = "incident-1-retry"
    later_result["proposal_id"] = "incident-1-retry"
    maps = [
        {
            "metadata": {"creationTimestamp": "2026-10-21T02:00:00Z"},
            "data": {"agent-task.json": json.dumps(later_task)},
        },
        {"metadata": {"creationTimestamp": "2026-10-21T01:00:00Z"}, "data": {"agent-task.json": request}},
        {"data": {"agent-result.json": result}},
        {"data": {"agent-result.json": json.dumps(later_result)}},
    ]
    receipt_dir = tmp_path / "incident"
    receipt_dir.mkdir()

    capture_checkpoints(CheckpointStore(tmp_path), 0, "incident-1", receipt_dir, maps)

    assert parse_json(CheckpointStore(tmp_path).load(0, "responder").request, AgentTask()).task_id == "incident-1-retry"


def test_capture_includes_redacted_session_snapshots_for_fake_api(tmp_path: Path) -> None:
    import hashlib

    from benchmarks.sregym.fastloop.replay import capture_checkpoints

    request, result = _task_and_result()
    receipt_dir = tmp_path / "incident"
    session = hashlib.sha256(b"incident-1").hexdigest()[:32]
    corpus = receipt_dir / "sdo_runtime" / "sessions" / session / "corpus"
    corpus.mkdir(parents=True)
    (corpus / "open@pre-apply.json").write_text('{"services":[]}', encoding="utf-8")
    (corpus / "fixed.json").write_text('{"services":[]}', encoding="utf-8")
    (corpus / "unrelated.json").write_text('{"services":[]}', encoding="utf-8")
    maps = [
        {"data": {"agent-task.json": request}},
        {"data": {"agent-result.json": result}},
    ]

    capture_checkpoints(CheckpointStore(tmp_path), 0, "incident-1", receipt_dir, maps)

    assert set(CheckpointStore(tmp_path).load(0, "responder").captures) == {"open@pre-apply.json", "fixed.json"}


def test_replay_cli_selects_one_stage_without_loading_environment(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from google.protobuf.any_pb2 import Any as ProtoAny

    from benchmarks.sregym.fastloop.cli import main

    request = ReflectionRequest(outcome_commit="a" * 40)
    request.outcome.incident_id = "incident-1"
    request.memory_snapshot.accepted_commit = "a" * 40
    packed_request = ProtoAny()
    packed_request.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection",
        request=packed_request,
        workspace={"proposal_id": "reflection-1", "base_commit": "a" * 40},
    )
    packed_result = ProtoAny()
    packed_result.Pack(ReflectionResult(learning_decision="no_change", no_change_reason="fixture"))
    result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        proposal_id="reflection-1",
        status="completed",
        response=packed_result,
    )
    CheckpointStore(tmp_path).save(0, "broker", request=to_canonical_json(task), result=to_canonical_json(result))

    assert main(["replay", "--run-dir", str(tmp_path), "--incident", "0", "--stage", "broker"]) == 0
    assert "no_change" in capsys.readouterr().out


def test_scripted_responder_replay_uses_captured_kubernetes_objects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from google.protobuf import json_format
    from google.protobuf.any_pb2 import Any as ProtoAny

    from benchmarks.sregym.adapter import scripted_worker
    from benchmarks.sregym.fastloop.cli import main
    from benchmarks.sregym.fastloop.replay_worker import CapturedKubernetesAPI
    from sdo.agents.responder.worker import run_worker_task

    source = tmp_path / "source"
    (source / "kubernetes").mkdir(parents=True)
    (source / "kubernetes" / "front.yaml").write_text(
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: front\nspec:\n"
        "  ports:\n  - port: 80\n    targetPort: 8080\n",
        encoding="utf-8",
    )
    for args in (
        ["git", "init", "-q", str(source)],
        ["git", "-C", str(source), "add", "."],
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Replay",
            "-c",
            "user.email=replay@example.invalid",
            "commit",
            "-qm",
            "seed",
        ],
    ):
        subprocess.run(args, check=True)
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    raw = json.loads((Path(__file__).resolve().parents[4] / "fixtures/sdo/contracts/incident_request.json").read_text())
    raw["namespace"] = "hotel"
    raw["incident_id"] = "incident-1"
    raw["repository_worktree"] = str(source)
    raw["repository_base_commit"] = commit
    raw["source_commit"] = commit
    raw["deployed_commit"] = commit
    raw["repair_policy"] = "recorded-actions"
    raw["findings"][0]["primary_resource"] = {
        "kind": "Service",
        "namespace": "hotel",
        "name": "front",
        "api_version": "v1",
    }
    raw["findings"][0]["playbook_ids"] = raw["findings"][0].pop("playbooks")
    raw["surfaced_playbooks"] = []
    raw["memory_snapshot"] = {"accepted_commit": commit}
    raw["state_changes"] = {
        "baseline_at": raw["response_deadline"],
        "observed_at": raw["response_deadline"],
        "changes": [{"kind": "Service", "name": "front", "change": "modified"}],
    }
    typed = json_format.ParseDict(raw, ProtoIncidentRequest())
    packed = ProtoAny()
    packed.Pack(typed)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="incident-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed,
        workspace={"proposal_id": "incident-1", "checkout_path": str(source), "base_commit": commit},
    )
    run_dir = tmp_path / "results" / "run"
    run_dir.mkdir(parents=True)
    (tmp_path / "fastloop.json").write_text(json.dumps({"workspace": str(source)}), encoding="utf-8")
    captures = {
        "open@pre-apply.json": json.dumps(
            {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"port": 80, "targetPort": 9999}]}}]}
        ).encode(),
        "fixed.json": json.dumps(
            {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"port": 80, "targetPort": 8080}]}}]}
        ).encode(),
    }
    api = CapturedKubernetesAPI(captures)
    monkeypatch.setattr(scripted_worker, "_command", api.command)
    monkeypatch.setattr(scripted_worker, "_tool", api.tool)
    task_path = tmp_path / "task.json"
    task_path.write_text(to_canonical_json(task), encoding="utf-8")

    class Publisher:
        payload = ""

        def publish_payload(self, _name: str, _key: str, payload: str) -> None:
            self.payload = payload

    publisher = Publisher()
    run_worker_task(task_path, "result", publisher, executor=scripted_worker.scripted_responder)
    CheckpointStore(run_dir).save(
        0, "responder", request=to_canonical_json(task), result=publisher.payload, captures=captures
    )

    assert main(["replay", "--run-dir", str(run_dir), "--incident", "0", "--stage", "responder"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["identical"] is True, "\n".join(output["difference"])


def test_scripted_reflection_replay_runs_worker_again(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from google.protobuf.any_pb2 import Any as ProtoAny

    from benchmarks.sregym.fastloop.cli import main
    from sdo.agents.responder.reflection_worker import run_reflection_task
    from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome
    from tests.unit.sdo.agents.responder.test_reflection_worker import _closure

    source = tmp_path / "source"
    program = source / ".sdo/playbooks/service-targetport-mismatch"
    program.mkdir(parents=True)
    (program / "diagnose.go").write_text("package targetport\n", encoding="utf-8")
    for args in (
        ["git", "init", "-q", str(source)],
        ["git", "-C", str(source), "add", "."],
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Replay",
            "-c",
            "user.email=replay@example.invalid",
            "commit",
            "-qm",
            "seed",
        ],
    ):
        subprocess.run(args, check=True)
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    closure = _closure()
    incident_id = closure.request.incident_id
    request = ReflectionRequest(
        closure=closure, outcome=_outcome(incident_id), outcome_commit=commit, session_mode="fresh"
    )
    request.memory_snapshot.accepted_commit = commit
    packed = ProtoAny()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection",
        request=packed,
        workspace={"proposal_id": "reflection-1", "checkout_path": str(source), "base_commit": commit},
    )
    task_path = tmp_path / "reflection-task.json"
    task_path.write_text(to_canonical_json(task), encoding="utf-8")

    class Publisher:
        payload = ""

        def publish_payload(self, _name: str, _key: str, payload: str) -> None:
            self.payload = payload

    from benchmarks.sregym.adapter.scripted_worker import ScriptedReflector

    publisher = Publisher()
    run_reflection_task(task_path, "result", publisher, reflector=ScriptedReflector())
    run_dir = tmp_path / "results" / "run"
    run_dir.mkdir(parents=True)
    (tmp_path / "fastloop.json").write_text(json.dumps({"workspace": str(source)}), encoding="utf-8")
    CheckpointStore(run_dir).save(0, "reflection", request=to_canonical_json(task), result=publisher.payload)

    assert main(["replay", "--run-dir", str(run_dir), "--incident", "0", "--stage", "reflection"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["identical"] is True, "\n".join(output["difference"])
