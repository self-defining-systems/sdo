"""Kubernetes death evidence releases the real persisted session owner."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from libs.agent_cli.session_ownership import SessionOwnership, WorkerIdentity
from libs.agent_cli.structured import run_structured_turn
from sdo.agents.responder.writer_termination import KubernetesWriterObserver
from tests.structured_turns import ScriptedAgent, reply


def test_oom_killed_owner_allows_reflection_after_positive_termination_observation(tmp_path: Path) -> None:
    worker = WorkerIdentity(namespace="control", pod_name="responder", pod_uid="uid-1", container="agent")
    producer = SessionOwnership(tmp_path, "codex", timeout_seconds=1, worker=worker)
    producer.claim("session-1", resume=False)
    producer.close(writer_closed=False)  # OOM killed: no worker release callback.
    observed = []

    def read_pod(identity, timeout):
        observed.append(identity)
        if len(observed) == 1:
            return {
                "metadata": {"namespace": "control", "name": "responder", "uid": "uid-1"},
                "spec": {"restartPolicy": "Never"},
                "status": {"phase": "Running", "containerStatuses": [{"name": "agent", "state": {"running": {}}}]},
            }
        return {
            "metadata": {"namespace": "control", "name": "responder", "uid": "uid-1"},
            "spec": {"restartPolicy": "Never"},
            "status": {
                "containerStatuses": [
                    {
                        "name": "agent",
                        "state": {
                            "terminated": {"reason": "OOMKilled", "exitCode": 137, "finishedAt": "2026-10-28T03:00:00Z"}
                        },
                    }
                ]
            },
        }

    observer = KubernetesWriterObserver(read_pod=read_pod)

    def reflect(_request):
        assert len(observed) == 2, "resumed before observing termination"
        return reply("codex", {"ok": True}, session_id="session-1")

    consumer = ScriptedAgent(reflect)
    result = run_structured_turn(
        "codex",
        "reflect",
        output_schema={
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": False,
        },
        cwd=tmp_path,
        access="danger-full-access",
        executor=consumer.executor,
        resume_session_id="session-1",
        timeout_seconds=2,
        session_ownership_directory=tmp_path,
        session_ownership_role="reflection",
        session_termination_observer=observer,
    )
    assert result.session_id == "session-1"
    assert observed == [worker, worker]
    (record,) = tmp_path.glob("*.json")
    confirmation = json.loads(record.read_text())["termination_confirmation"]
    assert confirmation["worker"]["pod_uid"] == "uid-1"
    assert confirmation["reason"] == "OOMKilled"
    assert confirmation["kind"] == "container_terminated"
    assert confirmation["exit_code"] == 137
    # A restarted consumer reads the same persisted evidence; it need not
    # rediscover or guess the vanished writer's identity.
    resumed = SessionOwnership(tmp_path, "codex", timeout_seconds=1, role="reflection")
    resumed.claim("session-1", resume=True)
    resumed.close(writer_closed=True)
    assert json.loads(record.read_text())["termination_confirmation"] == confirmation


@pytest.mark.parametrize("evidence", ["gone", "pod_failed", "pod_succeeded", "container_oom"])
def test_positive_worker_death_observations_are_uid_bound(evidence: str) -> None:
    worker = WorkerIdentity("control", "worker", "uid-1", "agent")
    pod = {
        "metadata": {"namespace": "control", "name": "worker", "uid": "uid-1"},
        "spec": {"restartPolicy": "Never"},
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "agent",
                    "state": {"terminated": {"reason": "OOMKilled", "finishedAt": "2026-10-28T03:00:00Z"}},
                }
            ],
        },
    }
    if evidence.startswith("pod_"):
        pod["status"]["phase"] = "Failed" if evidence == "pod_failed" else "Succeeded"
    observer = KubernetesWriterObserver(read_pod=lambda *_: None if evidence == "gone" else pod)
    confirmation = observer(worker, 1)
    assert confirmation is not None
    assert confirmation.worker == worker
    assert confirmation.kind == (
        "pod_gone"
        if evidence == "gone"
        else "container_terminated"
        if evidence == "container_oom"
        else "pod_terminated"
    )


@pytest.mark.parametrize(
    "case",
    [
        "running",
        "waiting",
        "old_oom_running",
        "heartbeat_expired",
        "wrong_uid",
        "wrong_name",
        "wrong_namespace",
        "restartable_container",
        "deleting_only",
        "api_error",
        "api_timeout",
        "unknown",
        "missing_container",
        "wrong_container",
        "malformed",
        "terminal_phase_running_container",
    ],
)
def test_uncertain_or_other_worker_observations_never_release(case: str, tmp_path: Path) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffTimeout

    worker = WorkerIdentity("control", "worker", "uid-1", "agent")
    producer = SessionOwnership(tmp_path, "codex", timeout_seconds=1, worker=worker)
    producer.claim("session-1", resume=False)
    producer.close(writer_closed=False)
    pod = {
        "metadata": {"namespace": "control", "name": "worker", "uid": "uid-1"},
        "spec": {"restartPolicy": "Never"},
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "agent",
                    "state": {"terminated": {"reason": "OOMKilled", "finishedAt": "2026-10-28T03:00:00Z"}},
                }
            ],
        },
    }
    pod = copy.deepcopy(pod)
    container = pod["status"]["containerStatuses"][0]
    if case in {"running", "old_oom_running", "heartbeat_expired", "deleting_only", "terminal_phase_running_container"}:
        container["lastState"] = container["state"]
        container["state"] = {"running": {}}
    elif case == "waiting":
        container["state"] = {"waiting": {"reason": "CrashLoopBackOff"}}
    elif case == "wrong_uid":
        pod["metadata"]["uid"] = "replacement-uid"
    elif case == "wrong_name":
        pod["metadata"]["name"] = "other"
    elif case == "wrong_namespace":
        pod["metadata"]["namespace"] = "other"
    elif case == "restartable_container":
        pod["spec"]["restartPolicy"] = "Always"
    elif case == "unknown":
        container["state"] = {}
        pod["status"]["phase"] = "Unknown"
    elif case == "missing_container":
        pod["status"]["containerStatuses"] = []
    elif case == "wrong_container":
        container["name"] = "sidecar"
    elif case == "malformed":
        pod["status"] = None
    if case == "deleting_only":
        pod["metadata"]["deletionTimestamp"] = "2026-10-28T03:00:00Z"
    if case == "terminal_phase_running_container":
        pod["status"]["phase"] = "Failed"

    def read(*_):
        if case in {"api_error", "api_timeout"}:
            raise OSError("API unavailable") if case == "api_error" else TimeoutError("request timed out")
        return pod

    observer = KubernetesWriterObserver(read_pod=read)
    with pytest.raises(SessionHandoffTimeout):
        SessionOwnership(
            tmp_path, "codex", timeout_seconds=0.03, role="reflection", termination_observer=observer
        ).claim("session-1", resume=True)
    (record,) = tmp_path.glob("*.json")
    assert json.loads(record.read_text())["state"] == "active"


def test_worker_identity_comes_from_registered_pod_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.responder.session_turn import session_turn_options

    monkeypatch.setenv("SDO_WORKER_NAMESPACE", "control")
    monkeypatch.setenv("SDO_WORKER_POD_NAME", "responder")
    monkeypatch.setenv("SDO_WORKER_POD_UID", "uid-1")
    monkeypatch.setenv("SDO_WORKER_CONTAINER", "agent")
    options = session_turn_options("codex", Path("/workspace"))
    assert options["session_ownership_worker"] == WorkerIdentity("control", "responder", "uid-1", "agent")
    assert isinstance(options["session_termination_observer"], KubernetesWriterObserver)


@pytest.mark.parametrize(
    ("status", "body", "confirms_gone"),
    [
        (404, b"", True),
        (403, b"", False),
        (500, b"", False),
        (302, b"", False),
        (200, b"null", False),
        (200, b"not-json", False),
        (200, b"x" * ((1 << 20) + 1), False),
    ],
)
def test_only_successful_exact_pod_get_notfound_confirms_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int, body: bytes, confirms_gone: bool
) -> None:
    import io
    import urllib.error

    from sdo.agents.responder import writer_termination

    # Only the external HTTP boundary is replaced. Exercise the real URL,
    # bounded response parser, HTTP classification and termination observer.
    (tmp_path / "token").write_text("synthetic-service-account-token")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    monkeypatch.setattr(writer_termination, "SERVICE_ACCOUNT_ROOT", tmp_path)
    monkeypatch.setattr(writer_termination.ssl, "create_default_context", lambda **_: None)
    requests = []

    class Opener:
        def open(self, request, *, timeout):
            requests.append(request.full_url)
            assert 0 < timeout <= 2
            if status != 200:
                raise urllib.error.HTTPError(request.full_url, status, "synthetic", {}, io.BytesIO())
            return io.BytesIO(body)

    monkeypatch.setattr(writer_termination.urllib.request, "build_opener", lambda *_: Opener())
    worker = WorkerIdentity("control", "worker", "uid-1", "agent")
    confirmation = KubernetesWriterObserver()(worker, 1)
    assert requests == ["https://kubernetes.default.svc:443/api/v1/namespaces/control/pods/worker"]
    assert (confirmation is not None) is confirms_gone
    if confirmation is not None:
        assert confirmation.worker.pod_uid == "uid-1"
        assert confirmation.kind == "pod_gone"


def test_unknown_owner_uid_cannot_be_released_by_pod_absence(tmp_path: Path) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffTimeout

    producer = SessionOwnership(tmp_path, "codex", timeout_seconds=1)
    producer.claim("session-1", resume=False)
    producer.close(writer_closed=False)

    def observe(*_):
        raise AssertionError("must not guess which Pod UID owned a legacy record")

    with pytest.raises(SessionHandoffTimeout):
        SessionOwnership(tmp_path, "codex", timeout_seconds=0.03, termination_observer=observe).claim(
            "session-1", resume=True
        )
