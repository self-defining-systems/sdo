"""The benchmark's active installer calls only the compiled Go operator."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

# The receipt checker binds the real subprocess.run when first imported; import it before any test fakes it.
import benchmarks.sregym.protocol.native_receipt  # noqa: F401
from benchmarks.fuzzops.config import BENCH_PROFILE
from benchmarks.sregym.adapter.deployment_source import raw_source
from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    NativeOperatorError,
    collect_native_receipt,
    export_runtime_artifacts,
    inspect_existing,
    install_existing,
    launch_host,
    read_incident_evidence,
    set_maintenance,
    sync_existing,
)
from tests.unit.benchmarks.sregym.adapter.launch_capture import overlay, stub_resolution


def _install(tmp_path: Path) -> NativeInstallConfig:
    return NativeInstallConfig(
        repository=tmp_path / "app",
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="agent-credentials",
        model="gpt-6-luna",
        kubeconfig=tmp_path / "lane.kubeconfig",
        repair_policy="recorded-actions",
        responder_env=("SDO_SREGYM_API_BASE=http://sdo-sregym-bridge:8000",),
    )


@pytest.mark.parametrize("code_path", ["go", "shell"])
def test_install_existing_invokes_go_cli_with_config_files_and_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code_path: str
) -> None:
    calls: list[tuple[list[str], dict[str, str], dict[str, object]]] = []
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    stub_resolution(monkeypatch)

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        env = kwargs.get("env")
        if command[0] == "/tmp/sdo-operator":
            assert isinstance(env, dict)
            calls.append((command, env, overlay(command)))
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"installed": True, "reused": True, "fingerprint": "abc", "accepted_commit": "a" * 40}),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    receipt = install_existing(
        replace(
            _install(tmp_path),
            code_path=code_path,
            healthy_baseline=True,
            permissive_responder=True,
            responder_namespaces=("policy-system",),
            max_response_loops=3,
            deployment_source=raw_source(("kubernetes",)),
            agent_effort="high",
        )
    )

    command, env, settings = calls[0]
    assert receipt.reused
    assert receipt.accepted_commit == "a" * 40
    assert command[0:2] == ["/tmp/sdo-operator", "install-existing"]
    assert settings == {
        "agent": {"code_path": code_path, "extra_env": ["SDO_SREGYM_API_BASE=http://sdo-sregym-bridge:8000"]},
        "response": {
            "max_loops": 3,
            "repair_policy": "recorded-actions",
            "permissive": True,
            "permissive_namespaces": ["policy-system"],
        },
        "learning": {"healthy_baseline": True},
    }
    assert (env["SDO_AGENT_MODEL"], env["SDO_AGENT_EFFORT"]) == ("gpt-6-luna", "high")
    assert "SDO_MODE" not in env
    assert json.loads(command[command.index("--deployment-source") + 1]) == raw_source(("kubernetes",))
    knob_flags = ("--model", "--agent-", "--repair-policy", "--max-response-loops", "--code-path")
    assert not [arg for arg in command if arg.startswith(knob_flags)]
    assert command[0] != "python3"
    assert "-m" not in command
    # The overlay lives for the launch only.
    assert not Path(command[command.index("--config", command.index("--config") + 1) + 1]).exists()


def test_frozen_and_external_memory_select_the_mode(tmp_path: Path) -> None:
    install = _install(tmp_path)
    assert "SDO_MODE" not in install.environment()
    assert replace(install, frozen_memory=True).environment()["SDO_MODE"] == "frozen"
    assert replace(install, frozen_memory=True, external_incidents=True).environment()["SDO_MODE"] == "external"


def test_a_lane_that_sets_no_knob_passes_only_the_benchmark_profile(tmp_path: Path) -> None:
    from benchmarks.sregym.adapter.native_operator import launch_config

    bare = replace(_install(tmp_path), repair_policy=None, responder_env=())
    assert bare.overlay() == {}
    with launch_config(bare) as (args, _env):
        assert args == ["--config", str(BENCH_PROFILE)]


def test_lane_settings_merge_over_the_typed_fields(tmp_path: Path) -> None:
    install = replace(
        _install(tmp_path),
        max_response_loops=3,
        settings={"response": {"max_loops": 5, "standby": False}, "mode": "watch-only"},
    )
    assert install.overlay()["response"] == {"max_loops": 5, "repair_policy": "recorded-actions", "standby": False}
    assert install.overlay()["mode"] == "watch-only"


def test_host_requires_typed_registry_and_strict_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    stub_resolution(monkeypatch)
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, overlay(command)))
        return subprocess.CompletedProcess(command, 0, "warning\n{}", "")

    monkeypatch.setattr(subprocess, "run", run)
    host = NativeHostConfig(
        install=replace(_install(tmp_path), healthy_baseline=True),
        health_objective="hotel serves requests",
        operation_id="benchmark-stage-1",
        buildkit_image="moby/buildkit@sha256:" + "b" * 64,
        registry_push_secret="registry-push",
        registry_host="registry.hotel-sdo.svc.cluster.local:5000",
        registry_insecure=True,
        max_attempts=2,
    )
    with pytest.raises(NativeOperatorError, match="JSON object"):
        launch_host(host)
    command, settings = calls[0]
    assert command[0:2] == ["/tmp/sdo-operator", "host"]
    assert "--registry-insecure" in command
    assert "--registry-push-secret" in command
    assert "--registry-host" in command
    assert "--max-attempts" not in command
    assert settings["learning"] == {"healthy_baseline": True}
    assert settings["deployment"] == {"max_attempts": 2}


def test_sync_and_maintenance_use_go_cli_without_python_broker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    config = _install(tmp_path)
    sync_existing(config)
    set_maintenance(config.control_namespace, paused=True, generation="stage-2", kubeconfig=config.kubeconfig)
    assert calls[0] == [
        "/tmp/sdo-operator",
        "sync-existing",
        "--repository",
        str(config.repository),
        "--control-namespace",
        "hotel-sdo",
        "--repository-pvc",
        "repository",
        "--controller-image",
        "controller:go",
        "--kubeconfig",
        str(config.kubeconfig),
    ]
    assert calls[1] == [
        "/tmp/sdo-operator",
        "maintenance",
        "--control-namespace",
        "hotel-sdo",
        "--paused=true",
        "--generation",
        "stage-2",
        "--kubeconfig",
        str(config.kubeconfig),
    ]
    set_maintenance(config.control_namespace, paused=True, generation="barrier", preserve_baseline=True)
    assert "--preserve-baseline" in calls[2]


def test_incident_evidence_requires_correlated_native_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "incident_id": "incident-1",
                    "responder_job_name": "sdo-incident-1-hash",
                    "ledger": {"incident_id": "incident-1", "acknowledged": True},
                }
            ),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert read_incident_evidence(_install(tmp_path), "incident-1")["ledger"]["acknowledged"] is True
    assert calls[0] == [
        "/tmp/sdo-operator",
        "incident-evidence",
        "--control-namespace",
        "hotel-sdo",
        "--repository-pvc",
        "repository",
        "--controller-image",
        "controller:go",
        "--incident-id",
        "incident-1",
        "--kubeconfig",
        str(tmp_path / "lane.kubeconfig"),
    ]
    with pytest.raises(ValueError, match="incident ID"):
        read_incident_evidence(_install(tmp_path), "")


@pytest.mark.parametrize("job_name", ["sdo-incident-1-hash", "sdo-standby-bound-identity"])
def test_collect_native_receipt_survives_retired_responder_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_name: str,
) -> None:
    from tests.unit.benchmarks.sregym.protocol.test_native_receipt import _ledger

    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[0] == "/tmp/sdo-operator":
            payload: object = {
                "incident_id": "incident-1",
                "responder_job_name": job_name,
                "ledger": {
                    **_ledger(),
                    "closure": {
                        **_ledger()["closure"],
                        "dispatched_at": "2026-10-18T03:00:00Z",
                        "responder_completed_at": "2026-10-18T03:01:00Z",
                    },
                },
            }
        elif f"job/{job_name}" in command:
            raise AssertionError("receipt queried a retired responder Job")
        elif "job/sdo-controller-run" in command:
            payload = {"metadata": {"uid": "controller-job-uid"}}
        else:
            payload = {
                "items": [
                    {
                        "metadata": {"uid": "old-controller-uid", "ownerReferences": [{"uid": "old-job-uid"}]},
                        "status": {"phase": "Succeeded"},
                    },
                    {
                        "metadata": {"uid": "controller-uid", "ownerReferences": [{"uid": "controller-job-uid"}]},
                        "status": {"phase": "Running"},
                    },
                ]
            }
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", run)
    receipt = collect_native_receipt(replace(_install(tmp_path), namespace="demo"), "incident-1")
    assert receipt["schema_version"] == "sdo.production-receipt/v2"
    assert receipt["controller_pod_uid"] == "controller-uid"
    assert receipt["responder_job_evidence"] == []
    assert not any(f"job/{job_name}" in call for call in calls)


def test_runtime_artifacts_export_is_go_owned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    output_dir = tmp_path / "results"
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({"directory": str(output_dir / "sdo_runtime")}), "")

    monkeypatch.setattr(subprocess, "run", run)
    assert export_runtime_artifacts(_install(tmp_path), output_dir) == output_dir / "sdo_runtime"
    assert calls[0][0:2] == ["/tmp/sdo-operator", "runtime-artifacts"]
    assert "--output-dir" in calls[0]
    assert str(output_dir) in calls[0]


def test_inspect_existing_reuses_live_go_controller_without_local_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", "/tmp/sdo-operator")
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(
                {
                    "job_uid": "job-uid",
                    "pod_uid": "pod-uid",
                    "fingerprint": "fp",
                    "accepted_commit": "a" * 40,
                }
            ),
            "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = inspect_existing(_install(tmp_path))
    assert result.accepted_commit == "a" * 40
    assert result.pod_uid == "pod-uid"
    assert calls[0][0:2] == ["/tmp/sdo-operator", "inspect-existing"]
    assert "--repository" not in calls[0]


def test_install_records_abort_target_before_transport_call(tmp_path, monkeypatch):
    from benchmarks.sregym.adapter import native_operator
    from benchmarks.sregym.adapter.persistent import PersistentState

    path = tmp_path / "abort-state.json"
    monkeypatch.setenv("SDO_RUN_REGISTRY_STATE", str(path))
    config = _install(tmp_path)

    def interrupted(*args, **kwargs):
        target = PersistentState.load(path).cleanup_targets["hotel"]
        assert target.control_namespace == "hotel-sdo"
        assert target.kubeconfig == str(config.kubeconfig)
        raise NativeOperatorError("interrupted install")

    monkeypatch.setattr(native_operator, "execute_operator", interrupted)
    stub_resolution(monkeypatch)
    with pytest.raises(NativeOperatorError, match="interrupted install"):
        install_existing(config)


def test_host_accepts_optional_goal_and_absent_goal_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.adapter import native_operator

    host = NativeHostConfig(
        install=_install(tmp_path),
        health_objective="",
        operation_id="no-goal",
        buildkit_image="buildkit@sha256:" + "b" * 64,
        registry_push_secret="registry",
        registry_host="registry:5000",
    )
    receipt = {"installed": True, "operation_id": "no-goal"}
    receipt.update(dict.fromkeys(("schema_commit", "source_commit", "architecture_commit", "judge_commit"), "a" * 40))
    stub_resolution(monkeypatch)
    monkeypatch.setattr(native_operator, "_run", lambda *args, **kwargs: receipt)
    assert launch_host(host) == receipt


def test_adapter_agent_efforts_match_the_production_provider() -> None:
    # The adapter reads the shared manifest so SREGym's venv can import it without agentshim.
    from benchmarks.sregym.adapter import native_operator
    from sdo.agents import provider

    assert native_operator.AGENT_EFFORTS == provider.AGENT_EFFORTS
    assert not hasattr(native_operator, "DEFAULT_AGENT_EFFORT")


@pytest.mark.parametrize("failure", ["not_ready", "exec_failed", "timeout", "mismatch"])
def test_shared_identity_distinguishes_transport_readiness_and_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from benchmarks.sregym.adapter import native_operator as native

    config = replace(_install(tmp_path), shared_accepted_id="a" * 32)
    identity = config.repository / ".git" / "sdo-shared-id"
    identity.parent.mkdir(parents=True)
    identity.write_text(config.shared_accepted_id)
    monkeypatch.setattr(native, "refresh_memory_mirror", lambda *_: None)
    monkeypatch.setattr(
        native,
        "_kubectl_json",
        lambda *_: {
            "items": [
                {
                    "metadata": {"name": "controller-pod"},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "False" if failure == "not_ready" else "True"}],
                    },
                }
            ]
        },
    )
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(args, 30)
        return subprocess.CompletedProcess(args, 1 if failure == "exec_failed" else 0, "b" * 32, "SENSITIVE_CANARY")

    monkeypatch.setattr(native.subprocess, "run", run)
    expected = {
        "not_ready": "SharedRepositoryControllerNotReady",
        "exec_failed": "SharedRepositoryExecFailed",
        "timeout": "SharedRepositoryExecFailed",
        "mismatch": "SharedRepositoryIdentityMismatch",
    }
    with pytest.raises(NativeOperatorError) as caught:
        native.assert_shared_repository(config)
    assert type(caught.value).__name__ == expected[failure]
    assert "hotel-sdo" in str(caught.value)
    assert "CANARY" not in str(caught.value)
    assert bool(calls) is (failure != "not_ready")
