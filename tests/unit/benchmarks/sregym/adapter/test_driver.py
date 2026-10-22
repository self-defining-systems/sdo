"""Benchmark driver contracts around the native Go lifecycle and transport."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import driver

if TYPE_CHECKING:
    from pathlib import Path


def test_application_name_is_a_kubernetes_dns_label(tmp_path: Path) -> None:
    repository = tmp_path / "Fallback_Repo"
    assert driver._application_label({"app_name": "Hotel Reservation"}, repository) == "hotel-reservation"
    assert driver._application_label({"app_name": "social_network"}, repository) == "social-network"
    assert driver._application_label({}, repository) == "fallback-repo"
    with pytest.raises(ValueError, match="DNS label"):
        driver._application_label({"app_name": "!!!"}, tmp_path / "???")


def test_local_conductor_uses_in_cluster_relay() -> None:
    assert driver._in_cluster_api_base("http://localhost:8123/") == "http://sdo-sregym-bridge:8000"
    assert driver._relay_target_api_base("http://localhost:8123/") == "http://localhost:8123"
    assert driver._relay_target_api_base("https://conductor.example/") is None


def test_health_objective_excludes_external_name_service_endpoints() -> None:
    payload = {
        "items": [
            {"kind": "Deployment", "metadata": {"name": "web"}, "spec": {}},
            {"kind": "Service", "metadata": {"name": "web"}, "spec": {"type": "ClusterIP"}},
            {"kind": "Service", "metadata": {"name": "external"}, "spec": {"type": "ExternalName"}},
        ]
    }

    def run(_args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, json.dumps(payload), "")

    context = driver._deployed_lifecycle_context("demo", command_runner=run)
    assert "web expose ready endpoints" in context.health_objective
    assert "ExternalName Services named external" in context.health_objective
    assert "must not be required to have ready endpoints" in context.health_objective


def test_native_result_submission_reads_go_ledger_only() -> None:
    receipt = {
        "schema_version": "sdo.production-receipt/v2",
        "native_ledger": {
            "closure": {
                "result": {
                    "confirmed_root_causes": [{"summary": "selector changed"}],
                    "repair_actions": [{"summary": "restored selector"}],
                }
            }
        },
    }
    submitted: list[tuple[str, str]] = []

    def submit(text: str, *, phase: str, api_base: str) -> dict[str, object]:
        assert api_base == "http://localhost:8000"
        submitted.append((phase, text))
        return {}

    driver._submit_recorded_result(
        receipt, "http://localhost:8000", current_stage=lambda _base: "diagnosis", submitter=submit
    )
    assert submitted == [("diagnosis", "selector changed"), ("mitigation", "restored selector")]


def test_native_result_submission_rejects_missing_responder_result() -> None:
    with pytest.raises(RuntimeError, match="responder result"):
        driver._submit_recorded_result(
            {"schema_version": "sdo.production-receipt/v2", "native_ledger": {}},
            "http://localhost:8000",
            current_stage=lambda _base: "diagnosis",
        )


def test_job_cleanup_uses_native_control_namespace_and_correlated_job() -> None:
    commands: list[list[str]] = []

    def run(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    driver._remove_sdo_jobs_before_benchmark_grading(
        {
            "schema_version": "sdo.production-receipt/v2",
            "control_namespace": "demo-sdo",
            "responder_job_evidence": [{"job_name": "sdo-responder-a"}],
        },
        "demo",
        command_runner=run,
    )
    assert commands[0][commands[0].index("--namespace") + 1] == "demo-sdo"
    assert "job/sdo-responder-a" in commands[0]


def test_agent_settings_keep_native_reflection_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SREGYM_EXPERIMENT_AGENT_CONFIG",
        json.dumps(
            {
                "reflection_session": "resume",
                "reflection_guidance": "generalize-spec",
                "late_findings": "pull",
            }
        ),
    )
    args = driver._parse_args([])
    assert args.reflection_session == "resume"
    assert args.reflection_guidance == "generalize-spec"
    assert args.late_findings == "pull"


def test_reflection_defaults_to_resume_and_fresh_remains_selectable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SREGYM_EXPERIMENT_AGENT_CONFIG", raising=False)
    assert driver._parse_args([]).reflection_session == "resume"
    assert driver._parse_args(["--reflection-session", "fresh"]).reflection_session == "fresh"


def test_receipt_is_persisted_atomically(tmp_path: Path) -> None:
    path = driver.persist_production_receipt({"schema_version": "sdo.production-receipt/v2"}, tmp_path)
    assert json.loads(path.read_text())["schema_version"] == "sdo.production-receipt/v2"
    assert not path.with_suffix(path.suffix + ".tmp").exists()
