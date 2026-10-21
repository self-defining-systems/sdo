"""Benchmark transport and evidence adapter over the native Go operator."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter.cluster_resources import kubectl
from benchmarks.sregym.adapter.native_operator import (
    NativeInstallConfig,
    collect_native_receipt,
    install_existing,
)
from benchmarks.sregym.protocol import ProductionReceiptValidationError
from benchmarks.sregym.protocol import validate_production_receipt as validate_receipt_contract

logger = logging.getLogger(__name__)
RUNTIME_ARTIFACTS_DIRNAME = "sdo_runtime"
FIRING_STREAM_FILENAME = "detector_firings.jsonl"
_CONTROLLER_JOB_SELECTOR = "job-name=sdo-controller-run"


class ControllerInstallError(RuntimeError):
    """The benchmark could not install or inspect a native Go controller."""


@dataclass(frozen=True)
class RuntimeConfig:
    """Benchmark settings passed to the native operator and persistent runner."""

    repository: Path
    namespace: str
    application: str
    controller_image: str
    responder_image: str
    repository_pvc: str
    credentials_secret: str
    model: str
    timeout_seconds: int
    validator_image: str = "sdo-detector-validator:v0.1.0"
    verification_timeout_seconds: int = 120
    wait_for_completion: bool = False
    repair_policy: str = "commit"
    agent_provider: str = "codex"
    reflection_session: str = "resume"
    reflection_guidance: str = "baseline"
    force_reflection: bool = False
    late_findings: str = "off"
    max_follow_ups: int = 0
    max_response_loops: int = 5
    follow_up_cooldown_seconds: int = 30
    healthy_baseline: bool = False
    controller_namespace: str | None = None
    reuse_existing: bool = False
    submission_api_base: str | None = None
    submission_relay_target_base: str | None = None
    allow_test_lifecycle: bool = False
    artifacts_dir: Path | None = None
    persistent: bool = False
    buildkit_image: str = "moby/buildkit@sha256:ef31818fffd0b9133d710742e06b492553f7ebc54f5fd66cd21a1a9bf2e51f76"
    registry_push_secret: str = "sdo-registry-push"
    kind_cluster_name: str | None = None
    max_deployment_attempts: int = 3
    agent_registrations: Path | None = None
    deployed_manifest_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field in (
            "namespace",
            "application",
            "controller_image",
            "responder_image",
            "validator_image",
            "repository_pvc",
            "credentials_secret",
            "model",
        ):
            if not getattr(self, field).strip():
                raise ValueError(f"{field} must be non-empty")
        if self.timeout_seconds <= 0 or self.verification_timeout_seconds <= 0:
            raise ValueError("controller timeouts must be positive")
        if self.controller_namespace is not None and not self.controller_namespace.strip():
            raise ValueError("controller namespace must be non-empty")
        if self.repair_policy not in ("commit", "recorded-actions"):
            raise ValueError("repair_policy must be commit or recorded-actions")
        if self.agent_provider not in ("codex", "claude"):
            raise ValueError("agent_provider must be codex or claude")
        if self.reflection_session not in ("fresh", "resume"):
            raise ValueError("reflection_session must be fresh or resume")
        if self.reflection_guidance not in ("baseline", "generalize", "generalize-spec"):
            raise ValueError("invalid reflection_guidance")
        if self.late_findings not in ("off", "pull"):
            raise ValueError("late_findings must be off or pull")
        if self.max_follow_ups < 0 or self.follow_up_cooldown_seconds <= 0 or self.max_response_loops <= 0:
            raise ValueError("invalid follow-up limit or cooldown")
        if self.max_deployment_attempts <= 0:
            raise ValueError("max_deployment_attempts must be positive")
        if self.agent_registrations is not None and not self.agent_registrations.is_absolute():
            raise ValueError("agent_registrations path must be absolute")

    @property
    def control_namespace(self) -> str:
        return self.controller_namespace or self.namespace


def native_install_config(config: RuntimeConfig) -> NativeInstallConfig:
    """Translate benchmark settings at the Go CLI boundary."""

    kubeconfig = os.getenv("SREGYM_BASE_KUBECONFIG", "").strip() or os.getenv("KUBECONFIG", "").strip()
    if not kubeconfig or not Path(kubeconfig).is_absolute():
        raise ControllerInstallError("native operator requires an absolute trusted kubeconfig")
    responder_env = (
        (
            f"SDO_SREGYM_API_BASE={config.submission_api_base}",
            f"SDO_RESPONDER_EXTRA_INSTRUCTIONS={_responder_instructions()}",
        )
        if config.submission_api_base
        else ()
    )
    return NativeInstallConfig(
        repository=config.repository.resolve(),
        namespace=config.namespace,
        control_namespace=config.control_namespace,
        application=config.application,
        controller_image=config.controller_image,
        responder_image=config.responder_image,
        validator_image=config.validator_image,
        repository_pvc=config.repository_pvc,
        credentials_secret=config.credentials_secret,
        model=config.model,
        kubeconfig=Path(kubeconfig),
        agent_provider=config.agent_provider,
        repair_policy=config.repair_policy,
        late_findings=config.late_findings,
        reflection_session=config.reflection_session,
        reflection_guidance=config.reflection_guidance,
        force_reflection=config.force_reflection,
        healthy_baseline=config.healthy_baseline,
        max_follow_ups=config.max_follow_ups,
        max_response_loops=config.max_response_loops,
        follow_up_cooldown_seconds=config.follow_up_cooldown_seconds,
        response_timeout_seconds=config.timeout_seconds,
        verification_timeout_seconds=config.verification_timeout_seconds,
        responder_env=responder_env,
        deployed_manifest_paths=config.deployed_manifest_paths,
    )


def validate_production_receipt(receipt: dict[str, Any], *, allow_test_lifecycle: bool = False) -> None:
    try:
        validate_receipt_contract(receipt, allow_test_lifecycle=allow_test_lifecycle)
    except ProductionReceiptValidationError as exc:
        raise ControllerInstallError(str(exc)) from exc


def install_persistent_controller(config: RuntimeConfig) -> bool:
    """Install a previously accepted checkout through Go; return whether reused."""

    if not config.persistent:
        raise ControllerInstallError("persistent install requires RuntimeConfig.persistent")
    return install_existing(native_install_config(config)).reused


def collect_production_receipt(config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
    """Sync accepted source and read one incident's validated Go ledger."""

    del artifacts_dir
    native = native_install_config(config)
    return collect_native_receipt(native, incident_id, sync_repository=True)


def _responder_instructions() -> str:
    return (
        "SREGym submission transport is available. After evidence-based diagnosis, run "
        "`python3 -m benchmarks.sregym.adapter.submission diagnosis '<diagnosis>'`; after repair and your own "
        "verification, run `python3 -m benchmarks.sregym.adapter.submission mitigation '<mitigation>'`. "
        "The controller alone determines closure from live detectors. Do not begin repair until diagnosis is "
        "acknowledged. Before mitigation, wait for affected rollouts, verify representative requests, and run "
        "`python3 -m sdo.agents.responder.status_cli` until it reports healthy. "
        "Submit mitigation once; return the incident "
        "result after the benchmark accepts it."
    )


def _materialize_firing_stream(exported: Path, artifacts_dir: Path) -> None:
    parts = [exported / "detector-firings.jsonl.1", exported / "detector-firings.jsonl"]
    chunks: list[str] = []
    for part in parts:
        if part.is_file():
            text = part.read_text(encoding="utf-8")
            chunks.append(text if text.endswith("\n") or not text else text + "\n")
    if chunks:
        (artifacts_dir / FIRING_STREAM_FILENAME).write_text("".join(chunks), encoding="utf-8")


def export_controller_logs(control_namespace: str, artifacts_dir: Path | None) -> dict[str, str | None]:
    """Keep bounded controller logs as benchmark diagnostics, never as verdict evidence."""

    if artifacts_dir is None:
        return {"directory": None, "error": "no artifacts directory"}
    destination = artifacts_dir / RUNTIME_ARTIFACTS_DIRNAME / "controller_logs"
    errors: list[str] = []
    try:
        destination.mkdir(parents=True, exist_ok=True)
        pods = kubectl(
            ["get", "pods", "--selector", _CONTROLLER_JOB_SELECTOR, "-o", "jsonpath={.items[*].metadata.name}"],
            namespace=control_namespace,
            check=False,
        )
        names = pods.stdout.split() if pods.returncode == 0 else []
        if pods.returncode != 0:
            errors.append(f"list controller pods: {pods.stderr.strip() or f'exit {pods.returncode}'}")
        elif not names:
            errors.append("no controller pods match sdo-controller-run")
        for name in names:
            logs = kubectl(
                ["logs", f"pod/{name}", "--all-containers=true", "--timestamps=true"],
                namespace=control_namespace,
                check=False,
            )
            if logs.returncode != 0:
                errors.append(f"logs pod/{name}: {logs.stderr.strip() or f'exit {logs.returncode}'}")
                continue
            (destination / f"{name}.log").write_text(logs.stdout, encoding="utf-8")
    except (OSError, RuntimeError, json.JSONDecodeError) as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
    error = "; ".join(errors) if errors else None
    if error:
        logger.warning("could not export SDO controller logs: %s", error)
        (destination / "export_error.txt").write_text(error + "\n", encoding="utf-8")
    return {"directory": str(destination), "error": error}
