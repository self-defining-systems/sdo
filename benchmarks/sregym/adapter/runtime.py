"""Benchmark transport and evidence adapter over the native Go operator."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import Any

from benchmarks.fuzzops.config import BENCH_PROFILE
from benchmarks.sregym.adapter.cluster_resources import kubectl
from benchmarks.sregym.adapter.deployment_source import DeploymentPolicyConfig
from benchmarks.sregym.adapter.native_operator import (
    NativeInstallConfig,
    collect_native_receipt,
    install_existing,
    validate_code_path_overrides,
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
    """Benchmark settings passed to the native operator and persistent runner.

    An operator knob left ``None`` is not overridden: the benchmark profile, ``config_files`` and the production
    defaults decide it (``docs/config.md``), and Go validates every value.
    """

    repository: Path
    namespace: str
    application: str
    controller_image: str
    responder_image: str
    repository_pvc: str
    credentials_secret: str
    model: str
    timeout_seconds: int
    verification_timeout_seconds: int | None = None
    wait_for_completion: bool = False
    repair_policy: str | None = None
    agent_provider: str = ""
    replay: str = "on"
    agent_effort: str = ""
    lifecycle_timeout_seconds: int | None = None
    reflection_guidance: str | None = None
    force_reflection: bool | None = None
    frozen_memory: bool = False
    external_incidents: bool = False
    code_path: str = "go"
    code_path_overrides: str = ""

    max_response_loops: int | None = None
    healthy_baseline: bool | None = None
    permissive_responder: bool = False
    responder_namespaces: tuple[str, ...] = ()
    # Operator config overlay files passed after the benchmark profile, in order, and further keys merged last.
    config_files: tuple[Path, ...] = ()
    settings: dict[str, Any] = dataclass_field(default_factory=dict)
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
    max_deployment_attempts: int | None = None
    agent_registrations: Path | None = None
    # ProtoJSON DeploymentSource (see ``deployment_source.py``) recorded as ``.sdo/deployment/plan.yaml`` by
    # install-existing; ``None`` requires the seeded repository to carry a plan already.
    deployment_source: dict[str, Any] | None = None
    # ProtoJSON DeploymentPlan the human provides (see ``deployment_source.py``); ``None``: agent-authored.
    deployment_plan: dict[str, Any] | None = None
    # Install-time deployment policy the lane chooses (registry allowlist, scope grants); default: none.
    deployment_policy: DeploymentPolicyConfig = DeploymentPolicyConfig()
    # The one branch SDO commits to and deploys from (``--source-branch-url`` / ``--source-branch``); both or
    # neither. Engineers, and the benchmark replayer, push to it.
    source_branch_url: str = ""
    source_branch: str = ""
    shared_accepted_id: str = ""
    # Evidence sink node directory and run ID (see ``NativeInstallConfig``); empty: no sink.
    evidence_sink_host_path: str = ""
    evidence_run_id: str = ""

    def __post_init__(self) -> None:
        if bool(self.source_branch_url) != bool(self.source_branch):
            raise ValueError("source_branch_url and source_branch must be given together")
        if self.source_branch.strip().lower() in {"main", "master"}:
            raise ValueError("the source branch must not be main or master")
        if type(self.external_incidents) is not bool:
            raise TypeError("external_incidents must be a boolean")
        if self.external_incidents and not self.frozen_memory:
            raise ValueError("external incidents require frozen memory")
        if type(self.frozen_memory) is not bool:
            raise TypeError("frozen_memory must be a boolean")
        if type(self.permissive_responder) is not bool:
            raise TypeError("permissive_responder must be a boolean")
        if self.responder_namespaces and not self.permissive_responder:
            raise ValueError("responder_namespaces requires permissive_responder")
        for field in (
            "namespace",
            "application",
            "controller_image",
            "responder_image",
            "repository_pvc",
            "credentials_secret",
            "model",
        ):
            if not getattr(self, field).strip():
                raise ValueError(f"{field} must be non-empty")
        if self.timeout_seconds <= 0:
            raise ValueError("controller timeouts must be positive")
        if self.controller_namespace is not None and not self.controller_namespace.strip():
            raise ValueError("controller namespace must be non-empty")
        # The harness branches on these two; Go validates every other value when it resolves the config.
        if self.replay not in ("on", "off"):
            raise ValueError("replay must be on or off")
        if self.code_path not in ("go", "shell"):
            raise ValueError("code_path must be go or shell")
        validate_code_path_overrides(self.code_path_overrides)

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
        repository_pvc=config.repository_pvc,
        credentials_secret=config.credentials_secret,
        model=config.model,
        kubeconfig=Path(kubeconfig),
        agent_provider=config.agent_provider,
        replay=config.replay,
        agent_effort=config.agent_effort,
        lifecycle_timeout_seconds=config.lifecycle_timeout_seconds,
        repair_policy=config.repair_policy,
        reflection_guidance=config.reflection_guidance,
        force_reflection=config.force_reflection,
        frozen_memory=config.frozen_memory,
        external_incidents=config.external_incidents,
        registration_configmap="sdo-agent-registrations" if config.agent_registrations else None,
        code_path=config.code_path,
        code_path_overrides=config.code_path_overrides or None,
        healthy_baseline=config.healthy_baseline,
        permissive_responder=config.permissive_responder or None,
        responder_namespaces=config.responder_namespaces,
        max_response_loops=config.max_response_loops,
        response_timeout_seconds=config.timeout_seconds,
        verification_timeout_seconds=config.verification_timeout_seconds,
        responder_env=responder_env,
        settings=config.settings,
        config_files=(BENCH_PROFILE, *config.config_files),
        deployment_source=config.deployment_source,
        deployment_policy=config.deployment_policy,
        shared_accepted_id=config.shared_accepted_id,
        evidence_sink_host_path=config.evidence_sink_host_path,
        evidence_run_id=config.evidence_run_id,
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
    native = native_install_config(config)
    if config.agent_registrations:
        from benchmarks.sregym.adapter.driver import publish_agent_registrations

        assert native.kubeconfig is not None
        publish_agent_registrations(
            config.agent_registrations,
            native.control_namespace,
            native.kubeconfig,
            native.responder_image,
            {native.controller_image, native.responder_image},
        )
    return install_existing(native).reused


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
        "`sdo detect` until the tests pass. "
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
