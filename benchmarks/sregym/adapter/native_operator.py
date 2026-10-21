"""Benchmark launch bridge for the production Go operator command.

This module passes benchmark configuration to the typed Go CLI. It never
implements a commit broker or writes operational memory itself.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.protocol.native_receipt import native_production_receipt

_OPERATOR_MODULE = Path(__file__).resolve().parents[3] / "sdo" / "operator"
_IMAGE_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
_REGISTRY_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[1-9][0-9]{0,4})?\Z")


class NativeOperatorError(RuntimeError):
    """The native Go operation failed or returned an invalid receipt."""


@dataclass(frozen=True)
class NativeInstallConfig:
    repository: Path
    namespace: str
    control_namespace: str
    application: str
    controller_image: str
    responder_image: str
    validator_image: str
    repository_pvc: str
    credentials_secret: str
    model: str
    kubeconfig: Path | None = None
    agent_provider: str = "codex"
    repair_policy: str = "commit"
    late_findings: str = "off"
    reflection_session: str = "resume"
    reflection_guidance: str = "baseline"
    force_reflection: bool = False
    max_follow_ups: int = 0
    max_response_loops: int = 5
    follow_up_cooldown_seconds: int = 30
    response_timeout_seconds: int = 1800
    verification_timeout_seconds: int = 120
    duration_seconds: int | None = None
    exit_after_closure: bool = False
    healthy_baseline: bool = False
    registration_configmap: str | None = None
    responder_env: tuple[str, ...] = ()
    deployed_manifest_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.repository.is_absolute():
            raise ValueError("accepted repository path must be absolute")
        if self.kubeconfig is not None and not self.kubeconfig.is_absolute():
            raise ValueError("kubeconfig path must be absolute")
        for field in (
            "namespace",
            "control_namespace",
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
        if self.agent_provider not in ("codex", "claude"):
            raise ValueError("agent_provider must be codex or claude")
        if self.repair_policy not in ("commit", "recorded-actions"):
            raise ValueError("repair_policy must be commit or recorded-actions")
        if self.late_findings not in ("off", "pull"):
            raise ValueError("late_findings must be off or pull")
        if self.reflection_session not in ("fresh", "resume"):
            raise ValueError("reflection_session must be fresh or resume")
        if self.reflection_guidance not in ("baseline", "generalize", "generalize-spec"):
            raise ValueError("reflection_guidance must be baseline, generalize, or generalize-spec")
        if self.max_follow_ups < 0 or self.follow_up_cooldown_seconds <= 0 or self.max_response_loops <= 0:
            raise ValueError("follow-up count must be nonnegative and cooldown positive")
        if self.response_timeout_seconds <= 0 or self.verification_timeout_seconds <= 0:
            raise ValueError("controller timeouts must be positive")
        if self.duration_seconds is not None and self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when set")
        for value in self.responder_env:
            name, separator, content = value.partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or "\n" in content or "\r" in content:
                raise ValueError(f"invalid responder environment {value!r}")

    def common_args(self) -> list[str]:
        args = [
            "--repository",
            str(self.repository),
            "--namespace",
            self.namespace,
            "--control-namespace",
            self.control_namespace,
            "--application",
            self.application,
            "--controller-image",
            self.controller_image,
            "--responder-image",
            self.responder_image,
            "--validator-image",
            self.validator_image,
            "--repository-pvc",
            self.repository_pvc,
            "--credentials-secret",
            self.credentials_secret,
            "--model",
            self.model,
            "--agent-provider",
            self.agent_provider,
        ]
        if self.kubeconfig is not None:
            args.extend(("--kubeconfig", str(self.kubeconfig)))
        if self.registration_configmap:
            args.extend(("--agent-registration-configmap", self.registration_configmap))
        if self.healthy_baseline:
            args.append("--healthy-baseline")
        for value in self.responder_env:
            args.extend(("--responder-env", value))
        return args


@dataclass(frozen=True)
class NativeHostConfig:
    install: NativeInstallConfig
    health_objective: str
    operation_id: str
    buildkit_image: str
    registry_push_secret: str
    registry_host: str
    registry_insecure: bool = False
    max_attempts: int = 3
    agent_registrations: Path | None = None

    def __post_init__(self) -> None:
        if not self.health_objective.strip() or not self.operation_id.strip():
            raise ValueError("host launch requires health objective and operation ID")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if not _IMAGE_DIGEST.search(self.buildkit_image):
            raise ValueError("BuildKit image must be pinned by sha256 digest")
        if not self.registry_push_secret.strip() or self.registry_push_secret == self.install.credentials_secret:
            raise ValueError("registry push Secret must be separate from agent credentials")
        if _REGISTRY_HOST.fullmatch(self.registry_host) is None:
            raise ValueError("registry host must be a host[:port] without a scheme or path")
        if self.agent_registrations is not None and not self.agent_registrations.is_absolute():
            raise ValueError("agent registrations path must be absolute")


@dataclass(frozen=True)
class NativeInstallReceipt:
    installed: bool
    reused: bool
    fingerprint: str
    accepted_commit: str
    controller_logs: str = ""


@dataclass(frozen=True)
class NativeExistingReceipt:
    job_uid: str
    pod_uid: str
    fingerprint: str
    accepted_commit: str


def _operator_command() -> tuple[list[str], Path | None]:
    specified = os.environ.get("SDO_OPERATOR_BIN", "").strip()
    if specified:
        path = Path(specified)
        if not path.is_absolute():
            raise NativeOperatorError("SDO_OPERATOR_BIN must be an absolute path")
        return [str(path)], None
    installed = shutil.which("sdo-operator")
    if installed:
        return [installed], None
    if not _OPERATOR_MODULE.is_dir():
        raise NativeOperatorError("Go operator source is unavailable; set SDO_OPERATOR_BIN")
    return ["go", "run", "-mod=mod", "./cmd/sdo-operator"], _OPERATOR_MODULE


def _execute(command: str, args: list[str], *, timeout: int) -> str:
    prefix, cwd = _operator_command()
    argv = [*prefix, command, *args]
    try:
        result = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise NativeOperatorError(f"Go operator {command} could not run: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-4096:]
        raise NativeOperatorError(f"Go operator {command} failed ({result.returncode}): {detail}")
    return result.stdout


def _run(command: str, args: list[str], *, timeout: int) -> dict[str, Any]:
    stdout = _execute(command, args, timeout=timeout)
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise NativeOperatorError(f"Go operator {command} did not return one JSON object: {stdout[:512]!r}") from exc
    if not isinstance(value, dict):
        raise NativeOperatorError(f"Go operator {command} did not return one JSON object")
    return value


def sync_existing(config: NativeInstallConfig) -> None:
    args = [
        "--repository",
        str(config.repository),
        "--control-namespace",
        config.control_namespace,
        "--repository-pvc",
        config.repository_pvc,
        "--controller-image",
        config.controller_image,
    ]
    if config.kubeconfig is not None:
        args.extend(("--kubeconfig", str(config.kubeconfig)))
    output = _execute("sync-existing", args, timeout=600)
    if output.strip():
        raise NativeOperatorError("Go sync-existing returned unexpected stdout")


def set_maintenance(
    control_namespace: str,
    *,
    paused: bool,
    generation: str,
    kubeconfig: Path | None = None,
) -> None:
    if not control_namespace.strip() or not generation.strip():
        raise ValueError("maintenance requires control namespace and generation")
    if kubeconfig is not None and not kubeconfig.is_absolute():
        raise ValueError("kubeconfig path must be absolute")
    args = ["--control-namespace", control_namespace, f"--paused={str(paused).lower()}", "--generation", generation]
    if kubeconfig is not None:
        args.extend(("--kubeconfig", str(kubeconfig)))
    output = _execute("maintenance", args, timeout=120)
    if output.strip():
        raise NativeOperatorError("Go maintenance returned unexpected stdout")


def read_incident_evidence(
    config: NativeInstallConfig, incident_id: str, *, sync_repository: bool = False
) -> dict[str, Any]:
    if not incident_id.strip():
        raise ValueError("incident ID must be non-empty")
    args = [
        "--control-namespace",
        config.control_namespace,
        "--repository-pvc",
        config.repository_pvc,
        "--controller-image",
        config.controller_image,
        "--incident-id",
        incident_id,
    ]
    if config.kubeconfig is not None:
        args.extend(("--kubeconfig", str(config.kubeconfig)))
    if sync_repository:
        args.extend(("--repository", str(config.repository)))
    value = _run("incident-evidence", args, timeout=120)
    ledger = value.get("ledger")
    if (
        value.get("incident_id") != incident_id
        or not isinstance(value.get("responder_job_name"), str)
        or not isinstance(ledger, dict)
        or ledger.get("incident_id") != incident_id
    ):
        raise NativeOperatorError("Go incident evidence is not correlated with the requested incident")
    return value


def _kubectl_json(config: NativeInstallConfig, *args: str) -> dict[str, Any]:
    command = ["kubectl"]
    if config.kubeconfig is not None:
        command.extend(("--kubeconfig", str(config.kubeconfig)))
    command.extend(("--namespace", config.control_namespace, *args, "-o", "json"))
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise NativeOperatorError(f"Kubernetes receipt query failed: {exc}") from exc
    if completed.returncode != 0:
        raise NativeOperatorError(f"Kubernetes receipt query failed: {(completed.stderr or completed.stdout)[-4096:]}")
    try:
        document = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise NativeOperatorError("Kubernetes receipt query returned invalid JSON") from exc
    if not isinstance(document, dict):
        raise NativeOperatorError("Kubernetes receipt query returned no object")
    return document


def collect_native_receipt(
    config: NativeInstallConfig, incident_id: str, *, sync_repository: bool = False
) -> dict[str, Any]:
    evidence = read_incident_evidence(config, incident_id, sync_repository=sync_repository)
    job_name = evidence["responder_job_name"]
    job = _kubectl_json(config, "get", f"job/{job_name}")
    job_metadata = job.get("metadata")
    job_status = job.get("status")
    if not isinstance(job_metadata, dict) or not isinstance(job_status, dict):
        raise NativeOperatorError("responder Job has no metadata or status")
    job_uid = job_metadata.get("uid")
    if not isinstance(job_uid, str) or not job_uid or job_status.get("succeeded") != 1:
        raise NativeOperatorError("responder Job is not durably successful")
    controller_job = _kubectl_json(config, "get", "job/sdo-controller-run")
    controller_metadata = controller_job.get("metadata")
    controller_job_uid = controller_metadata.get("uid") if isinstance(controller_metadata, dict) else None
    if not isinstance(controller_job_uid, str) or not controller_job_uid:
        raise NativeOperatorError("controller Job has no UID")
    pods = _kubectl_json(config, "get", "pods", "--selector", "job-name=sdo-controller-run").get("items")
    if not isinstance(pods, list):
        raise NativeOperatorError("controller Pod query lacks items")
    eligible = [
        pod
        for pod in pods
        if isinstance(pod, dict)
        and isinstance(pod.get("status"), dict)
        and pod["status"].get("phase") in ("Running", "Succeeded")
        and isinstance(pod.get("metadata"), dict)
        and any(
            isinstance(owner, dict) and owner.get("uid") == controller_job_uid
            for owner in pod["metadata"].get("ownerReferences", [])
        )
    ]
    if len(eligible) != 1 or not isinstance(eligible[0].get("metadata"), dict):
        raise NativeOperatorError("receipt requires exactly one current controller Pod")
    controller_uid = eligible[0]["metadata"].get("uid")
    if not isinstance(controller_uid, str) or not controller_uid:
        raise NativeOperatorError("controller Pod has no UID")
    return native_production_receipt(
        evidence["ledger"],
        namespace=config.namespace,
        control_namespace=config.control_namespace,
        controller_image=config.controller_image,
        responder_image=config.responder_image,
        validator_image=config.validator_image,
        controller_pod_uid=controller_uid,
        responder_job_evidence=[
            {"incident_id": incident_id, "job_name": job_name, "job_uid": job_uid, "succeeded": True}
        ],
    )


def export_runtime_artifacts(config: NativeInstallConfig, output_dir: Path) -> Path:
    if not output_dir.is_absolute():
        raise ValueError("runtime artifact output directory must be absolute")
    args = [
        "--control-namespace",
        config.control_namespace,
        "--repository-pvc",
        config.repository_pvc,
        "--controller-image",
        config.controller_image,
        "--output-dir",
        str(output_dir),
    ]
    if config.kubeconfig is not None:
        args.extend(("--kubeconfig", str(config.kubeconfig)))
    value = _run("runtime-artifacts", args, timeout=600)
    expected = output_dir / "sdo_runtime"
    if value.get("directory") != str(expected):
        raise NativeOperatorError("Go runtime artifact export returned an unexpected directory")
    return expected


def install_existing(config: NativeInstallConfig) -> NativeInstallReceipt:
    args = config.common_args()
    args.extend(("--repair-policy", config.repair_policy))
    args.extend(("--late-findings", config.late_findings))
    args.extend(("--reflection-session", config.reflection_session))
    args.extend(("--reflection-guidance", config.reflection_guidance))
    if config.force_reflection:
        args.append("--force-reflection")
    for path in config.deployed_manifest_paths:
        args.extend(("--deployed-manifest-path", path))
    args.extend(("--max-follow-ups", str(config.max_follow_ups)))
    args.extend(("--max-response-loops", str(config.max_response_loops)))
    args.extend(("--follow-up-cooldown", f"{config.follow_up_cooldown_seconds}s"))
    args.extend(("--response-timeout", f"{config.response_timeout_seconds}s"))
    args.extend(("--verification-timeout", f"{config.verification_timeout_seconds}s"))
    if config.duration_seconds is not None:
        args.extend(("--duration", f"{config.duration_seconds}s"))
    if config.exit_after_closure:
        args.append("--exit-after-closure")
    value = _run("install-existing", args, timeout=config.response_timeout_seconds + 600)
    if value.get("installed") is not True or type(value.get("reused")) is not bool:
        raise NativeOperatorError("Go install-existing returned an invalid installation receipt")
    fingerprint = value.get("fingerprint")
    commit = value.get("accepted_commit")
    if (
        not isinstance(fingerprint, str)
        or not fingerprint
        or not isinstance(commit, str)
        or not re.fullmatch(r"[0-9a-f]{40}", commit)
    ):
        raise NativeOperatorError("Go install-existing returned invalid provenance")
    return NativeInstallReceipt(True, value["reused"], fingerprint, commit, str(value.get("controller_logs") or ""))


def inspect_existing(config: NativeInstallConfig) -> NativeExistingReceipt:
    args = [
        "--control-namespace",
        config.control_namespace,
        "--namespace",
        config.namespace,
        "--application",
        config.application,
        "--repository-pvc",
        config.repository_pvc,
        "--controller-image",
        config.controller_image,
    ]
    if config.kubeconfig is not None:
        args.extend(("--kubeconfig", str(config.kubeconfig)))
    value = _run("inspect-existing", args, timeout=120)
    for field in ("job_uid", "pod_uid", "fingerprint"):
        if not isinstance(value.get(field), str) or not value[field].strip():
            raise NativeOperatorError(f"Go inspect-existing lacks {field}")
    commit = value.get("accepted_commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise NativeOperatorError("Go inspect-existing lacks a full accepted commit")
    return NativeExistingReceipt(value["job_uid"], value["pod_uid"], value["fingerprint"], commit)


def launch_host(config: NativeHostConfig) -> dict[str, Any]:
    args = config.install.common_args()
    args.extend(("--late-findings", config.install.late_findings))
    args.extend(("--reflection-session", config.install.reflection_session))
    args.extend(("--reflection-guidance", config.install.reflection_guidance))
    if config.install.force_reflection:
        args.append("--force-reflection")
    args.extend(("--max-follow-ups", str(config.install.max_follow_ups)))
    args.extend(("--max-response-loops", str(config.install.max_response_loops)))
    args.extend(("--follow-up-cooldown", f"{config.install.follow_up_cooldown_seconds}s"))
    args.extend(("--repair-policy", config.install.repair_policy))
    args.extend(("--response-timeout", f"{config.install.response_timeout_seconds}s"))
    args.extend(("--verification-timeout", f"{config.install.verification_timeout_seconds}s"))
    if config.install.duration_seconds is not None:
        args.extend(("--controller-duration", f"{config.install.duration_seconds}s"))
    if config.install.exit_after_closure:
        args.append("--exit-after-closure")
    args.extend(("--health-objective", config.health_objective, "--operation-id", config.operation_id))
    args.extend(("--max-attempts", str(config.max_attempts)))
    args.extend(("--buildkit-image", config.buildkit_image, "--registry-push-secret", config.registry_push_secret))
    args.extend(("--registry-host", config.registry_host))
    if config.registry_insecure:
        args.append("--registry-insecure")
    if config.agent_registrations is not None:
        args.extend(("--agent-registrations", str(config.agent_registrations)))
    value = _run("host", args, timeout=config.max_attempts * config.install.response_timeout_seconds + 900)
    if value.get("installed") is not True or value.get("operation_id") != config.operation_id:
        raise NativeOperatorError("Go host returned an invalid operation receipt")
    for field in ("goal_commit", "schema_commit", "source_commit", "architecture_commit", "judge_commit"):
        if not isinstance(value.get(field), str) or not re.fullmatch(r"[0-9a-f]{40}", value[field]):
            raise NativeOperatorError(f"Go host receipt lacks {field}")
    return value
