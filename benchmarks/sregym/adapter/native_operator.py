"""Benchmark launch bridge for the production Go operator command.

This module passes benchmark configuration to the typed Go CLI. It never
implements a commit broker or writes operational memory itself. Operator
configuration crosses as ``--config`` files (the benchmark profile, then the
lane's overlay) and ``SDO_*`` variables, never as one flag per knob
(``docs/config.md``).
"""

from __future__ import annotations

import atexit
import json
import os
import re
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from benchmarks.fuzzops import config as operator_config
from benchmarks.sregym.adapter.budgets import WaitKind, wait_for
from benchmarks.sregym.adapter.deployment_source import DeploymentPolicyConfig, plan_flag_value, source_flag_value
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sdo.contracts import ControllerObservation

_OPERATOR_MODULE = Path(__file__).resolve().parents[3] / "sdo" / "operator"
# SREGym's venv imports this module without SDO's agent dependencies, so the
# effort names come from the shared worker-environment manifest itself.
_AGENT_ENVIRONMENT = json.loads(
    (Path(__file__).resolve().parents[3] / "sdo" / "contracts" / "agent_env.json").read_text("utf-8")
)
AGENT_EFFORTS: tuple[str, ...] = tuple(_AGENT_ENVIRONMENT["efforts"])
_IMAGE_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
_REGISTRY_HOST = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[1-9][0-9]{0,4})?\Z")


class NativeOperatorError(RuntimeError):
    """The native Go operation failed or returned an invalid receipt."""


class SharedRepositoryControllerNotReady(NativeOperatorError):
    """Repository identity cannot be checked until the controller is Ready."""


class SharedRepositoryExecFailed(NativeOperatorError):
    """The controller identity read failed; it does not establish an ID mismatch."""


class SharedRepositoryIdentityMismatch(NativeOperatorError):
    """A successful identity read disagrees with the configured shared repository."""


def validate_code_path_overrides(value: str) -> None:
    """Accept an empty value or a JSON object; the Go CLI validates the override names."""
    if not value:
        return
    try:
        document = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"code_path_overrides must be JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("code_path_overrides must be a JSON object")


def validate_evidence_sink(host_path: str, run_id: str) -> None:
    """Shape checks only; the Go installer owns the sink rules (volume kind, run ID charset)."""

    if host_path and not host_path.startswith("/"):
        raise ValueError("evidence_sink_host_path must be an absolute node path")
    if run_id and not host_path:
        raise ValueError("evidence_run_id requires evidence_sink_host_path")


def _seconds(value: int | None) -> str | None:
    return None if value is None else f"{value}s"


def _present(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None and value != [] and value != {}}


@dataclass(frozen=True)
class NativeInstallConfig:
    """One lane's ``install-existing``/``host`` launch.

    A knob left ``None`` is not overridden: the config files (``config_files``, the benchmark profile by default)
    and the production defaults decide it, and Go validates every value.
    """

    repository: Path
    namespace: str
    control_namespace: str
    application: str
    controller_image: str
    responder_image: str
    repository_pvc: str
    credentials_secret: str
    model: str
    kubeconfig: Path | None = None
    # Per-run agent selection and evidence sink: ``SDO_*`` variables; empty keeps the configured value.
    agent_provider: str = ""
    agent_effort: str = ""
    # ``frozen_memory`` alone is mode ``frozen``; with ``external_incidents`` it is ``external`` (``SDO_MODE``).
    frozen_memory: bool = False
    external_incidents: bool = False
    # Lane overlay (written to a temporary config file after ``config_files``).
    replay: str | None = None
    lifecycle_timeout_seconds: int | None = None
    repair_policy: str | None = None
    reflection_guidance: str | None = None
    force_reflection: bool | None = None
    code_path: str | None = None
    code_path_overrides: str | None = None
    max_response_loops: int | None = None
    response_timeout_seconds: int | None = None
    verification_timeout_seconds: int | None = None
    healthy_baseline: bool | None = None
    permissive_responder: bool | None = None
    responder_namespaces: tuple[str, ...] = ()
    responder_env: tuple[str, ...] = ()
    # Install-time deployment policy (registry allowlist, scope grants); the default is no policy.
    deployment_policy: DeploymentPolicyConfig = DeploymentPolicyConfig()
    # Any further config keys of the lane or experiment, merged over the fields above.
    settings: dict[str, Any] = field(default_factory=dict)
    config_files: tuple[Path, ...] = (operator_config.BENCH_PROFILE,)
    duration_seconds: int | None = None
    exit_after_closure: bool = False
    registration_configmap: str | None = None
    deployment_source: dict[str, Any] | None = None
    shared_accepted_id: str = ""
    # Evidence sink: a directory on the node (for kind, an extraMounts target) that receives run evidence and
    # outlives the control namespace; empty keeps evidence on the repository PVC. The run ID names the run directory.
    evidence_sink_host_path: str = ""
    evidence_run_id: str = ""

    def __post_init__(self) -> None:
        validate_evidence_sink(self.evidence_sink_host_path, self.evidence_run_id)
        for name in ("frozen_memory", "external_incidents"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")
        if self.external_incidents and not self.frozen_memory:
            raise ValueError("external incidents require frozen memory")
        if self.shared_accepted_id and not re.fullmatch(r"[0-9a-f]{32}", self.shared_accepted_id):
            raise ValueError("shared accepted identity must be a 128-bit hexadecimal ID")
        if not self.repository.is_absolute():
            raise ValueError("accepted repository path must be absolute")
        if self.kubeconfig is not None and not self.kubeconfig.is_absolute():
            raise ValueError("kubeconfig path must be absolute")
        if not isinstance(self.settings, dict):
            raise TypeError("settings must be a mapping of config sections")
        for field_name in (
            "namespace",
            "control_namespace",
            "application",
            "controller_image",
            "responder_image",
            "repository_pvc",
            "credentials_secret",
            "model",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must be non-empty")
        if self.code_path_overrides:
            validate_code_path_overrides(self.code_path_overrides)
        if self.duration_seconds is not None and self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when set")
        for value in self.responder_env:
            name, separator, content = value.partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or "\n" in content or "\r" in content:
                raise ValueError(f"invalid responder environment {value!r}")

    @property
    def mode(self) -> str:
        """``SDO_MODE``; empty leaves the configured mode."""
        if self.external_incidents:
            return "external"
        return "frozen" if self.frozen_memory else ""

    def overlay(self) -> dict[str, Any]:
        """The lane's config overlay: every field set here, then ``settings``."""
        sections = {
            "agent": _present(
                {
                    "lifecycle_timeout": _seconds(self.lifecycle_timeout_seconds),
                    "code_path": self.code_path,
                    "code_path_overrides": self.code_path_overrides or None,
                    "extra_env": list(self.responder_env),
                }
            ),
            "response": _present(
                {
                    "timeout": _seconds(self.response_timeout_seconds),
                    "verification_timeout": _seconds(self.verification_timeout_seconds),
                    "max_loops": self.max_response_loops,
                    "repair_policy": self.repair_policy,
                    "permissive": self.permissive_responder,
                    "permissive_namespaces": list(self.responder_namespaces),
                }
            ),
            "learning": _present(
                {
                    "replay": self.replay,
                    "reflection_guidance": self.reflection_guidance,
                    "force_reflection": self.force_reflection,
                    "healthy_baseline": self.healthy_baseline,
                }
            ),
            "deployment": _present({"policy": self.deployment_policy.settings()}),
        }
        return operator_config.merge(_present(sections), self.settings)

    def environment(self) -> dict[str, str]:
        """The ``SDO_*`` variables of the launch: agent, mode and evidence sink."""
        return operator_config.operator_env(
            provider=self.agent_provider,
            model=self.model,
            effort=self.agent_effort,
            evidence_host_path=self.evidence_sink_host_path,
            evidence_run_id=self.evidence_run_id,
            mode=self.mode,
        )

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
            "--repository-pvc",
            self.repository_pvc,
            "--credentials-secret",
            self.credentials_secret,
        ]
        if self.kubeconfig is not None:
            args.extend(("--kubeconfig", str(self.kubeconfig)))
        if self.registration_configmap:
            args.extend(("--agent-registration-configmap", self.registration_configmap))
        return args


@contextmanager
def launch_config(config: NativeInstallConfig) -> Iterator[tuple[list[str], dict[str, str]]]:
    """``--config`` arguments and ``SDO_*`` variables of one launch; the overlay file lives for the block."""
    files = list(config.config_files)
    overlay = config.overlay()
    written = operator_config.temporary_overlay(overlay, prefix="sdo-lane-config-") if overlay else None
    try:
        if written is not None:
            files.append(written)
        yield operator_config.config_args(files), config.environment()
    finally:
        if written is not None:
            written.unlink(missing_ok=True)


def resolved_settings(config: NativeInstallConfig) -> dict[str, Any]:
    """The configuration this launch resolves to (``sdo-operator config print``)."""
    prefix, _ = operator_command()
    with launch_config(config) as (args, env):
        files = [Path(value) for flag, value in zip(args[::2], args[1::2], strict=True) if flag == "--config"]
        return operator_config.resolved(prefix, files, env)


def configured_agent_effort(config_files: tuple[Path, ...] = ()) -> str:
    """The agent effort a lane runs at when it sets none: its config files over the Go default."""
    prefix, _ = operator_command()
    files = (operator_config.BENCH_PROFILE, *config_files)
    settings = operator_config.resolved(prefix, files, {}, complete=False)
    return str(settings["agent"]["effort"])


def _launch_seconds(settings: dict[str, Any], section: str, key: str) -> float:
    return operator_config.duration_seconds(str(settings[section][key]))


@dataclass(frozen=True)
class NativeHostConfig:
    install: NativeInstallConfig
    health_objective: str
    operation_id: str
    buildkit_image: str
    registry_push_secret: str
    registry_host: str
    registry_insecure: bool = False
    # Deployment attempts (``deployment.max_attempts``); ``None`` keeps the configured value.
    max_attempts: int | None = None
    agent_registrations: Path | None = None
    # ProtoJSON DeploymentPlan for an app that ships its own deployment; ``None``: the deployer agent authors one.
    deployment_plan: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.operation_id.strip():
            raise ValueError("host launch requires operation ID")
        if self.max_attempts is not None and self.max_attempts < 1:
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


# A cold build downloads modules and compiles the whole controller graph.
_OPERATOR_BUILD_TIMEOUT_SECONDS = int(wait_for(WaitKind.ROLLOUT))


@lru_cache(maxsize=1)
def _source_operator(module: Path) -> str:
    directory = Path(tempfile.mkdtemp(prefix="sdo-operator-"))
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    binary = directory / "sdo-operator"
    try:
        built = subprocess.run(
            ["go", "build", "-mod=mod", "-o", str(binary), "./cmd/sdo-operator"],
            cwd=module,
            capture_output=True,
            text=True,
            timeout=_OPERATOR_BUILD_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise NativeOperatorError(f"Go operator could not build: {exc}") from exc
    if built.returncode:
        raise NativeOperatorError(f"Go operator build failed: {built.stderr[-4096:]}")
    return str(binary)


def operator_command() -> tuple[list[str], Path | None]:
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
    return [_source_operator(_OPERATOR_MODULE)], _OPERATOR_MODULE


def execute_operator(
    command: str,
    args: list[str],
    *,
    timeout: float,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    prefix, cwd = operator_command()
    argv = [*prefix, command, *args]
    environment = {**os.environ, **env} if env else None
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise NativeOperatorError(f"Go operator {command} could not run: {exc}") from exc
    if result.returncode != 0:
        detail = "\n".join(part.strip()[-2048:] for part in (result.stderr, result.stdout) if part.strip())
        raise NativeOperatorError(f"Go operator {command} failed ({result.returncode}): {detail}")
    return result.stdout


def observe_controller_state(
    state: dict[str, Any],
    *,
    known: set[str] | None = None,
    incident_id: str = "",
    timeout: float = wait_for(WaitKind.COMMAND),
) -> ControllerObservation:
    # The SREGym worker runs execute_operator without SDO's contract dependencies.
    from sdo.contracts import ControllerObservation, ControllerObservationRequest, validate

    request = ControllerObservationRequest(known_incident_ids=sorted(known or ()), incident_id=incident_id)
    payload = json_format.MessageToDict(request, preserving_proto_field_name=True)
    payload["state"] = state
    stdout = execute_operator("observe-controller", [], timeout=timeout, input_text=json.dumps(payload))
    try:
        result = json_format.Parse(stdout, ControllerObservation())
        if result.HasField("failure"):
            validate(result.failure)
    except (json_format.ParseError, ValueError) as exc:
        raise NativeOperatorError(f"Go controller observation returned invalid ProtoJSON: {exc}") from exc
    return result


def _run(command: str, args: list[str], *, timeout: int, env: dict[str, str] | None = None) -> dict[str, Any]:
    stdout = execute_operator(command, args, timeout=timeout, env=env)
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise NativeOperatorError(f"Go operator {command} did not return one JSON object: {stdout[:512]!r}") from exc
    if not isinstance(value, dict):
        raise NativeOperatorError(f"Go operator {command} did not return one JSON object")
    return value


def sync_existing(config: NativeInstallConfig) -> None:
    if config.shared_accepted_id:
        assert_shared_repository(config)
        return
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
    output = execute_operator("sync-existing", args, timeout=wait_for(WaitKind.OPERATION))
    if output.strip():
        raise NativeOperatorError("Go sync-existing returned unexpected stdout")


MEMORY_MIRROR_MARKER = "sdo-memory-remote"


def refresh_memory_mirror(repository: Path) -> None:
    """Bring a host mirror of a remote memory branch to the branch head; no-op for other checkouts.

    Lanes sharing memory through a remote branch have no host-visible accepted checkout.
    Their host workspace is a mirror marked by ``.git/sdo-memory-remote`` ({url, branch});
    every shared-identity read refreshes it first, so evidence reads see what the brokers pushed.
    """
    marker = repository / ".git" / MEMORY_MIRROR_MARKER
    if not marker.is_file():
        return
    remote = json.loads(marker.read_text(encoding="utf-8"))
    environment = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    for args in (
        ("fetch", "--no-tags", "--quiet", "--", remote["url"], f"+refs/heads/{remote['branch']}:refs/sdo/upstream"),
        ("reset", "--hard", "--quiet", "refs/sdo/upstream"),
    ):
        subprocess.run(
            host_git_command(repository, *args),
            check=True,
            capture_output=True,
            env=environment,
            timeout=wait_for(WaitKind.COMMAND),
        )


def assert_shared_repository(config: NativeInstallConfig) -> None:
    """Prove the controller and host share identity before bypassing archive transport."""
    refresh_memory_mirror(config.repository)
    identity = config.repository / ".git" / "sdo-shared-id"
    if not config.shared_accepted_id or identity.read_text().strip() != config.shared_accepted_id:
        raise SharedRepositoryIdentityMismatch(
            f"shared accepted repository host identity differs: repository={config.repository}, "
            f"control_namespace={config.control_namespace}; verify the host shared-ID file"
        )
    pods = _kubectl_json(config, "get", "pods", "--selector", "job-name=sdo-controller-run").get("items", [])
    active = [pod for pod in pods if pod.get("status", {}).get("phase") == "Running"]
    context = f"control_namespace={config.control_namespace}"
    if len(active) != 1 or not any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in active[0].get("status", {}).get("conditions", [])
    ):
        raise SharedRepositoryControllerNotReady(
            f"shared identity requires one Ready controller: {context}, running_pods={len(active)}; "
            "inspect controller Pod readiness and container runtime before retrying"
        )
    context += f", pod={active[0]['metadata']['name']}"
    args = ["kubectl"]
    if config.kubeconfig:
        args.extend(["--kubeconfig", str(config.kubeconfig)])
    args.extend(
        [
            "-n",
            config.control_namespace,
            "exec",
            active[0]["metadata"]["name"],
            "--",
            "cat",
            "/workspace/application/.git/sdo-shared-id",
        ]
    )
    try:
        observed = subprocess.run(args, text=True, capture_output=True, check=False, timeout=wait_for(WaitKind.COMMAND))
    except (subprocess.TimeoutExpired, OSError) as error:
        raise SharedRepositoryExecFailed(
            f"controller shared identity exec failed: {context}, failure={type(error).__name__}; "
            "inspect Pod exec connectivity and container runtime"
        ) from None
    if observed.returncode:
        raise SharedRepositoryExecFailed(
            f"controller shared identity exec failed: {context}, exit_code={observed.returncode}; "
            "inspect Pod exec connectivity and container runtime"
        )
    if observed.stdout.strip() != config.shared_accepted_id:
        raise SharedRepositoryIdentityMismatch(
            f"controller shared accepted repository identity differs: {context}; verify the shared repository mount"
        )


def set_maintenance(
    control_namespace: str,
    *,
    paused: bool,
    generation: str,
    preserve_baseline: bool = False,
    kubeconfig: Path | None = None,
) -> None:
    if not control_namespace.strip() or not generation.strip():
        raise ValueError("maintenance requires control namespace and generation")
    if kubeconfig is not None and not kubeconfig.is_absolute():
        raise ValueError("kubeconfig path must be absolute")
    args = ["--control-namespace", control_namespace, f"--paused={str(paused).lower()}", "--generation", generation]
    if preserve_baseline:
        args.append("--preserve-baseline")
    if kubeconfig is not None:
        args.extend(("--kubeconfig", str(kubeconfig)))
    output = execute_operator("maintenance", args, timeout=wait_for(WaitKind.SLOW_COMMAND))
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
        if config.shared_accepted_id:
            assert_shared_repository(config)
        else:
            args.extend(("--repository", str(config.repository)))
    value = _run("incident-evidence", args, timeout=int(wait_for(WaitKind.SLOW_COMMAND)))
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
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=wait_for(WaitKind.SLOW_COMMAND), check=False
        )
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
    from benchmarks.sregym.protocol.native_receipt import native_production_receipt

    evidence = read_incident_evidence(config, incident_id, sync_repository=sync_repository)
    # Early closure intentionally retires responder Jobs. The frozen request,
    # lifecycle timestamps, result and acknowledged broker decision outlive them.
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
        controller_pod_uid=controller_uid,
        responder_job_evidence=[],
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
    value = _run("runtime-artifacts", args, timeout=int(wait_for(WaitKind.OPERATION)))
    expected = output_dir / "sdo_runtime"
    if value.get("directory") != str(expected):
        raise NativeOperatorError("Go runtime artifact export returned an unexpected directory")
    return expected


@dataclass(frozen=True)
class RuntimeEvidenceTarget:
    """Where one lane's sealed runtime evidence lives; contains no credential."""

    control_namespace: str
    repository_pvc: str
    controller_image: str
    kubeconfig: Path | None = None

    def __post_init__(self) -> None:
        if not self.control_namespace or not self.repository_pvc or not self.controller_image:
            raise ValueError("runtime evidence target requires namespace, PVC and controller image")
        if self.kubeconfig is not None and not self.kubeconfig.is_absolute():
            raise ValueError("runtime evidence kubeconfig must be absolute")

    @classmethod
    def of(cls, config: NativeInstallConfig) -> RuntimeEvidenceTarget:
        return cls(config.control_namespace, config.repository_pvc, config.controller_image, config.kubeconfig)

    def args(self) -> list[str]:
        args = [
            "--control-namespace",
            self.control_namespace,
            "--repository-pvc",
            self.repository_pvc,
            "--controller-image",
            self.controller_image,
        ]
        if self.kubeconfig is not None:
            args.extend(("--kubeconfig", str(self.kubeconfig)))
        return args


@dataclass(frozen=True)
class RuntimeSnapshot:
    """One immutable archive of the whitelisted runtime evidence sealed at an incident boundary."""

    snapshot_id: str
    sha256: str
    bytes: int
    created: bool
    # "controller" when sealed by exec in the running controller, "sync" via a sync Pod.
    sealed_by: str = ""

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.snapshot_id):
            raise ValueError("runtime snapshot ID is not a lowercase DNS label")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("runtime snapshot digest must be SHA-256 hex")
        if self.bytes < 0:
            raise ValueError("runtime snapshot size must be non-negative")


def snapshot_runtime_artifacts(target: RuntimeEvidenceTarget, snapshot_id: str) -> RuntimeSnapshot:
    """Seal the current evidence; a repeated ID returns the sealed archive, never a recapture."""

    args = [*target.args(), "--mode", "snapshot", "--snapshot-id", snapshot_id]
    value = _run("runtime-artifacts", args, timeout=int(wait_for(WaitKind.OPERATION)))
    try:
        snapshot = RuntimeSnapshot(
            snapshot_id=str(value["snapshot_id"]),
            sha256=str(value["sha256"]),
            bytes=int(value.get("bytes", 0)),
            created=bool(value.get("created", False)),
            sealed_by=str(value.get("sealed_by", "")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise NativeOperatorError(f"Go runtime snapshot returned an invalid report: {exc}") from exc
    if snapshot.snapshot_id != snapshot_id:
        raise NativeOperatorError("Go runtime snapshot returned a different snapshot")
    return snapshot


def deliver_runtime_snapshot(target: RuntimeEvidenceTarget, snapshot: RuntimeSnapshot, output_dir: Path) -> Path:
    """Copy exactly one sealed archive into ``output_dir/sdo_runtime`` after verifying its digest."""

    if not output_dir.is_absolute():
        raise ValueError("runtime artifact output directory must be absolute")
    args = [
        *target.args(),
        "--mode",
        "deliver",
        "--snapshot-id",
        snapshot.snapshot_id,
        "--sha256",
        snapshot.sha256,
        "--output-dir",
        str(output_dir),
    ]
    value = _run("runtime-artifacts", args, timeout=int(wait_for(WaitKind.OPERATION)))
    expected = output_dir / "sdo_runtime"
    if value.get("directory") != str(expected) or value.get("sha256") != snapshot.sha256:
        raise NativeOperatorError("Go runtime snapshot delivery returned an unexpected result")
    return expected


def _record_launch(config: NativeInstallConfig) -> None:
    from benchmarks.sregym.adapter.persistent import record_cleanup_target

    kubeconfig = str(config.kubeconfig) if config.kubeconfig else os.getenv("KUBECONFIG")
    record_cleanup_target(config.namespace, config.control_namespace, kubeconfig)


def install_existing(config: NativeInstallConfig) -> NativeInstallReceipt:
    if config.shared_accepted_id:
        raise NativeOperatorError(
            "shared accepted memory requires its prepared running controller; seeding is forbidden"
        )
    _record_launch(config)
    response_timeout = _launch_seconds(resolved_settings(config), "response", "timeout")
    args = config.common_args()
    if config.deployment_source is not None:
        args.extend(("--deployment-source", source_flag_value(config.deployment_source)))
    if config.duration_seconds is not None:
        args.extend(("--duration", f"{config.duration_seconds}s"))
    if config.exit_after_closure:
        args.append("--exit-after-closure")
    with launch_config(config) as (config_args, env):
        value = _run("install-existing", [*args, *config_args], timeout=int(response_timeout) + 600, env=env)
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
    value = _run("inspect-existing", args, timeout=int(wait_for(WaitKind.SLOW_COMMAND)))
    for name in ("job_uid", "pod_uid", "fingerprint"):
        if not isinstance(value.get(name), str) or not value[name].strip():
            raise NativeOperatorError(f"Go inspect-existing lacks {name}")
    commit = value.get("accepted_commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise NativeOperatorError("Go inspect-existing lacks a full accepted commit")
    return NativeExistingReceipt(value["job_uid"], value["pod_uid"], value["fingerprint"], commit)


def launch_host(config: NativeHostConfig) -> dict[str, Any]:
    _record_launch(config.install)
    install = config.install
    if config.max_attempts is not None:
        install = replace(
            install,
            settings=operator_config.merge(install.settings, {"deployment": {"max_attempts": config.max_attempts}}),
        )
    resolved = resolved_settings(install)
    attempts = int(resolved["deployment"]["max_attempts"])
    per_attempt = _launch_seconds(resolved, "response", "timeout") + _launch_seconds(
        resolved, "agent", "lifecycle_timeout"
    )
    args = config.install.common_args()
    if config.install.duration_seconds is not None:
        args.extend(("--controller-duration", f"{config.install.duration_seconds}s"))
    if config.install.exit_after_closure:
        args.append("--exit-after-closure")
    args.extend(("--health-objective", config.health_objective, "--operation-id", config.operation_id))
    args.extend(("--buildkit-image", config.buildkit_image, "--registry-push-secret", config.registry_push_secret))
    args.extend(("--registry-host", config.registry_host))
    if config.registry_insecure:
        args.append("--registry-insecure")
    if config.agent_registrations is not None:
        args.extend(("--agent-registrations", str(config.agent_registrations)))
    if config.deployment_plan is not None:
        args.extend(("--deployment-plan", plan_flag_value(config.deployment_plan)))
    with launch_config(install) as (config_args, env):
        value = _run("host", [*args, *config_args], timeout=int(attempts * per_attempt), env=env)
    if value.get("installed") is not True or value.get("operation_id") != config.operation_id:
        raise NativeOperatorError("Go host returned an invalid operation receipt")
    fields = ["schema_commit", "source_commit", "architecture_commit", "judge_commit"]
    if config.health_objective.strip() or value.get("goal_commit"):
        fields.append("goal_commit")
    for name in fields:
        if not isinstance(value.get(name), str) or not re.fullmatch(r"[0-9a-f]{40}", value[name]):
            raise NativeOperatorError(f"Go host receipt lacks {name}")
    return value


def stop_runtime(
    control_namespace: str, *, kubeconfig: str | None = None, inspect: bool = False
) -> dict[str, list[str]]:
    """Delegate production runtime ownership to the Go operator, including deletion verification."""
    args = ["--control-namespace", control_namespace, "--timeout", "60s"]
    if kubeconfig:
        args.extend(("--kubeconfig", kubeconfig))
    if inspect:
        args.append("--inspect")
    result = json.loads(execute_operator("stop", args, timeout=wait_for(WaitKind.SLOW_COMMAND)))
    if not isinstance(result, dict) or any(
        not isinstance(result.get(key), list) or any(not isinstance(item, str) for item in result[key])
        for key in ("removed", "leftovers")
    ):
        raise NativeOperatorError("Go stop returned an invalid runtime inventory")
    return result


def reset_control_state(control_namespace: str, *, kubeconfig: str | None = None) -> dict[str, Any]:
    """Explicitly retire SDO onboarding authority before a fresh-memory install.

    The caller must stop the previous runtime before invoking this command.
    """
    if not control_namespace:
        raise ValueError("reset control state requires an explicit control namespace")
    args = ["--control-namespace", control_namespace, "--timeout", "60s"]
    if kubeconfig:
        args.extend(("--kubeconfig", kubeconfig))
    result = json.loads(execute_operator("reset-control-state", args, timeout=wait_for(WaitKind.SLOW_COMMAND)))
    if (
        not isinstance(result, dict)
        or result.get("control_namespace") != control_namespace
        or not isinstance(result.get("deleted"), list)
        or any(not isinstance(name, str) for name in result["deleted"])
    ):
        raise NativeOperatorError("Go reset-control-state returned an invalid audit receipt")
    return result
