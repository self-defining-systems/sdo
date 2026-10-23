"""Launch SREGym incidents through the production long-running SDO controller."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from benchmarks.sregym.adapter.cluster_resources import (
    BenchmarkClusterResourcesConfig,
    kubectl,
    prepare_benchmark_resources,
    wait_submission_bridge,
)
from benchmarks.sregym.adapter.fault_gate import (
    AWAITING_FAULT_INJECTION,
    FaultGateError,
    inject_fault_after_controller_baseline,
    request_fault_injection,
)
from benchmarks.sregym.adapter.healthy_baseline import capture_snapshots, kubectl_capture_ops, publish_snapshots
from benchmarks.sregym.adapter.native_operator import (
    NativeHostConfig,
    NativeInstallConfig,
    NativeOperatorError,
    collect_native_receipt,
    export_runtime_artifacts,
    inspect_existing,
    install_existing,
    launch_host,
    sync_existing,
)
from benchmarks.sregym.adapter.persistent import (
    PERSISTENT_STATE_ENV,
    ClusterOps,
    KubectlClusterOps,
    StageInputs,
    control_namespace_for,
    persist_resolution,
    run_persistent_stage,
)
from benchmarks.sregym.adapter.runtime import RuntimeConfig, _responder_instructions, native_install_config
from benchmarks.sregym.adapter.submission import submit_solution
from benchmarks.sregym.protocol.conductor import (
    get_api_base,
    get_app_info,
    get_current_stage_sync,
    poll_stage_sync,
    signal_cleanup,
)
from benchmarks.sregym.protocol.injection import InvalidInjectionError
from benchmarks.sregym.protocol.schema import READY_STAGES, TERMINAL_STAGES

logger = logging.getLogger(__name__)
DEFAULT_REFLECTION_SESSION = "resume"

DEFAULT_BUILDKIT_IMAGE = "moby/buildkit@sha256:ef31818fffd0b9133d710742e06b492553f7ebc54f5fd66cd21a1a9bf2e51f76"

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _cleanup_defer_timeout_seconds() -> float:
    raw = os.getenv("SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS", "600").strip()
    timeout = float(raw)
    if timeout <= 0:
        raise ValueError("SREGYM_CLEANUP_DEFER_TIMEOUT_SECONDS must be positive")
    return timeout


def persist_lifecycle_seed(repository: Path) -> Path | None:
    """Checkpoint validated lifecycle memory outside a resettable stage directory.

    The stage directory is located from *repository*'s own ancestors, not
    from SREGym's ``AGENT_LOGS_DIR``: our pipeline always places the
    application workspace at ``<stage_dir>/application_workspace``
    (``application_workspace_dir`` in the SREGym harness), but SREGym's own
    per-run logs directory is an ephemeral staging path
    (``<sregym_dir>/.runtime/<agent>/<artifact_id>``) that is never nested
    under the stage directory while the agent runs — it is only renamed
    there, by SREGym, after the driver process exits. Keying the search off
    ``logs_dir`` therefore never found the stage directory in a real
    benchmark run, and the checkpoint silently never got written.
    """

    resolved_repository = repository.resolve()
    stage_dir = next(
        (parent for parent in resolved_repository.parents if re.fullmatch(r"stage_(\d+)_.+", parent.name)),
        None,
    )
    if stage_dir is None:
        logger.warning(
            "lifecycle seed checkpoint skipped: %s has no stage_<n>_<name> ancestor directory", resolved_repository
        )
        return None
    stage_index = re.fullmatch(r"stage_(\d+)_.+", stage_dir.name)
    if stage_index is None:
        return None
    target = stage_dir.parent / f"lifecycle_seed_stage{stage_index.group(1)}"
    temporary = target.with_name(target.name + ".tmp")
    if temporary.exists():
        shutil.rmtree(temporary)
    shutil.copytree(repository, temporary, symlinks=True)
    if target.exists():
        shutil.rmtree(target)
    temporary.replace(target)
    return target


def persist_production_receipt(receipt: dict[str, Any], receipt_dir: Path) -> Path:
    """Persist one strict receipt atomically beside the SREGym run artifacts."""
    receipt_dir.mkdir(parents=True, exist_ok=True)
    path = receipt_dir / "sdo_production_receipt_strict.json"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def _remove_sdo_jobs_before_benchmark_grading(
    receipt: dict[str, Any],
    namespace: str,
    *,
    command_runner: CommandRunner = subprocess.run,
) -> None:
    """Keep completed SDO infrastructure out of application-pod grading."""

    if receipt.get("schema_version") == "sdo.production-receipt/v2":
        namespace = str(receipt.get("control_namespace") or namespace)
        evidence = receipt.get("responder_job_evidence")
        responder_jobs = (
            [
                item.get("job_name")
                for item in evidence
                if isinstance(item, dict) and isinstance(item.get("job_name"), str)
            ]
            if isinstance(evidence, list)
            else []
        )
    else:
        responder_jobs = receipt.get("responder_jobs", [])
    names = ["sdo-controller-run"]
    if isinstance(responder_jobs, list):
        names.extend(name for name in responder_jobs if isinstance(name, str) and name.startswith("sdo-"))
    completed = command_runner(
        [
            "kubectl",
            "--namespace",
            namespace,
            "delete",
            *(f"job/{name}" for name in dict.fromkeys(names)),
            "--ignore-not-found=true",
            "--wait=true",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "kubectl delete failed"
        raise RuntimeError(f"remove SDO jobs before benchmark grading failed: {details}")


def _submit_recorded_result(
    receipt: dict[str, Any],
    api_base: str,
    *,
    current_stage: Callable[[str], str | None] = get_current_stage_sync,
    submitter: Callable[..., dict[str, Any]] = submit_solution,
) -> None:
    """Publish the responder-authored result when it omitted benchmark transport."""

    stage = current_stage(api_base)
    if stage in TERMINAL_STAGES:
        return
    if stage not in READY_STAGES:
        raise RuntimeError(f"cannot publish SDO result while SREGym is at stage {stage!r}")

    result: dict[str, Any] = receipt
    if receipt.get("schema_version") == "sdo.production-receipt/v2":
        ledger = receipt.get("native_ledger")
        closure = ledger.get("closure") if isinstance(ledger, dict) else None
        candidate = closure.get("result") if isinstance(closure, dict) else None
        if not isinstance(candidate, dict):
            raise RuntimeError("native receipt has no responder result")
        result = candidate
    root_causes = result.get("confirmed_root_causes", [])
    diagnosis = "; ".join(
        str(item.get("summary", "")).strip()
        for item in root_causes
        if isinstance(item, dict) and str(item.get("summary", "")).strip()
    )
    repair_actions = result.get("repair_actions", [])
    mitigation = "; ".join(
        str(item.get("summary", "")).strip()
        for item in repair_actions
        if isinstance(item, dict) and str(item.get("summary", "")).strip()
    )
    if stage == "diagnosis":
        if not diagnosis:
            raise RuntimeError("SDO result has no confirmed root cause to submit")
        submitter(diagnosis, phase="diagnosis", api_base=api_base)
    if not mitigation:
        raise RuntimeError("SDO result has no successful repair action to submit")
    submitter(mitigation, phase="mitigation", api_base=api_base)


def _receipt_directory(logs_dir: str | None, repository: Path) -> Path:
    """Resolve durable receipt storage for both CLI and registry-launched agents."""

    return Path(logs_dir) if logs_dir else repository.resolve().parent


def _configuration() -> dict[str, Any]:
    raw = os.getenv("SREGYM_EXPERIMENT_AGENT_CONFIG", "").strip()
    if not raw:
        return {}
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise ValueError("SREGYM_EXPERIMENT_AGENT_CONFIG must contain a JSON object")
    return decoded


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    config = _configuration()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("agent-cli",), default=config.get("backend", "agent-cli"))
    parser.add_argument("--provider", choices=("codex", "claude"), default=config.get("provider", "codex"))
    parser.add_argument("--model", default=config.get("model", os.getenv("MODEL_ID", "gpt-6-luna")))
    parser.add_argument("--timeout-sec", type=int, default=int(config.get("timeout_sec", 1800)))
    raw_detection = config.get("detection_timeout_sec")
    parser.add_argument(
        "--detection-timeout-sec",
        type=int,
        default=int(raw_detection) if raw_detection is not None else None,
        help="persistent mode: end a stage as a detection miss if no incident opens this long after injection",
    )
    parser.add_argument("--controller-image", default=config.get("controller_image", "sdo-controller:v0.1.0"))
    parser.add_argument(
        "--responder-image",
        default=config.get("responder_image", "sdo-sregym-responder:v0.1.0"),
    )
    parser.add_argument(
        "--validator-image",
        default=config.get("validator_image", "sdo-detector-validator:v0.1.0"),
    )
    parser.add_argument("--repository-pvc", default=config.get("repository_pvc", "sdo-application-repository"))
    parser.add_argument("--credentials-secret", default=config.get("credentials_secret", "sdo-codex-credentials"))
    parser.add_argument("--registry-push-secret", default=config.get("registry_push_secret", "sdo-registry-push"))
    parser.add_argument("--buildkit-image", default=config.get("buildkit_image", DEFAULT_BUILDKIT_IMAGE))
    parser.add_argument("--kind-cluster-name", default=config.get("kind_cluster_name"))
    parser.add_argument("--max-deployment-attempts", type=int, default=int(config.get("max_deployment_attempts", 3)))
    parser.add_argument(
        "--deployed-manifest-path",
        dest="deployed_manifest_paths",
        action="append",
        default=None,
        help="explicit deployed manifest file/directory; repeat to select multiple paths (replaces configured list)",
    )
    parser.add_argument(
        "--program-fast-path",
        action=argparse.BooleanOptionalAction,
        default=config.get("program_fast_path", False),
        help="enable trusted playbook programs to resolve incidents without a responder",
    )
    parser.add_argument(
        "--program-trust-threshold",
        type=int,
        default=config.get("program_trust_threshold", 3),
        help="successful verified repairs required before a program is trusted (positive integer)",
    )
    parser.add_argument("--agent-registrations", default=config.get("agent_registrations"))
    parser.add_argument(
        "--reflection-session",
        choices=("resume", "fresh"),
        default=config.get("reflection_session", DEFAULT_REFLECTION_SESSION),
    )
    parser.add_argument(
        "--reflection-guidance",
        choices=("baseline", "generalize", "generalize-spec"),
        default=config.get("reflection_guidance", "baseline"),
        help="reflection guidance: baseline, generalize, or generalize-spec (generalize with spec-first predicates)",
    )
    parser.add_argument(
        "--late-findings",
        choices=("off", "pull"),
        default=config.get("late_findings", "off"),
        help="let the responder pull findings that activated after dispatch (off by default)",
    )
    parser.add_argument("--max-response-loops", type=int, default=int(config.get("max_response_loops", 5)))
    parser.add_argument(
        "--healthy-baseline",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("healthy_baseline", False)),
        help="record the healthy namespace before fault injection; reject learned detectors that fire on it",
    )
    parser.add_argument(
        "--persistent-controller",
        action=argparse.BooleanOptionalAction,
        default=bool(config.get("persistent_controller", False)),
        help="keep one controller per application running across benchmark problems",
    )
    parser.add_argument(
        "--logs-dir",
        default=config.get("logs_dir", os.getenv("AGENT_LOGS_DIR")),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--summary-dir", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--summary-model", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--enable-summary", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--no-inject-summary", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    paths = args.deployed_manifest_paths
    if paths is None:
        paths = config.get("deployed_manifest_paths", [])
    if not isinstance(paths, list) or any(not isinstance(path, str) or not path.strip() for path in paths):
        raise ValueError("deployed_manifest_paths must be a list of non-empty strings")
    args.deployed_manifest_paths = tuple(paths)
    if type(args.program_fast_path) is not bool:
        raise ValueError("program_fast_path must be a boolean")
    if type(args.program_trust_threshold) is not int or args.program_trust_threshold <= 0:
        raise ValueError("program_trust_threshold must be a positive integer")
    return args


def _application_repository() -> Path:
    raw = os.getenv("SREGYM_AGENT_WORKDIR", "").strip()
    if not raw:
        raise RuntimeError("SREGYM_AGENT_WORKDIR is required for the SDO production adapter")
    repository = Path(raw).resolve()
    if not repository.is_dir():
        raise RuntimeError(f"SREGYM_AGENT_WORKDIR is not a directory: {repository}")
    return repository


def _application_label(app_info: dict[str, Any], repository: Path) -> str:
    """The SREGym application's display name as the Kubernetes DNS label the Go operator requires."""

    raw = str(app_info.get("app_name") or repository.name)
    label = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")[:63].strip("-")
    if not label:
        raise ValueError(f"application name {raw!r} cannot be converted to a Kubernetes DNS label")
    return label


def _in_cluster_api_base(api_base: str) -> str:
    parsed = urlsplit(api_base)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return api_base.rstrip("/")
    return urlunsplit(("http", "sdo-sregym-bridge:8000", parsed.path.rstrip("/"), "", ""))


def _relay_target_api_base(api_base: str) -> str | None:
    parsed = urlsplit(api_base)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return None
    return api_base.rstrip("/")


@dataclass(frozen=True)
class ActiveTopologyResourceDTO:
    kind: str
    name: str


@dataclass(frozen=True)
class DeployedLifecycleContext:
    health_objective: str
    active_resources: list[ActiveTopologyResourceDTO]


def _deployed_lifecycle_context(
    namespace: str,
    *,
    command_runner: CommandRunner = subprocess.run,
) -> DeployedLifecycleContext:
    """Bind the benchmark objective to the source variant deployed by SREGym."""

    command = [
        "kubectl",
        "--namespace",
        namespace,
        "get",
        "deployments,services,configmaps,networkpolicies",
        "--output=json",
    ]
    completed = command_runner(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"cannot inventory deployed SREGym resources: {details}")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("kubectl returned an invalid deployed-resource inventory") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise RuntimeError("kubectl deployed-resource inventory has no items list")

    def names(kind: str) -> list[str]:
        return sorted(
            {
                name
                for item in items
                if isinstance(item, dict) and item.get("kind") == kind
                for metadata in [item.get("metadata")]
                if isinstance(metadata, dict)
                for name in [metadata.get("name")]
                if isinstance(name, str) and name
            }
        )

    deployments = names("Deployment")
    all_services = names("Service")
    # ExternalName Services are DNS aliases: Kubernetes never gives them endpoints.
    external_name_services = sorted(
        {
            str(item["metadata"]["name"])
            for item in items
            if isinstance(item, dict)
            and item.get("kind") == "Service"
            and isinstance(item.get("metadata"), dict)
            and item["metadata"].get("name")
            and isinstance(item.get("spec"), dict)
            and item["spec"].get("type") == "ExternalName"
        }
    )
    services = [name for name in all_services if name not in external_name_services]
    if not deployments:
        raise RuntimeError(f"no deployed Deployments found in SREGym namespace {namespace!r}")
    service_clause = (
        f"the deployed Services named {', '.join(services)} expose ready endpoints"
        if services
        else "the deployed workload remains reachable through its declared interfaces"
    )
    if external_name_services:
        service_clause += (
            f"; the ExternalName Services named {', '.join(external_name_services)} are DNS aliases with no "
            "endpoints and must not be required to have ready endpoints"
        )
    health_objective = (
        f"The deployed Deployments named {', '.join(deployments)} remain available; {service_clause}; "
        "and representative requests succeed."
    )
    active_keys = {
        (kind, name) for kind in ("ConfigMap", "Deployment", "NetworkPolicy", "Service") for name in names(kind)
    }
    for item in items:
        if not isinstance(item, dict) or item.get("kind") != "Deployment":
            continue
        spec = item.get("spec")
        template = spec.get("template") if isinstance(spec, dict) else None
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        volumes = pod_spec.get("volumes", []) if isinstance(pod_spec, dict) else []
        for volume in volumes if isinstance(volumes, list) else []:
            config_map = volume.get("configMap") if isinstance(volume, dict) else None
            if not isinstance(config_map, dict):
                continue
            name = config_map.get("name")
            if isinstance(name, str) and name and config_map.get("optional") is not True:
                active_keys.add(("ConfigMap", name))
    return DeployedLifecycleContext(
        health_objective=health_objective,
        active_resources=[ActiveTopologyResourceDTO(kind=kind, name=name) for kind, name in sorted(active_keys)],
    )


def _deployed_health_objective(
    namespace: str,
    *,
    command_runner: CommandRunner = subprocess.run,
) -> str:
    """Return the human-readable portion of the deployed lifecycle context."""

    return _deployed_lifecycle_context(namespace, command_runner=command_runner).health_objective


# The agent CLI layer appends one accounting record per turn to this file.
TURN_USAGE_LOG_ENV = "SDO_TURN_USAGE_LOG"


def _configure_turn_usage_log(logs_dir: str | None) -> None:
    """Account host-side agent turns (lifecycle) beside the run's artifacts."""

    if logs_dir and not os.environ.get(TURN_USAGE_LOG_ENV, "").strip():
        os.environ[TURN_USAGE_LOG_ENV] = str(Path(logs_dir) / "sdo_turn_usage.jsonl")


def _wait_for_installed_controller(install: NativeInstallConfig, *, timeout_seconds: float = 300.0) -> None:
    """An unbounded persistent install returns at creation; wait for its ready Pod."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            inspect_existing(install)
            return
        except NativeOperatorError as exc:
            if time.monotonic() >= deadline:
                raise
            logger.info("waiting for the seeded controller: %s", exc)
            time.sleep(3)


def _has_accepted_lifecycle(repository: Path) -> bool:
    """Whether *repository* already holds an accepted SDO lifecycle (a seeded workspace)."""

    sdo = repository / ".sdo"
    if not (sdo / "lifecycle-provenance.yaml").is_file():
        return False
    diagnostics = sdo / "diagnostics"
    return (diagnostics / "manifest.yaml").is_file() or any(
        manifest.is_file() for manifest in (diagnostics / "detectors" / "health").glob("*/manifest.yaml")
    )


def _publish_agent_registrations(
    directory: Path, control_namespace: str, kubeconfig: Path, responder_image: str, loaded_images: set[str]
) -> None:
    files = sorted(directory.glob("*.json"))
    if not files:
        raise ValueError(f"agent registration directory {directory} has no JSON files")
    data: dict[str, str] = {}
    for path in files:
        content = path.read_text(encoding="utf-8")
        registration = json.loads(content)
        if not isinstance(registration, dict):
            raise ValueError(f"agent registration {path} must be a JSON object")
        job = registration.get("kubernetes_job")
        if isinstance(job, dict):
            if job.get("image") == "${RESPONDER_IMAGE}":
                job["image"] = responder_image
            if job.get("image") not in loaded_images:
                raise ValueError(f"agent registration {path} uses image {job.get('image')!r} not loaded into kind")
            content = json.dumps(registration)
        data[path.name] = content
    kubectl(
        ["apply", "-f", "-"],
        namespace=control_namespace,
        kubeconfig=kubeconfig,
        input_text=json.dumps(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "sdo-agent-registrations", "namespace": control_namespace},
                "data": data,
            }
        ),
    )


# Shared with the toy fastloop through the adapter facade.
publish_agent_registrations = _publish_agent_registrations


def _launch_native_lifecycle(
    args: argparse.Namespace,
    *,
    repository: Path,
    namespace: str,
    application: str,
    health_objective: str,
    api_base: str,
    persistent: bool,
    install_config: NativeInstallConfig | None = None,
) -> dict[str, Any]:
    """Provision benchmark transport, then run the production Go host lifecycle."""

    if args.agent_registrations and not _has_accepted_lifecycle(repository):
        raise ValueError("worker registration override requires an accepted lifecycle seed")
    kind_cluster_name = (args.kind_cluster_name or os.getenv("SREGYM_KIND_CLUSTER_NAME", "")).strip()
    if not kind_cluster_name:
        raise RuntimeError("native benchmark operation requires SREGYM_KIND_CLUSTER_NAME or --kind-cluster-name")
    raw_kubeconfig = os.getenv("SREGYM_BASE_KUBECONFIG", "").strip() or os.getenv("KUBECONFIG", "").strip()
    if not raw_kubeconfig or not Path(raw_kubeconfig).is_absolute():
        raise RuntimeError("native benchmark operation requires an absolute trusted kubeconfig")
    kubeconfig = Path(raw_kubeconfig)
    if namespace == "astronomy-shop":
        from benchmarks.sregym.adapter.oracle_observability import prepare_oracle_observability, route_astronomy_traces

        prepare_oracle_observability(
            kubeconfig=kubeconfig,
            sregym_dir=Path(__file__).resolve().parents[3] / "third_party/sregym",
            cluster=kind_cluster_name,
        )
        route_astronomy_traces(kubeconfig)
    control_namespace = control_namespace_for(namespace)
    cluster = BenchmarkClusterResourcesConfig(
        namespace=namespace,
        control_namespace=control_namespace,
        responder_image=install_config.responder_image if install_config else args.responder_image,
        credentials_secret=install_config.credentials_secret if install_config else args.credentials_secret,
        registry_push_secret=args.registry_push_secret,
        registry_insecure=True,
        kind_cluster_name=kind_cluster_name,
    )
    from benchmarks.sregym.adapter.persistent import record_cleanup_target

    record_cleanup_target(namespace, control_namespace, str(kubeconfig))
    relay_target = _relay_target_api_base(api_base)
    prepare_benchmark_resources(cluster, relay_target_base=relay_target, kubeconfig=kubeconfig)
    if relay_target is not None:
        wait_submission_bridge(control_namespace, kubeconfig)
    install = install_config or NativeInstallConfig(
        repository=repository.resolve(),
        namespace=namespace,
        control_namespace=control_namespace,
        application=application,
        controller_image=args.controller_image,
        responder_image=args.responder_image,
        validator_image=args.validator_image,
        repository_pvc=args.repository_pvc,
        credentials_secret=args.credentials_secret,
        model=args.model,
        kubeconfig=kubeconfig,
        agent_provider=args.provider,
        repair_policy=getattr(args, "repair_policy", "recorded-actions"),
        late_findings=args.late_findings,
        reflection_session=args.reflection_session,
        reflection_guidance=args.reflection_guidance,
        deployed_manifest_paths=args.deployed_manifest_paths,
        program_fast_path=args.program_fast_path,
        program_trust_threshold=args.program_trust_threshold,
        healthy_baseline=args.healthy_baseline,
        max_response_loops=args.max_response_loops,
        response_timeout_seconds=args.timeout_sec,
        verification_timeout_seconds=120,
        duration_seconds=None if persistent else args.timeout_sec,
        exit_after_closure=not persistent,
        registration_configmap="sdo-agent-registrations" if args.agent_registrations else None,
        responder_env=(
            f"SDO_SREGYM_API_BASE={_in_cluster_api_base(api_base)}",
            f"SDO_RESPONDER_EXTRA_INSTRUCTIONS={_responder_instructions()}",
        )
        if api_base
        else (),
    )
    if install_config is not None:
        install = replace(
            install,
            control_namespace=control_namespace,
            duration_seconds=None if persistent else install.response_timeout_seconds,
            exit_after_closure=not persistent,
            registration_configmap="sdo-agent-registrations" if args.agent_registrations else None,
            responder_env=(
                f"SDO_SREGYM_API_BASE={_in_cluster_api_base(api_base)}",
                f"SDO_RESPONDER_EXTRA_INSTRUCTIONS={_responder_instructions()}",
            )
            if api_base
            else (),
        )
    if _has_accepted_lifecycle(repository):
        # A seeded workspace already carries an independently accepted lifecycle (goal, architecture, health
        # detectors): install the controller over it instead of running the deployer and health judge again.
        if args.agent_registrations:
            _publish_agent_registrations(
                Path(args.agent_registrations),
                control_namespace,
                kubeconfig,
                install.responder_image,
                {install.controller_image, install.responder_image, install.validator_image},
            )
        receipt = install_existing(install)
        # Bounded nonpersistent installs already wait for completion, so their Job is no longer active.
        if persistent:
            _wait_for_installed_controller(install)
        return {"installed": True, "seeded": True, "reused": receipt.reused, "accepted_commit": receipt.accepted_commit}
    host = NativeHostConfig(
        install=install,
        health_objective=health_objective,
        operation_id=f"sregym-{uuid.uuid4().hex}",
        buildkit_image=args.buildkit_image,
        registry_push_secret=args.registry_push_secret,
        registry_host=cluster.registry_target.host,
        registry_insecure=cluster.registry_target.insecure,
        max_attempts=args.max_deployment_attempts,
        agent_registrations=Path(args.agent_registrations).resolve() if args.agent_registrations else None,
    )
    return launch_host(host)


def launch_native_lifecycle(config: RuntimeConfig, health_objective: str, api_base: str) -> dict[str, Any]:
    """Run fresh benchmark deployment and controller installation through Go."""

    args = argparse.Namespace(
        kind_cluster_name=config.kind_cluster_name,
        registry_push_secret=config.registry_push_secret,
        buildkit_image=config.buildkit_image,
        max_deployment_attempts=config.max_deployment_attempts,
        agent_registrations=config.agent_registrations,
    )
    return _launch_native_lifecycle(
        args,
        repository=config.repository,
        namespace=config.namespace,
        application=config.application,
        health_objective=health_objective,
        api_base=api_base,
        persistent=config.persistent,
        install_config=native_install_config(config),
    )


def _native_install_for_run(
    args: argparse.Namespace, repository: Path, namespace: str, application: str, api_base: str
) -> NativeInstallConfig:
    raw_kubeconfig = os.getenv("SREGYM_BASE_KUBECONFIG", "").strip() or os.getenv("KUBECONFIG", "").strip()
    if not raw_kubeconfig or not Path(raw_kubeconfig).is_absolute():
        raise RuntimeError("native benchmark evidence requires an absolute trusted kubeconfig")
    return NativeInstallConfig(
        repository=repository.resolve(),
        namespace=namespace,
        control_namespace=control_namespace_for(namespace),
        application=application,
        controller_image=args.controller_image,
        responder_image=args.responder_image,
        validator_image=args.validator_image,
        repository_pvc=args.repository_pvc,
        credentials_secret=args.credentials_secret,
        model=args.model,
        kubeconfig=Path(raw_kubeconfig),
        agent_provider=args.provider,
        repair_policy="recorded-actions",
        late_findings=args.late_findings,
        reflection_session=args.reflection_session,
        reflection_guidance=args.reflection_guidance,
        deployed_manifest_paths=args.deployed_manifest_paths,
        program_fast_path=args.program_fast_path,
        program_trust_threshold=args.program_trust_threshold,
        max_response_loops=args.max_response_loops,
        response_timeout_seconds=args.timeout_sec,
        verification_timeout_seconds=120,
        duration_seconds=args.timeout_sec,
        exit_after_closure=True,
        responder_env=(
            f"SDO_SREGYM_API_BASE={_in_cluster_api_base(api_base)}",
            f"SDO_RESPONDER_EXTRA_INSTRUCTIONS={_responder_instructions()}",
        ),
    )


def _acknowledged_incident_id(control_namespace: str, kubeconfig: Path) -> str:
    completed = kubectl(
        ["get", "configmap/sdo-controller-state", "-o", "json"],
        namespace=control_namespace,
        kubeconfig=kubeconfig,
    )
    try:
        document = json.loads(completed.stdout)
        state = json.loads(document["data"]["runtime-state.json"])
        incident_id = state["last_acknowledged_incident_id"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Go controller has no durable acknowledged incident") from exc
    if not isinstance(incident_id, str) or not incident_id.strip():
        raise RuntimeError("Go controller has no durable acknowledged incident")
    return incident_id


def _cluster_ops(args: argparse.Namespace, namespace: str) -> ClusterOps:
    ops: ClusterOps = KubectlClusterOps()
    if args.healthy_baseline:
        ops = kubectl_capture_ops(ops, namespace=namespace, kubectl_runner=kubectl)  # type: ignore[assignment]
    return ops


def _record_healthy_baseline(namespace: str, control_namespace: str) -> Callable[[], None]:
    """Capture the application before injection and publish to the operator namespace."""

    def record() -> None:
        publish_snapshots(
            capture_snapshots(namespace, kubectl_runner=kubectl),
            control_namespace=control_namespace,
            kubectl_runner=kubectl,
        )

    return record


class _FaultGate:
    """Inject the deferred benchmark fault once the installed controller is watching."""

    def __init__(self, namespace: str, api_base: str, *, before_inject: Callable[[], None] | None = None) -> None:
        self._namespace = namespace
        self._api_base = api_base
        self._before_inject = before_inject
        self._error: BaseException | None = None
        self.timings: dict[str, float] = {}
        self._thread = threading.Thread(target=self._run, name="sdo-fault-gate", daemon=True)
        self._not_before = datetime.now(timezone.utc)

    def start(self) -> None:
        self._not_before = datetime.now(timezone.utc)
        self._thread.start()

    def _inject(self) -> None:
        if self._before_inject is not None:
            self._before_inject()
        from benchmarks.sregym.adapter.persistent import KubectlClusterOps, inject_while_paused

        inject_while_paused(
            KubectlClusterOps(),
            self._namespace,
            f"deferred-{uuid.uuid4().hex[:8]}",
            lambda: request_fault_injection(self._api_base),
        )

    def _run(self) -> None:
        try:
            self.timings = inject_fault_after_controller_baseline(
                self._namespace,
                inject=self._inject,
                kubectl_runner=kubectl,
                not_before=self._not_before,
            )
            logger.info("benchmark fault injected after controller baseline: %s", self.timings)
        except BaseException as exc:  # surfaced to the driver by join()
            logger.exception("benchmark fault gate failed")
            self._error = exc

    def join(self) -> None:
        self._thread.join(timeout=5)
        if isinstance(self._error, InvalidInjectionError):
            raise self._error
        if self._error is not None:
            raise FaultGateError(f"deferred fault was not injected: {self._error}") from self._error
        if not self.timings:
            raise FaultGateError("controller completed before the deferred fault was injected")


def _lifecycle_fingerprint(context: DeployedLifecycleContext) -> str:
    """Identify the validated lifecycle inputs; a redeploy of the same variant keeps it."""

    payload = {
        "health_objective": context.health_objective,
        "active_resources": sorted((resource.kind, resource.name) for resource in context.active_resources),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class DeployedLifecycle:
    """The deployed lifecycle context and the fingerprint a persistent controller is reused under."""

    context: DeployedLifecycleContext
    fingerprint: str


def deployed_lifecycle(namespace: str, *, command_runner: CommandRunner = subprocess.run) -> DeployedLifecycle:
    context = _deployed_lifecycle_context(namespace, command_runner=command_runner)
    return DeployedLifecycle(context=context, fingerprint=_lifecycle_fingerprint(context))


@contextlib.contextmanager
def _environment(name: str, value: str | None) -> Iterator[None]:
    previous = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _stage_label(logs_dir: str | None) -> str:
    if logs_dir:
        stage_dir = next(
            (parent for parent in Path(logs_dir).resolve().parents if re.fullmatch(r"stage_\d+_.+", parent.name)),
            None,
        )
        if stage_dir is not None:
            return stage_dir.name
    return datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S")


def _run_persistent(args: argparse.Namespace, api_base: str, started: float) -> dict[str, Any]:
    """Serve one problem with the application's long-running controller."""

    initial_stage = poll_stage_sync(
        api_base,
        wait_for=READY_STAGES | {AWAITING_FAULT_INJECTION},
        timeout=300,
        on_timeout="raise",
    )
    if initial_stage != AWAITING_FAULT_INJECTION:
        raise RuntimeError("persistent controller mode requires deferred fault injection")
    conductor_ready = time.monotonic()
    raw_state_path = os.getenv(PERSISTENT_STATE_ENV, "").strip()
    if not raw_state_path:
        raise RuntimeError(f"persistent controller mode requires {PERSISTENT_STATE_ENV} from the runner")
    app_info = get_app_info(api_base)
    repository = _application_repository()
    _configure_turn_usage_log(args.logs_dir)
    application = _application_label(app_info, repository)
    namespace = str(app_info.get("namespace") or "default")
    trusted_kubeconfig = os.getenv("SREGYM_BASE_KUBECONFIG", "").strip() or None
    if trusted_kubeconfig is not None:
        if not Path(trusted_kubeconfig).is_absolute():
            raise RuntimeError("SREGYM_BASE_KUBECONFIG must be absolute")
        os.environ["KUBECONFIG"] = trusted_kubeconfig
    lifecycle_context = _deployed_lifecycle_context(namespace)
    receipt_dir = _receipt_directory(args.logs_dir, repository)

    def lifecycle() -> bool:
        launch_native_lifecycle(config, lifecycle_context.health_objective, api_base)
        return False

    config = RuntimeConfig(
        repository=repository,
        namespace=namespace,
        application=application,
        controller_image=args.controller_image,
        responder_image=args.responder_image,
        validator_image=args.validator_image,
        repository_pvc=args.repository_pvc,
        credentials_secret=args.credentials_secret,
        model=args.model,
        timeout_seconds=args.timeout_sec,
        repair_policy="recorded-actions",
        agent_provider=args.provider,
        reflection_session=args.reflection_session,
        reflection_guidance=args.reflection_guidance,
        deployed_manifest_paths=args.deployed_manifest_paths,
        program_fast_path=args.program_fast_path,
        program_trust_threshold=args.program_trust_threshold,
        late_findings=args.late_findings,
        healthy_baseline=args.healthy_baseline,
        max_response_loops=args.max_response_loops,
        submission_api_base=_in_cluster_api_base(api_base),
        submission_relay_target_base=_relay_target_api_base(api_base),
        artifacts_dir=receipt_dir,
        controller_namespace=control_namespace_for(namespace),
        persistent=True,
        wait_for_completion=False,
        buildkit_image=args.buildkit_image,
        registry_push_secret=args.registry_push_secret,
        kind_cluster_name=args.kind_cluster_name,
        max_deployment_attempts=args.max_deployment_attempts,
        agent_registrations=Path(args.agent_registrations).resolve() if args.agent_registrations else None,
    )
    resolution = run_persistent_stage(
        StageInputs(
            stage_label=_stage_label(args.logs_dir),
            application=application,
            namespace=namespace,
            lifecycle_fingerprint=_lifecycle_fingerprint(lifecycle_context),
            runtime_config=config,
            receipt_dir=receipt_dir,
            state_path=Path(raw_state_path),
            kubeconfig=trusted_kubeconfig,
            verification_timeout_seconds=float(args.timeout_sec + 300),
            detection_timeout_seconds=(
                float(args.detection_timeout_sec) if args.detection_timeout_sec is not None else None
            ),
        ),
        ops=_cluster_ops(args, namespace),
        run_lifecycle=lifecycle,
        inject=lambda: request_fault_injection(api_base),
    )
    resolution["driver_phase_timings_seconds"]["conductor_wait"] = conductor_ready - started
    return resolution


def _run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    if os.getenv("SREGYM_DEFER_CLEANUP", "").strip() != "1":
        raise RuntimeError("sdo_codex requires defer_cleanup: true in the SREGym agent registry")
    api_base = get_api_base()
    logger.info("SDO agent backend: %s/%s", args.backend, args.provider)
    initial_stage = poll_stage_sync(
        api_base,
        wait_for=READY_STAGES | {AWAITING_FAULT_INJECTION},
        timeout=300,
        on_timeout="raise",
    )
    fault_deferred = initial_stage == AWAITING_FAULT_INJECTION
    conductor_ready = time.monotonic()
    app_info = get_app_info(api_base)
    repository = _application_repository()
    _configure_turn_usage_log(args.logs_dir)
    application = _application_label(app_info, repository)
    namespace = str(app_info.get("namespace") or "default")
    lifecycle_context = _deployed_lifecycle_context(namespace)
    health_objective = lifecycle_context.health_objective
    trusted_kubeconfig = os.getenv("SREGYM_BASE_KUBECONFIG", "").strip()
    if trusted_kubeconfig:
        os.environ["KUBECONFIG"] = trusted_kubeconfig
    control_namespace = control_namespace_for(namespace)
    gate = (
        _FaultGate(control_namespace, api_base, before_inject=_record_healthy_baseline(namespace, control_namespace))
        if fault_deferred and args.healthy_baseline
        else _FaultGate(control_namespace, api_base)
        if fault_deferred
        else None
    )
    if gate is not None:
        gate.start()
    host_receipt = _launch_native_lifecycle(
        args,
        repository=repository,
        namespace=namespace,
        application=application,
        health_objective=health_objective,
        api_base=api_base,
        persistent=False,
    )
    runtime_ready = time.monotonic()
    if gate is not None:
        try:
            gate.join()
        except InvalidInjectionError:
            _remove_sdo_jobs_before_benchmark_grading({}, control_namespace)
            raise
    native = _native_install_for_run(args, repository, namespace, application, api_base)
    if native.kubeconfig is None:
        raise RuntimeError("native benchmark evidence requires an absolute trusted kubeconfig")
    incident_id = _acknowledged_incident_id(control_namespace, native.kubeconfig)
    sync_existing(native)
    receipt = collect_native_receipt(native, incident_id)
    receipt["native_host_receipt"] = host_receipt
    artifact_dir = _receipt_directory(args.logs_dir, repository).resolve()
    receipt["runtime_artifacts"] = {"directory": str(export_runtime_artifacts(native, artifact_dir))}
    receipt["fault_injection_deferred"] = fault_deferred
    if gate is not None:
        receipt["fault_gate_timings_seconds"] = gate.timings
    receipt["driver_phase_timings_seconds"] = {
        "conductor_wait": conductor_ready - started,
        "native_lifecycle_and_controller": runtime_ready - conductor_ready,
        "driver_total_before_submission": runtime_ready - started,
    }
    return receipt


def _finish_invalid_injection(args: argparse.Namespace, api_base: str, exc: InvalidInjectionError) -> int:
    directory = _receipt_directory(args.logs_dir, _application_repository())
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "sdo_infrastructure_outcome.json").write_text(
        json.dumps(
            {
                "infra_error": exc.infra_error,
                "injection_evidence": exc.details,
                "cleanup_error": exc.cleanup_error,
                "incident_credit": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    signal_cleanup(api_base)
    return 1 if exc.cleanup_error else 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    api_base = get_api_base()
    logger.info("cleanup deferral watchdog configured for %.0fs", _cleanup_defer_timeout_seconds())
    if args.persistent_controller:
        return _main_persistent(args, api_base)
    try:
        receipt = _run(args)
    except InvalidInjectionError as exc:
        return _finish_invalid_injection(args, api_base, exc)
    except Exception:
        logger.exception("production SDO incident lifecycle failed")
        return 1
    receipt_dir = _receipt_directory(args.logs_dir, _application_repository())
    # Persist the SDO-authoritative recovery before optional benchmark transport
    # so a scoring failure cannot erase evidence of a completed incident.
    persist_production_receipt(receipt, receipt_dir)
    _remove_sdo_jobs_before_benchmark_grading(receipt, str(receipt["namespace"]))
    submission_started = time.monotonic()
    _submit_recorded_result(receipt, api_base)
    receipt.setdefault("driver_phase_timings_seconds", {})["benchmark_submission"] = (
        time.monotonic() - submission_started
    )
    persist_production_receipt(receipt, receipt_dir)
    print(f"SDO_RUN_TELEMETRY={json.dumps(receipt, sort_keys=True)}")
    signal_cleanup(api_base)
    return 0


def _main_persistent(args: argparse.Namespace, api_base: str) -> int:
    """Report resolution at verified health; the strict receipt follows the reflection drain."""

    started = time.monotonic()
    if os.getenv("SREGYM_DEFER_CLEANUP", "").strip() != "1":
        raise RuntimeError("sdo_codex requires defer_cleanup: true in the SREGym agent registry")
    try:
        resolution = _run_persistent(args, api_base, started)
    except InvalidInjectionError as exc:
        return _finish_invalid_injection(args, api_base, exc)
    except Exception:
        logger.exception("persistent SDO incident lifecycle failed")
        return 1
    receipt_dir = _receipt_directory(args.logs_dir, _application_repository())
    persist_resolution(resolution, receipt_dir)
    submission_started = time.monotonic()
    _submit_recorded_result(resolution, api_base)
    resolution["driver_phase_timings_seconds"]["benchmark_submission"] = time.monotonic() - submission_started
    persist_resolution(resolution, receipt_dir)
    print(f"SDO_RUN_TELEMETRY={json.dumps(resolution, sort_keys=True, default=str)}")
    signal_cleanup(api_base)
    return 0


if __name__ == "__main__":
    sys.exit(main())
