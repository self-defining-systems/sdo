"""The run manifest: what produced a run directory, written into it at launch.

``run_manifest.json`` records the code (SDO and SREGym commits and whether
either was dirty), the images (IDs and repo digests) and the Codex CLI and
agentshim versions actually present, the model, provider and effort each agent
role and the judge resolved to, the kind topology, the hash of the config
snapshot (and of the source TOML), the host's load and free disk at launch, a
Codex baseline's resolved verify-protocol mode and a hash of its appendix text
(``codex_prompt_appendix``), and the launch preflight report. A resumed run
keeps its first manifest and appends each resume under ``resumes``.

The validity checker (``benchmarks.sregym.analysis.run_validity``) requires a
manifest, and treats a manifest whose preflight was waived as ``invalid_infra``.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, cast

from benchmarks.sregym.runner.preflight import (
    DEFAULT_KIND_WORKERS,
    PreflightSettings,
    lane_clusters,
    resolve_roles,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from benchmarks.sregym.runner.experiment import ExperimentConfig
    from benchmarks.sregym.runner.preflight import HostProbe, PreflightReport

MANIFEST_NAME = "run_manifest.json"
MANIFEST_SCHEMA_VERSION = 1
SUBMODULE_PATH = "third_party/sregym"
RunKind = Literal["experiment", "pipeline", "stage"]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_state(host: HostProbe, project_root: Path, sregym_dir: Path) -> dict[str, Any]:
    status = host.git(project_root, "status", "--porcelain", "--untracked-files=no")
    recorded = host.git(project_root, "ls-tree", "HEAD", SUBMODULE_PATH)
    submodule_status = host.git(sregym_dir, "status", "--porcelain", "--untracked-files=no")
    return {
        "sha": host.git(project_root, "rev-parse", "HEAD"),
        "dirty": None if status is None else bool(status),
        "dirty_paths": [] if not status else [line[3:] for line in status.splitlines()],
        "submodule": {
            "path": SUBMODULE_PATH,
            "sha": host.git(sregym_dir, "rev-parse", "HEAD"),
            "recorded_sha": recorded.split()[2] if recorded and len(recorded.split()) >= 3 else None,
            "dirty": None if submodule_status is None else bool(submodule_status),
        },
    }


def _images(report: PreflightReport, host: HostProbe) -> dict[str, Any]:
    """The preflight's probed image facts, with IDs re-read now (a stage may start long after preflight)."""

    images: dict[str, Any] = {}
    for label, facts in cast("dict[str, dict[str, Any]]", report.facts.get("images") or {}).items():
        current = host.image(str(facts["ref"]))
        images[label] = {
            **facts,
            "id": current.id if current else None,
            "repo_digests": list(current.repo_digests) if current else [],
            "id_at_preflight": facts.get("id"),
        }
    return images


def _topology(configs: Sequence[ExperimentConfig], env: Mapping[str, str], host: HostProbe) -> dict[str, Any]:
    clusters = sorted({cluster for config in configs for cluster in lane_clusters(config, env)})
    requested = {config.env.kind_worker_nodes or DEFAULT_KIND_WORKERS for config in configs}
    existing = host.kind_clusters() or []
    return {
        "clusters": clusters,
        "worker_nodes": requested.pop() if len(requested) == 1 else sorted(requested),
        "nodes": {cluster: host.kind_nodes(cluster) for cluster in clusters if cluster in existing},
    }


def _codex_prompt_appendix(configs: Sequence[ExperimentConfig]) -> list[dict[str, Any]] | None:
    """Each ``agent = "codex"`` config's resolved verify-protocol mode and appendix hash.

    Recorded so a run manifest shows exactly what instruction text the agent
    ran under (the assurance plan C11 needs to distinguish "concise" from "full" from
    "none"), without embedding the whole appendix text in every manifest.
    """

    from benchmarks.sregym.runner.codex_baseline import CodexBaselineConfig

    entries: list[dict[str, Any]] = []
    for config in configs:
        if config.agent != "codex":
            continue
        baseline = CodexBaselineConfig.from_agent_config(config.agent_config.get("codex") or {})
        appendix = baseline.prompt_appendix()
        entries.append(
            {
                "mode": baseline.verify_protocol,
                "exec_disclosure": baseline.exec_disclosure,
                "allow_exec": baseline.allow_exec,
                "sha256": hashlib.sha256(appendix.encode("utf-8")).hexdigest() if appendix else None,
            }
        )
    return entries or None


def _host(host: HostProbe, sregym_dir: Path) -> dict[str, Any]:
    load = host.load_average()
    docker_root = host.docker_root()
    disks = {"logs": host.free_bytes(sregym_dir / "logs")}
    if docker_root is not None:
        disks["docker"] = host.free_bytes(docker_root)
    return {
        "hostname": socket.gethostname(),
        "kernel": f"{os.uname().sysname} {os.uname().release} {os.uname().machine}",
        "python": sys.version.split()[0],
        "cpu_count": os.cpu_count(),
        "load_average": list(load) if load is not None else None,
        "disk_free_bytes": disks,
    }


def build_run_manifest(
    *,
    run_dir: Path,
    configs: Sequence[ExperimentConfig],
    snapshot: Path,
    report: PreflightReport,
    host: HostProbe,
    project_root: Path,
    sregym_dir: Path,
    env: Mapping[str, str],
    kind: RunKind = "experiment",
    source: Path | None = None,
    settings: PreflightSettings | None = None,
) -> dict[str, Any]:
    """Collect the manifest of one run directory; *snapshot* is the config file written into it."""

    policy = (settings or PreflightSettings()).policy
    roles = [
        {role: vars(resolved) for role, resolved in resolve_roles(config, env, policy).items()} for config in configs
    ]
    judges = {(arm["sregym_judge"]["model"], arm["sregym_judge"]["effort"]) for arm in roles}
    judge_model, judge_effort = judges.pop() if len(judges) == 1 else (sorted(str(j) for j in judges), None)
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": kind,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "git": _git_state(host, project_root, sregym_dir),
        "images": _images(report, host),
        "versions": {
            "codex_cli": report.facts.get("codex_cli"),
            "agentshim": report.facts.get("agentshim"),
        },
        "models": roles,
        "judge": {"model": judge_model, "effort": judge_effort},
        "kind_topology": _topology(configs, env, host),
        "config": {
            "snapshot": snapshot.name,
            "sha256": _sha256(snapshot),
            "source": str(source) if source else None,
            "source_sha256": _sha256(source) if source and source.is_file() else None,
        },
        "host": _host(host, sregym_dir),
        "codex_prompt_appendix": _codex_prompt_appendix(configs),
        "codex_quota": report.facts.get("codex_quota"),
        "lanes": report.facts.get("lanes"),
        "preflight": report.to_dict(),
        "fault_capabilities": report.facts.get("fault_capabilities", []),
    }


def write_run_manifest(run_dir: Path, manifest: dict[str, Any]) -> Path:
    """Write *manifest*; when the run already has one (a resume), append it under ``resumes``."""

    path = run_dir / MANIFEST_NAME
    if path.is_file():
        document = cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
        resumes = cast("list[dict[str, Any]]", document.setdefault("resumes", []))
        resumes.append({key: value for key, value in manifest.items() if key != "resumes"})
    else:
        document = manifest
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def read_run_manifest(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / MANIFEST_NAME
    if not path.is_file():
        return None
    return cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
