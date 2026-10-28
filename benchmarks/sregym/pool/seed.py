"""Render clean pool memory from source inputs and repository-owned health templates."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tarfile
from typing import TYPE_CHECKING

import yaml

from benchmarks.sregym.fastloop.accepted_source import AcceptedSource, manifest_paths_for
from benchmarks.sregym.pool.real_apps import SEED_RECEIPT

if TYPE_CHECKING:
    from pathlib import Path

TEMPLATES = {
    "hotel-reservation": "hotel_reservation_30e023d",
    "social-network": "social_network_handwritten",
    "opentelemetry-demo-astronomy-shop": "astronomy_shop_handwritten",
}


def render_seed(code_root: Path, source: Path, destination: Path, application: str, namespace: str) -> str:
    """Never import a run's memory: source archive + authored fixture inputs + current Go policy."""
    if destination.exists():
        raise ValueError(f"pool: seed destination already exists: {destination}")
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    build = subprocess.check_output(["git", "-C", str(code_root), "rev-parse", "HEAD"], text=True).strip()
    destination.mkdir(parents=True)
    archive = subprocess.Popen(["git", "-C", str(source), "archive", revision], stdout=subprocess.PIPE)
    try:
        assert archive.stdout is not None
        with tarfile.open(fileobj=archive.stdout, mode="r|") as contents:
            for member in contents:
                if member.name.split("/")[0] not in {".sdo", ".sdo-runtime"}:
                    contents.extract(member, destination, filter="data")
    finally:
        if archive.stdout is not None:
            archive.stdout.close()
        if archive.wait() != 0:
            raise RuntimeError("pool: source archive failed")
    template = (
        code_root / "benchmarks/sregym/experiments/assurance/seeds" / TEMPLATES[application] / "operational_memory"
    )
    memory = destination / ".sdo"
    memory.mkdir()
    # Keep only authored health inputs. No old generic packages, topology, rendered snapshots or incident data.
    for relative in (
        "goal.md",
        "playbooks/health-objective",
        "diagnostics/detectors/health/health-objective",
        "diagnostics/traffic",
    ):
        original, target = template / relative, memory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if original.is_dir():
            shutil.copytree(original, target)
        else:
            shutil.copy2(original, target)
    subprocess.run(["git", "init", "--quiet", str(destination)], check=True)
    git = ["git", "-C", str(destination)]
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run(
        [*git, "-c", "user.name=SDO pool", "-c", "user.email=pool@sdo.invalid", "commit", "-qm", "source inputs"],
        check=True,
    )
    rendered = AcceptedSource(destination, namespace, manifest_paths_for(application)).manifests
    fingerprint = hashlib.sha256(json.dumps(rendered, sort_keys=True).encode()).hexdigest()
    summary = (
        "# Application architecture\n\nAccepted source resources:\n\n"
        + "\n".join(f"- {obj['kind']} {obj['metadata']['name']}" for obj in rendered)
        + "\n"
    )
    header = {
        "schema_version": 1,
        "generated_at_commit": revision,
        "application": application,
        "topology_fingerprint": fingerprint,
    }
    (memory / "arch.md").write_text("---\n" + yaml.safe_dump(header) + "---\n" + summary)
    subprocess.run(
        ["go", "run", str(code_root / "benchmarks/sregym/pool/seed_render.go"), str(destination)],
        cwd=code_root / "sdo/operator/runtime",
        check=True,
    )
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run(
        [
            *git,
            "-c",
            "user.name=SDO pool",
            "-c",
            "user.email=pool@sdo.invalid",
            "commit",
            "-qm",
            "render fresh pool memory",
        ],
        check=True,
    )
    head = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
    (destination / ".git" / SEED_RECEIPT).write_text(
        json.dumps(
            {
                "build_revision": build,
                "source_revision": revision,
                "seed_revision": head,
                "render_digest": fingerprint,
                "template": str(template.relative_to(code_root)),
            },
            indent=2,
        )
        + "\n"
    )
    return head
