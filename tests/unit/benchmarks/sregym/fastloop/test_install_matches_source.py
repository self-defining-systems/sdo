"""A row is not admitted on an install whose workload images differ from the accepted source.

Social lanes 55/59/50/53 were deployed with the harness source-build tag while the accepted source declared
``:latest``/``:xenial``. SDO read that as 13 unexplained drifts, opened an incident 15 s after onboarding, and
its mitigation rolled media-frontend (a git clone in an init container). The restore decision must name the
mismatch before any cluster write instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    InstallDivergesFromSource,
    accepted_restore_baselines,
    load_application_baseline,
    save_application_baseline,
)
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.accepted_source import AcceptedSource

LANE_TAG = "sregym-src-app-lane-1"


def _deployment(name: str, image: str, *, init_image: str | None = None, kind: str = "Deployment") -> dict[str, Any]:
    spec: dict[str, Any] = {"containers": [{"name": name, "image": image}]}
    if init_image is not None:
        spec["initContainers"] = [{"name": "init", "image": init_image}]
    return {
        "apiVersion": "apps/v1",
        "kind": kind,
        "metadata": {"name": name, "namespace": "app"},
        "spec": {"template": {"spec": spec}},
    }


def _seal(tmp_path: Path, source: AcceptedSource, installed: list[dict[str, Any]]) -> tuple[ApplicationBaseline, ...]:
    path = tmp_path / "baseline.json"
    path.write_text("{}")
    save_application_baseline(path, (ApplicationBaseline("app", installed),), installed=True)
    baselines = load_application_baseline(path, "app")
    assert baselines is not None
    return baselines


def test_lane_tag_install_is_refused_with_every_diverging_workload_named(tmp_path: Path) -> None:
    declared = [
        _deployment("api", "docker.io/org/svc:latest"),
        _deployment(
            "media-frontend", "docker.io/yg397/media-frontend:xenial", init_image="docker.io/alpine/git:latest"
        ),
    ]
    source = commit_accepted_source(tmp_path / "workspace", declared)
    installed = [
        _deployment("api", f"docker.io/org/svc:{LANE_TAG}"),
        _deployment(
            "media-frontend",
            f"docker.io/yg397/media-frontend:{LANE_TAG}",
            init_image="docker.io/alpine/git:latest",
        ),
    ]

    with pytest.raises(InstallDivergesFromSource) as error:
        accepted_restore_baselines(source, _seal(tmp_path, source, installed))

    message = str(error.value)
    assert "Deployment/api" in message
    assert "Deployment/media-frontend" in message
    assert f"installed org/svc:{LANE_TAG}, accepted source org/svc:latest" in message
    assert "redeploy" in message


def test_init_container_image_divergence_is_refused(tmp_path: Path) -> None:
    source = commit_accepted_source(
        tmp_path / "workspace", [_deployment("api", "org/svc:1", init_image="org/init:1", kind="StatefulSet")]
    )
    installed = [_deployment("api", "org/svc:1", init_image="org/init:2", kind="StatefulSet")]
    with pytest.raises(InstallDivergesFromSource, match=r"StatefulSet/api.*org/init:2.*org/init:1"):
        accepted_restore_baselines(source, _seal(tmp_path, source, installed))


def test_matching_install_restores_unchanged_and_registry_prefixes_are_not_divergence(tmp_path: Path) -> None:
    source = commit_accepted_source(
        tmp_path / "workspace", [_deployment("api", "org/svc:latest"), _deployment("db", "mongo:4.4.6")]
    )
    installed = [_deployment("api", "docker.io/org/svc:latest"), _deployment("db", "docker.io/library/mongo:4.4.6")]
    restored = accepted_restore_baselines(source, _seal(tmp_path, source, installed))
    assert restored[0].objects == installed


def test_workloads_the_source_does_not_declare_are_not_compared(tmp_path: Path) -> None:
    source = commit_accepted_source(tmp_path / "workspace", [_deployment("api", "org/svc:latest")])
    installed = [_deployment("api", "org/svc:latest"), _deployment("runtime-only", "org/other:7")]
    assert accepted_restore_baselines(source, _seal(tmp_path, source, installed))[0].objects == installed


def test_repaired_source_wins_over_the_install_without_a_divergence_error(tmp_path: Path) -> None:
    """After SDO accepts a source repair the source, not the sealed install, is the restore input."""
    workspace = tmp_path / "workspace"
    seed_source = commit_accepted_source(workspace, [_deployment("api", "org/svc:1")])
    installed = [_deployment("api", "org/svc:1")]
    baselines = _seal(tmp_path, seed_source, installed)
    import subprocess

    seed = subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    repaired = commit_accepted_source(workspace, [_deployment("api", "org/svc:2")])

    restored = accepted_restore_baselines(repaired, baselines, seed_commit=seed)

    assert restored[0].objects[0]["spec"]["template"]["spec"]["containers"][0]["image"] == "org/svc:2"
