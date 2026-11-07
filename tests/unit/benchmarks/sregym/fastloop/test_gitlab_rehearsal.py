"""``fastloop gitlab rehearse``: the harness input is the lane's source and one vendored tree per release."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import gitlab_rehearsal as rh
from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.gitlab_lane import NAMESPACE, deployment_source
from benchmarks.sregym.fastloop.gitlab_releases import load_releases
from tests.unit.apps.test_gitlab_workspace import FILES

if TYPE_CHECKING:
    from pathlib import Path

RELEASES = load_releases()


def _archive(directory: Path, version: str) -> Path:
    archive = directory / f"gitlab-{version}.tgz"
    files = {name: text.replace("version: 10.5.4", f"version: {version}") for name, text in FILES.items()}
    with tarfile.open(archive, "w:gz") as tar:
        for name, text in files.items():
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return archive


def test_the_plan_is_the_baseline_and_the_releases_after_it() -> None:
    assert [r.version for r in rh.plan_rehearsal(RELEASES, "10.4.2")] == [
        "10.4.2",
        "10.5.0",
        "10.5.1",
        "10.5.3",
        "10.5.4",
    ]
    assert [r.version for r in rh.plan_rehearsal(RELEASES, "10.5.0", limit=1)] == ["10.5.0", "10.5.1"]
    with pytest.raises(ValueError, match="no release follows"):
        rh.plan_rehearsal(RELEASES, "10.5.4")


def test_prepare_vendors_each_release_and_writes_the_harness_input(tmp_path: Path) -> None:
    archives = {}
    plan = []
    for version in ("10.4.2", "10.5.0"):
        archive = _archive(tmp_path, version)
        archives[version] = archive
        plan.append(replace(RELEASES.get(version), sha256=hashlib.sha256(archive.read_bytes()).hexdigest()))
    behaviors = (rh.Behavior(name="gitlab-objectstore-buckets", kind="Job", job_failures=-1, from_release=1),)
    path = rh.prepare(tmp_path / "out", tuple(plan), archives, behaviors=behaviors, warm_up="2m")
    document = json.loads(path.read_text())
    assert document["namespace"] == NAMESPACE
    assert document["deployment_source"] == deployment_source()
    assert [r["name"] for r in document["releases"]] == ["10.4.2", "10.5.0"]
    for entry in document["releases"]:
        chart_yaml = tmp_path / "out" / "releases" / entry["name"] / "chart" / "charts" / "gitlab" / "Chart.yaml"
        assert f"version: {entry['name']}" in chart_yaml.read_text()
        assert entry["dir"] == str((tmp_path / "out" / "releases" / entry["name"]).resolve())
    assert document["behaviors"] == [
        {"name": "gitlab-objectstore-buckets", "kind": "Job", "job_failures": -1, "from_release": 1}
    ]
    assert document["warm_up"] == "2m"


def test_input_and_behaviors_are_validated() -> None:
    with pytest.raises(ValueError, match="baseline and at least one release"):
        rh.RehearsalInput(NAMESPACE, {}, ())
    with pytest.raises(ValueError, match="names a workload"):
        rh.Behavior(name="")
    assert rh.parse_behaviors('[{"name": "x", "job_failures": 2}]') == (rh.Behavior(name="x", job_failures=2),)
    assert rh.parse_behaviors("") == ()


def test_the_cli_registers_gitlab_rehearse(tmp_path: Path) -> None:
    args = build_parser().parse_args(["gitlab", "rehearse", "--out", str(tmp_path), "--limit", "2"])
    assert args.handler is rh.rehearse
    assert args.limit == 2
