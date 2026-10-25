"""Fail before worker discovery when a nested source checkout is missing."""

import json
import subprocess

import pytest


def test_initialized_harness_with_missing_nested_chart_is_rejected(tmp_path):
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources

    def git(directory, *args):
        return subprocess.run(
            ["git", "-C", str(directory), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    git(tmp_path, "init", "-q")
    metadata = tmp_path / "sregym/service/metadata"
    metadata.mkdir(parents=True)
    for app in ("astronomy-shop", "social-network", "hotel-reservation"):
        (metadata / f"{app}.json").write_text(json.dumps({"Namespace": app, "Helm Config": {"chart_path": app}}))
    apps = tmp_path / "SREGym-applications"
    apps.mkdir()
    git(apps, "init", "-q")
    for app in ("astronomy-shop", "social-network", "hotel-reservation"):
        (apps / app).mkdir()
        (apps / app / "Chart.yaml").write_text("name: fixture\n")
    git(apps, "add", ".")
    git(apps, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qm", "sources")
    git(tmp_path, "add", ".")
    git(tmp_path, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qm", "harness")
    assert_harness_sources(tmp_path)
    (apps / "astronomy-shop/Chart.yaml").unlink()
    with pytest.raises(ValueError, match="application source prerequisite.*astronomy-shop"):
        assert_harness_sources(tmp_path)
    git(apps, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qam", "wrong revision")
    with pytest.raises(ValueError, match="pinned applications revision"):
        assert_harness_sources(tmp_path)
