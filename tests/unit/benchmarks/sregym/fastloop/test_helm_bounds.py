"""Helm calls of an app deploy end with a named error instead of holding a worker request."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from benchmarks.sregym.fastloop import idempotent_helm


def test_a_helm_install_that_never_finishes_is_killed_and_named(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        seen.append(command)
        assert kwargs.get("timeout"), f"{command[:3]} has no timeout"
        if command[:3] == ["helm", "upgrade", "--install"]:
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="did not finish"):
        idempotent_helm.install_or_upgrade(release_name="r", chart_path="/c", namespace="n")
    assert seen[0][:3] == ["helm", "dependency", "update"]


def test_a_helm_dependency_update_that_hangs_falls_back_to_the_charts_on_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if command[:3] == ["helm", "dependency", "update"]:
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    idempotent_helm.install_or_upgrade(release_name="r", chart_path="/c", namespace="n")
