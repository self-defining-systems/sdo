from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.reset import ensure_application_namespace

if TYPE_CHECKING:
    from pathlib import Path


def test_restore_recreates_only_a_confirmed_missing_application_namespace(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def command(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        if args[0] == "get":
            return subprocess.CompletedProcess(
                args, 1, "", 'Error from server (NotFound): namespaces "social-network" not found'
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", command)
    ensure_application_namespace(tmp_path / "kubeconfig", "social-network")
    assert json.loads(calls[-1][1]["input_text"]) == {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "social-network"},
    }


def test_restore_never_treats_authorization_failure_as_absence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = []

    def command(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", "Forbidden")

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", command)
    with pytest.raises(RuntimeError, match="Forbidden"):
        ensure_application_namespace(tmp_path / "kubeconfig", "social-network")
    assert len(calls) == 1


@pytest.mark.parametrize("name", ["kube-system", "observe", "social-network-sdo"])
def test_restore_refuses_nonapplication_namespaces(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValueError, match="non-application"):
        ensure_application_namespace(tmp_path / "kubeconfig", name)
