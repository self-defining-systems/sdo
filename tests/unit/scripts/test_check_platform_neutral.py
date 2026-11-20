"""Contract of the platform-neutrality guard, using throwaway git repos as negative controls."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from scripts import check_platform_neutral as guard

if TYPE_CHECKING:
    from pathlib import Path


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    return tmp_path


def _run(root: Path, allow: dict | None = None) -> list[str]:
    (root / "allow.json").write_text(json.dumps(allow or {}))
    return guard.check(root, ("sdo",), root / "allow.json")[0]


@pytest.mark.parametrize(
    "text",
    [
        "run docker exec node ls",
        "a kind cluster",
        "corev1.HostPathVolumeSource{}",
        "crictl pull x",
        'import "github.com/aws/aws-sdk-go-v2"',
        "123.dkr.ecr.us-west-2.amazonaws.com/app",
        "us-docker.pkg.dev/p/r",
        "myreg.azurecr.io/app",
        "the EKS lane",
        "&corev1.NFSVolumeSource{}",
    ],
)
def test_platform_specifics_are_caught(tmp_path: Path, text: str) -> None:
    assert any("sdo/a.go" in e for e in _run(_repo(tmp_path, {"sdo/a.go": text})))


@pytest.mark.parametrize(
    "text",
    ['Kind: "ClusterRole"', 'ref.Kind == "Pod"', "if v.HostPath != nil { reject }", "kind of like a cluster"],
)
def test_ordinary_kubernetes_code_passes(tmp_path: Path, text: str) -> None:
    assert _run(_repo(tmp_path, {"sdo/a.go": text})) == []


def test_tests_generated_code_and_other_trees_are_ignored(tmp_path: Path) -> None:
    files = {"sdo/a_test.go": "docker exec", "sdo/x/gen/a.go": "kind cluster", "benchmarks/a.py": "docker exec"}
    assert _run(_repo(tmp_path, files)) == []


def test_ratchet_allows_equal_rejects_growth_and_stale_counts(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"sdo/a.go": "docker exec; docker run"})
    assert _run(root, {"sdo/a.go": {"count": 2, "reason": "r"}}) == []
    assert any("exceed" in e for e in _run(root, {"sdo/a.go": {"count": 1, "reason": "r"}}))
    assert any("lower the ratchet" in e for e in _run(root, {"sdo/a.go": {"count": 3, "reason": "r"}}))
    assert any("needs a reason" in e for e in _run(root, {"sdo/a.go": {"count": 2, "reason": ""}}))


def test_repository_is_within_its_ratchet() -> None:
    assert guard.check()[0] == []
