"""``check-input`` upgrades an old baseline with no human step: SDO's schema upgrade owns its own rewrites.

Runs this checkout's ``sdo-operator`` (the prebuilt test binary) through the real ``memory migrate`` that
``check-input`` runs, which compiles the migrated memory. It runs nightly; the PR-tier ownership rows of the Go
gate table (``memory/validate/gate_table_test.go``, "upkeep migration ...") pin the same rule in milliseconds.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import hooks
from benchmarks.fuzzops.hooks import Baseline, write_baseline

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.go_integration

GOAL = '---\nschema_version: 1\nowner: human\napplication: "synthetic"\n---\n# Health objective\n\nusers are served\n'
ARCH = '---\nschema_version: 1\ngenerated_at_commit: base\napplication: "synthetic"\n---\nA synthetic web service.\n'
# The plan as the deployer wrote it before the one-deployment-path refactor: its retired fields and a retired view.
PLAN = {
    "source": {
        "rendererId": "raw",
        "config": {
            "@type": "type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig",
            "paths": [".sdo/deployment/manifests"],
        },
    },
    "expectedWorkloadIds": ["Deployment/ns1/web"],
    "strategyId": "raw-manifests-v1",
}
PATCH = (
    "SDO-Patch: 1\nOwner: deployer\nReason: synthetic\n\n"
    "diff --git a/chart/job.yaml b/chart/job.yaml\n--- a/chart/job.yaml\n+++ b/chart/job.yaml\n"
    "@@ -1,2 +1,3 @@\n kind: Job\n+pull: IfNotPresent\n name: migrate\n"
)


def _git(repo: Path, *args: str) -> str:
    base = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@invalid"]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def _old_baseline(tmp: Path) -> Path:
    """A minimal synthetic schema-14 baseline: retired plan fields, a retired view and a deployment patch."""
    repo = tmp / "repo"
    files = {
        ".sdo/schema-version": "14\n",
        ".sdo/goal.md": GOAL,
        ".sdo/arch.md": ARCH,
        ".sdo/deployment/plan.yaml": json.dumps(PLAN) + "\n",
        ".sdo/deployment/manifests/web.yaml": "kind: Deployment\n",
        ".sdo/deployment/patches/0001-pull.patch": PATCH,
        "chart/job.yaml": "kind: Job\nname: migrate\n",
    }
    for rel, text in files.items():
        (repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (repo / rel).write_text(text)
    _git(repo, "init", "-q", "-b", "baseline")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "synthetic baseline built before the deployment refactor")
    root = tmp / "baseline"
    root.mkdir()
    _git(repo, "bundle", "create", str(root / "memory.bundle"), "baseline")
    (root / "render.yaml").write_text("apiVersion: v1\nkind: Service\nmetadata: {name: web, namespace: ns1}\n")
    write_baseline(
        root,
        Baseline(
            namespace="ns1",
            bundle="memory.bundle",
            branch="baseline",
            commit=_git(repo, "rev-parse", "HEAD"),
            render="render.yaml",
            objective="users are served",
        ),
    )
    return root


def test_check_input_upgrades_a_baseline_from_before_the_deployment_refactor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any, operator_binary: Path
) -> None:
    # check-input builds the checkout's operator; the session's verified build is that binary.
    monkeypatch.setattr(hooks, "build_operator", lambda root, out: shutil.copy2(operator_binary, out))
    monkeypatch.setenv("BENCH_INPUT_KIND", "baseline")
    monkeypatch.setenv("BENCH_INPUT_PATH", str(_old_baseline(tmp_path)))
    monkeypatch.setenv("BENCH_INPUT_PHASE", "add")
    monkeypatch.setenv("FUZZOPS_NAMESPACE", "ns1")
    assert hooks.main(["check-input"]) == 0
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert answer["ok"] is True, answer
    assert "memory check clean" in answer["detail"]
