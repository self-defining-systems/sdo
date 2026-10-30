"""Pinned-tools lanes refuse injection unless the rendered controller Job can reuse attestations.

h9.26 lanes launched from mutable tags and a dev mount, so the broker never reused a trusted
validation live even though the digest-pinned rehearsal had proven it. The fixtures are the
controller Jobs the Go installer renders (``sdo/operator/install/controller_job_golden_test.go``).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import pinned_tools
from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images
from benchmarks.sregym.fastloop.pinned_tools import (
    AttestationIneligibleError,
    assert_attestation_eligible,
    attestation_problems,
)
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_sdo_agent import FakeOps, _agent, _inject

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.loop import InjectionWindow

GOLDENS = Path(__file__).resolve().parents[5] / "sdo/operator/install/testdata/controller_job"
DIGEST = "127.0.0.1:5000/sdo/images@sha256:" + "c" * 64


def _job(name: str) -> dict[str, Any]:
    return json.loads((GOLDENS / name).read_text())


def test_only_the_rendered_digest_pinned_job_without_dev_mount_is_eligible() -> None:
    assert attestation_problems(_job("pinned.json")) == ()
    tagged = attestation_problems(_job("tagged.json"))
    assert any("validator image is not an immutable digest" in problem for problem in tagged)
    assert any("compiler image is not an immutable digest" in problem for problem in tagged)
    dev = attestation_problems(_job("dev_mount.json"))
    assert any("SDO_DEV_MOUNT=1" in problem for problem in dev)
    assert any("mounts development sources" in problem for problem in dev)


def test_the_live_check_fails_closed_on_an_unreadable_job() -> None:
    def unreadable(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", "jobs.batch not found")

    with pytest.raises(AttestationIneligibleError, match="unreadable"):
        assert_attestation_eligible("/k", "demo-sdo", run=unreadable)


@pytest.mark.parametrize(
    ("golden", "injects"), [("pinned.json", True), ("tagged.json", False), ("dev_mount.json", False)]
)
def test_a_pinned_lane_injects_only_after_its_launched_controller_proves_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, golden: str, injects: bool
) -> None:
    reads: list[list[str]] = []

    def kubectl(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        reads.append(command)
        return subprocess.CompletedProcess(command, 0, (GOLDENS / golden).read_text(), "")

    # Only the pinned-tools module reads through this kubectl; Go observation keeps the real subprocess.
    monkeypatch.setattr(pinned_tools, "subprocess", SimpleNamespace(run=kubectl))
    agent = _agent(tmp_path, FakeOps(), [], require_attestation_eligible=True)
    injected: list[bool] = []

    def inject() -> InjectionWindow:
        injected.append(True)
        return _inject()

    if injects:
        agent.resolve(0, "missing_configmap_hotel_reservation", inject)
    else:
        with pytest.raises(AttestationIneligibleError):
            agent.resolve(0, "missing_configmap_hotel_reservation", inject)
    assert injected == ([True] if injects else [])
    assert reads
    assert reads[0][-4:] == ["job", "sdo-controller-run", "-o", "json"]
    assert "hotel-reservation-sdo" in reads[0]


def _environment(tmp_path: Path, **fields: Any) -> FastloopEnvironment:
    return FastloopEnvironment(
        run_dir=tmp_path,
        cluster="lane",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        dev_mount=tmp_path / "worktree",
        **fields,
    )


def test_a_pinned_lane_requires_digest_tools_and_launches_its_controller_without_the_dev_mount(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="immutable controller digest"):
        _environment(tmp_path, pinned_tools=True)
    pinned = _environment(
        tmp_path, pinned_tools=True, images=Images(controller=DIGEST, validator=DIGEST, responder="r:tag")
    )
    environ = {"SDO_DEV_MOUNT": "1"}
    pinned.apply_operator_env(environ)
    assert environ == {"SDO_OPERATOR_BIN": str(tmp_path / "worktree/.fastloop/sdo-operator")}

    environ = {}
    _environment(tmp_path).apply_operator_env(environ)
    assert environ["SDO_DEV_MOUNT"] == "1"
