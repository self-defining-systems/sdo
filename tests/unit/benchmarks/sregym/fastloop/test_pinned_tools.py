"""Pinned-tools lanes require a digest controller image and never launch it from the development mount."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images

if TYPE_CHECKING:
    from pathlib import Path

DIGEST = "127.0.0.1:5000/sdo/images@sha256:" + "c" * 64


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
    pinned = _environment(tmp_path, pinned_tools=True, images=Images(controller=DIGEST, responder="r:tag"))
    environ = {"SDO_DEV_MOUNT": "1"}
    pinned.apply_operator_env(environ)
    assert environ == {"SDO_OPERATOR_BIN": str(tmp_path / "worktree/.fastloop/sdo-operator")}

    environ = {}
    _environment(tmp_path).apply_operator_env(environ)
    assert environ["SDO_DEV_MOUNT"] == "1"
