"""A seeded toy install runs the scripted health refresh, so its responder image must carry bwrap."""

import subprocess
from types import SimpleNamespace

import pytest


def _run_reporting(stdout: str, returncode: int = 0):
    calls: list[list[str]] = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout, "docker: no such image")

    return run, calls


def test_image_executables_names_the_missing_ones_and_the_build_command() -> None:
    from benchmarks.sregym.adapter import assert_image_executables

    run, calls = _run_reporting("bwrap\n")
    with pytest.raises(ValueError, match=r"lacks bwrap.*SDO_SCRIPTED_WORKER=1"):
        assert_image_executables("img:tag", ("bwrap",), run=run)
    assert calls[0][-1] == "bwrap"
    assert "img:tag" in calls[0]


def test_image_executables_accepts_an_image_with_all_of_them() -> None:
    from benchmarks.sregym.adapter import assert_image_executables

    run, _ = _run_reporting("")
    assert_image_executables("img:tag", ("bwrap",), run=run)


def test_image_executables_reports_a_failed_probe() -> None:
    from benchmarks.sregym.adapter import assert_image_executables

    run, _ = _run_reporting("", returncode=125)
    with pytest.raises(ValueError, match="probe failed"):
        assert_image_executables("img:tag", ("bwrap",), run=run)


def test_seeded_toy_up_rejects_an_unscripted_responder_before_touching_a_cluster(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from benchmarks.sregym.fastloop import toy

    run, _ = _run_reporting("bwrap\n")
    monkeypatch.setattr(subprocess, "run", run)
    args = SimpleNamespace(
        dev_mount=tmp_path,
        goal=None,
        fresh_memory=False,
        frozen_memory=False,
        variant="raw",
        responder_image="img:tag",
    )
    with pytest.raises(ValueError, match="lacks bwrap"):
        toy.up(args)
