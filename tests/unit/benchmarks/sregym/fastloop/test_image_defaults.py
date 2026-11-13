"""``up`` defaults to the images ``scripts/build_sdo_images.sh`` builds for the same ``SDO_IMAGE_TAG``."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.environment import Images

BUILD_SCRIPT = Path(__file__).resolve().parents[5] / "scripts/build_sdo_images.sh"


def test_defaults_follow_the_build_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_IMAGE_TAG", "pr-1")
    args = build_parser().parse_args(["up", "--run-dir", "x"])
    assert (args.controller_image, args.responder_image) == ("sdo-controller:pr-1", "sdo-sregym-responder:pr-1")
    monkeypatch.delenv("SDO_IMAGE_TAG")
    assert Images().controller == "sdo-controller:fastloop"


def test_default_image_names_are_built_by_the_script() -> None:
    built = set(re.findall(r"image=(sdo-[a-z-]+)", BUILD_SCRIPT.read_text()))
    assert {Images().controller.split(":")[0], Images().responder.split(":")[0]} <= built
