"""The image-only preflight CI runs on freshly built SDO images, before any experiment config exists."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.runner import preflight
from benchmarks.sregym.runner.preflight import ImageVersions, check_images

if TYPE_CHECKING:
    from sregym_fake_host import FakeHost

REPO_ROOT = Path(__file__).resolve().parents[5]


def _check(host: FakeHost, images: dict[str, str]) -> preflight.PreflightReport:
    return check_images(images, project_root=REPO_ROOT, sregym_dir=REPO_ROOT / "third_party" / "sregym", host=host)


def test_built_images_that_import_their_entry_points_at_the_pins_pass(fake_host: FakeHost) -> None:
    report = _check(fake_host, {"controller_image": "ci/controller:1", "responder_image": "ci/responder:1"})

    assert report.ok, report.to_dict()
    assert sorted(fake_host.probed) == ["ci/controller:1", "ci/responder:1"]
    assert {check.name for check in report.checks} == {"codex-cli-pins", "agentshim", "sdo-images"}


def test_an_image_that_cannot_import_its_entry_point_fails(fake_host: FakeHost) -> None:
    fake_host.versions["ci/responder:1"] = ImageVersions(
        codex="0.163.0", agentshim="0.7.0", import_error="sdo.agents.responder.worker: ModuleNotFoundError: httpx"
    )

    report = _check(fake_host, {"responder_image": "ci/responder:1"})

    assert not report.ok
    assert "ModuleNotFoundError" in next(c for c in report.checks if c.name == "sdo-images").detail


def test_an_unknown_image_role_is_rejected(fake_host: FakeHost) -> None:
    with pytest.raises(ValueError, match="responder"):
        _check(fake_host, {"reponder_image": "ci/responder:1"})


def test_the_cli_checks_only_the_named_images(fake_host: FakeHost, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(preflight, "SystemHost", lambda: fake_host)

    assert preflight.main(["--image", "validator_image=ci/validator:1"]) == 0
    assert fake_host.probed == ["ci/validator:1"]

    fake_host.missing_images = {"ci/validator:1"}
    assert preflight.main(["--image", "validator_image=ci/validator:1"]) == 1


def test_the_cli_needs_configs_or_images() -> None:
    with pytest.raises(SystemExit):
        preflight.main([])
