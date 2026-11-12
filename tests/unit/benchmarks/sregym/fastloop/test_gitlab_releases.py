"""The GitLab release list (data), the umbrella dependency bump, and a lane that follows its upstream."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import tarfile
from typing import TYPE_CHECKING

import pytest
import yaml

from apps.gitlab import assemble
from benchmarks.sregym.fastloop.gitlab_releases import RELEASES_PATH, load_releases, parse_releases
from tests.unit.apps.test_gitlab_workspace import FILES

if TYPE_CHECKING:
    from pathlib import Path


def _document() -> dict:
    return json.loads(RELEASES_PATH.read_text())


def test_the_committed_list_is_the_10_5_line_after_a_baseline() -> None:
    releases = load_releases()
    assert releases.line == "10.5"
    assert releases.releases[0].baseline
    assert [r.version for r in releases.releases[1:]] == ["10.5.0", "10.5.1", "10.5.3", "10.5.4"]
    assert len(releases.releases) >= 5


def test_the_lane_pin_is_a_release_of_the_list() -> None:
    pin = assemble.load_pins()["gitlab_chart"]
    release = load_releases().get(pin["version"])
    assert (release.sha256, release.url, release.app_version) == (pin["sha256"], pin["url"], pin["app_version"])


def test_release_pins_replace_only_the_upstream_chart() -> None:
    base = assemble.load_pins()
    pinned = load_releases().get("10.5.0").pins(base)
    assert pinned["gitlab_chart"]["version"] == "10.5.0"
    assert pinned["gitlab_chart"]["sha256"] != base["gitlab_chart"]["sha256"]
    assert pinned["gitlab_chart"]["name"] == base["gitlab_chart"]["name"]
    assert {k: v for k, v in pinned.items() if k != "gitlab_chart"} == {
        k: v for k, v in base.items() if k != "gitlab_chart"
    }
    assert base["gitlab_chart"]["version"] == "10.5.4"


def test_after_names_the_releases_a_lane_still_has_to_follow() -> None:
    releases = load_releases()
    assert [r.version for r in releases.after("10.5.1")] == ["10.5.3", "10.5.4"]
    assert releases.after("10.5.4") == ()
    with pytest.raises(ValueError, match="10.9.9"):
        releases.after("10.9.9")


def test_change_classification_decides_whether_zero_agent_turns_are_expected() -> None:
    by_version = {r.version: r for r in load_releases().releases}
    images = {v: r.changes.changes_images for v, r in by_version.items() if r.changes is not None}
    assert images == {"10.5.0": True, "10.5.1": True, "10.5.3": True, "10.5.4": False}
    assert all(by_version[v].expects_zero_agent_turns for v in ("10.5.0", "10.5.1", "10.5.3", "10.5.4"))
    document = _document()
    document["releases"][2]["changes"]["workloads_added"] = ["Deployment/gitlab/new"]
    assert not parse_releases(document).get("10.5.1").expects_zero_agent_turns
    document = _document()
    document["releases"][2]["changes"]["values_schema"] = True
    assert not parse_releases(document).get("10.5.1").expects_zero_agent_turns


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (lambda d: d["releases"][1].update(sha256="abc"), "sha256"),
        (lambda d: d["releases"][1].update(version="10.5"), "major.minor.patch"),
        (lambda d: d["releases"][1].update(url="http://x/gitlab-10.5.0.tgz"), "https archive"),
        (lambda d: d["releases"].reverse(), "baseline"),
        (lambda d: d["releases"][0].pop("baseline"), "baseline"),
        (lambda d: d["releases"].append(copy.deepcopy(d["releases"][1])), "unique"),
        (lambda d: d["releases"][2].update(created="2026-01-01T00:00:00Z"), "publication order"),
        (lambda d: d["releases"][1]["changes"].update(images_changed=-1), "negative"),
    ],
)
def test_malformed_release_data_is_refused(edit, message: str) -> None:
    document = _document()
    edit(document)
    with pytest.raises(ValueError, match=message):
        parse_releases(document)


def _archive_at(directory: Path, version: str) -> tuple[Path, dict]:
    """The synthetic stand-in chart, repacked as ``version`` (the vendored chart declares what the pins name)."""
    archive = directory / f"gitlab-{version}.tgz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, text in FILES.items():
            data = text.replace("version: 10.5.4", f"version: {version}").encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    pins = load_releases().get(version).pins(assemble.load_pins())
    pins["gitlab_chart"]["sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    return archive, pins


def test_assembling_at_another_release_declares_that_dependency(tmp_path: Path) -> None:
    archive, pins = _archive_at(tmp_path, "10.5.0")
    dest = assemble.assemble(tmp_path / "ws", archive=archive, pins=pins)
    umbrella = yaml.safe_load((dest / "chart" / "Chart.yaml").read_text())
    assert next(d for d in umbrella["dependencies"] if d["name"] == "gitlab")["version"] == "10.5.0"
    assert "version: 10.5.0" in (dest / "chart/charts/gitlab/Chart.yaml").read_text()
    assert (assemble.CHART / "Chart.yaml").read_text().count("version: 10.5.4") == 1


def test_bump_dependency_is_idempotent_and_refuses_an_unknown_dependency(tmp_path: Path) -> None:
    chart_yaml = tmp_path / "Chart.yaml"
    chart_yaml.write_text((assemble.CHART / "Chart.yaml").read_text())
    assert assemble.bump_dependency(chart_yaml, "gitlab", "10.5.1")
    assert not assemble.bump_dependency(chart_yaml, "gitlab", "10.5.1")
    assert "version: 10.5.1" in chart_yaml.read_text()
    assert "name: postgresql\n    version: 0.1.0" in chart_yaml.read_text()
    with pytest.raises(ValueError, match="nope"):
        assemble.bump_dependency(chart_yaml, "nope", "1.0.0")


def test_up_takes_the_release_the_lane_starts_at() -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(["up", "--run-dir", "/tmp/x", "--app", "gitlab", "--chart-release", "10.5.0"])
    assert args.chart_release == "10.5.0"
    assert load_releases().get(args.chart_release).baseline is False
