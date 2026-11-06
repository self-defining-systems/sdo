"""Assemble the GitLab lane's source workspace: the app-owned umbrella chart plus the vendored upstream chart.

The upstream gitlab chart is a large third-party artifact: it is fetched here at lane setup, verified against
``pins.json`` and never committed. The result is one Git-committable source root:

    chart/                      umbrella chart (committed, ``apps/gitlab/chart``)
    chart/charts/gitlab/        the upstream chart, unmodified (fetched)
    chart/charts/{postgresql,redis,objectstore}/   app-owned datastores

SDO renders it with one helm DeploymentSource (``deployment_source()``).

    python apps/gitlab/assemble.py <empty-dest> [--archive gitlab-10.5.4.tgz]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

APP = Path(__file__).resolve().parent
CHART = APP / "chart"
PINS = APP / "pins.json"
VALUE_FILES = ("chart/values-kind.yaml",)
_CONFIG_TYPE = "type.googleapis.com/sdodev.contracts.v1alpha1."


def load_pins() -> dict[str, Any]:
    return json.loads(PINS.read_text())


def deployment_source(value_files: tuple[str, ...] = VALUE_FILES) -> dict[str, Any]:
    """ProtoJSON ``DeploymentSource`` for the assembled workspace (helm renderer over the umbrella chart)."""
    return {
        "renderer_id": "helm",
        "config": {"@type": _CONFIG_TYPE + "HelmRenderConfig", "chart_path": "chart", "value_files": list(value_files)},
    }


def fetch_chart(pins: dict[str, Any], dest: Path) -> Path:
    """Download the pinned upstream chart archive into ``dest`` (a directory) and verify its digest."""
    chart = pins["gitlab_chart"]
    target = dest / f"{chart['name']}-{chart['version']}.tgz"
    with urllib.request.urlopen(chart["url"], timeout=300) as response:
        target.write_bytes(response.read())
    return target


def verify_archive(archive: Path, sha256: str) -> None:
    actual = hashlib.sha256(archive.read_bytes()).hexdigest()
    if actual != sha256:
        raise ValueError(f"{archive.name}: sha256 {actual} does not match the pin {sha256}")


def vendor_chart(archive: Path, chart_dir: Path, pins: dict[str, Any]) -> Path:
    """Unpack the verified upstream archive as ``chart_dir/charts/<name>`` (the archive's top directory is the name)."""
    pin = pins["gitlab_chart"]
    verify_archive(archive, pin["sha256"])
    charts = chart_dir / "charts"
    with tarfile.open(archive) as tar:
        tar.extractall(charts, filter="data")
    vendored = charts / pin["name"]
    chart_yaml = vendored / "Chart.yaml"
    if not chart_yaml.is_file() or f"version: {pin['version']}" not in chart_yaml.read_text():
        raise ValueError(f"{archive.name} does not unpack to {pin['name']} {pin['version']}")
    return vendored


def bump_dependency(chart_yaml: Path, name: str, version: str) -> bool:
    """Set the umbrella dependency ``name`` to ``version`` in ``chart_yaml`` (a release bump); True when it changed."""
    text = chart_yaml.read_text()
    pattern = re.compile(rf"(- name: {re.escape(name)}\n\s+version: )(\S+)")
    match = pattern.search(text)
    if match is None:
        raise ValueError(f"{chart_yaml} has no dependency {name!r}")
    if match.group(2) == version:
        return False
    chart_yaml.write_text(pattern.sub(rf"\g<1>{version}", text, count=1))
    return True


def assemble(dest: Path, *, archive: Path | None = None, pins: dict[str, Any] | None = None) -> Path:
    """Write the lane workspace into the empty ``dest``; fetch the upstream chart unless ``archive`` is given."""
    pins = pins or load_pins()
    if dest.exists() and any(dest.iterdir()):
        raise ValueError(f"destination {dest} is not empty")
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copytree(CHART, dest / "chart")
    # A lane assembled at another release declares that release as its dependency, as an upstream bump commit would.
    bump_dependency(dest / "chart" / "Chart.yaml", pins["gitlab_chart"]["name"], pins["gitlab_chart"]["version"])
    if archive is not None:
        vendor_chart(archive, dest / "chart", pins)
    else:
        with tempfile.TemporaryDirectory() as scratch:
            vendor_chart(fetch_chart(pins, Path(scratch)), dest / "chart", pins)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dest", type=Path)
    parser.add_argument("--archive", type=Path, help="use this upstream chart archive instead of downloading it")
    args = parser.parse_args()
    print(assemble(args.dest, archive=args.archive))


if __name__ == "__main__":
    main()
