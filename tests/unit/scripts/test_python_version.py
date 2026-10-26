"""Local uv commands and hosted CI use the same Python minor version."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


def test_local_python_version_matches_every_ci_job() -> None:
    pinned = (ROOT / ".python-version").read_text().strip()
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    versions = [
        (name, step["with"]["python-version"])
        for name, job in workflow["jobs"].items()
        for step in job.get("steps", [])
        if "python-version" in step.get("with", {})
    ]
    assert versions, "CI must declare its Python version"
    assert {version for _, version in versions} == {pinned}, f".python-version={pinned}, CI={versions}"

    filter_step = next(step for step in workflow["jobs"]["changes"]["steps"] if step.get("id") == "filter")
    filters = yaml.safe_load(filter_step["with"]["filters"])
    assert ".python-version" in filters["python"], "Python pin changes must run the parity check in CI"
