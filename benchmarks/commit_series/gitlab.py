"""The GitLab series: successive releases of the upstream GitLab Helm chart, one commit each.

Where it comes from: ``apps/gitlab/releases.json`` lists the real releases of the upstream chart
(https://gitlab.com/gitlab-org/charts/gitlab) around the pinned one: 10.4.2 (the base), then 10.5.0, 10.5.1, 10.5.3
and 10.5.4, each with its archive digest and what it changes in the rendered output (images, values keys, workloads).
Why it is realistic: this is what an operator of a GitLab deployment actually has to absorb over time. A minor release
moves every CNG image and adds values; patch releases move images again; the last one is a chart-only release that
changes a label. Nothing is invented: each commit's tree is ``helm template`` of the lane's umbrella chart with that
release (``apps/gitlab/chart`` plus the upstream archive, the same render ``releases.json`` was measured on), split into
one file per object under ``k8s/`` so a commit's diff is exactly the objects the release changed.

The commits are rendered manifests because that is what the fuzzops GitLab pack's SDO repository holds; a lane whose
repository holds the chart itself would replay the vendored-chart bump instead (the fastloop GitLab replay does).
The series is built at run time from the cached or downloaded archives (digest checked); nothing is committed.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from apps.gitlab import assemble
from benchmarks.sregym.fastloop.gitlab_releases import Release, load_releases

if TYPE_CHECKING:
    from collections.abc import Callable

_IDENT = ("-c", "user.name=gitlab-chart-release", "-c", "user.email=chart-release@invalid")
NAMESPACE = "gitlab"


def split_objects(rendered: str) -> dict[str, str]:
    """``helm template`` output as ``k8s/<kind>-<name>.yaml`` -> the object's document (one file per object)."""
    out: dict[str, str] = {}
    for document in rendered.split("\n---\n"):
        text = document.strip("\n")
        if not text.strip():
            continue
        body = "\n".join(line for line in text.splitlines() if not line.startswith("# Source:"))
        obj = yaml.safe_load(body)
        if not isinstance(obj, dict) or "kind" not in obj:
            continue
        name = f"k8s/{str(obj['kind']).lower()}-{obj['metadata']['name']}.yaml"
        if name in out:
            raise ValueError(f"two objects render to {name}")
        out[name] = body + "\n"
    return out


def render_release(release: Release, archive: Path) -> dict[str, str]:
    """The release's objects: the umbrella chart assembled at the release, rendered with the lane's kind values."""
    pins = release.pins(assemble.load_pins())
    with tempfile.TemporaryDirectory(prefix="gitlab-series-") as scratch:
        workspace = assemble.assemble(Path(scratch) / "ws", archive=archive, pins=pins)
        done = subprocess.run(
            ["helm", "template", "gitlab", str(workspace / "chart"), "--namespace", NAMESPACE]
            + [arg for value in assemble.VALUE_FILES for arg in ("-f", str(workspace / value))],
            capture_output=True,
            text=True,
            check=True,
        )
    return split_objects(done.stdout)


def fetch(release: Release, cache: Path) -> Path:
    """The release's archive, from ``cache`` or downloaded, digest checked."""
    target = cache / f"gitlab-{release.version}.tgz"
    if not target.is_file():
        pins = release.pins(assemble.load_pins())
        cache.mkdir(parents=True, exist_ok=True)
        target = assemble.fetch_chart(pins, cache)
    assemble.verify_archive(target, release.sha256)
    return target


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *_IDENT, "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def _write_tree(repo: Path, objects: dict[str, str]) -> None:
    for old in (repo / "k8s").glob("*.yaml") if (repo / "k8s").is_dir() else ():
        if f"k8s/{old.name}" not in objects:
            old.unlink()
    for name, text in objects.items():
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)


def build(
    dest: Path,
    cache: Path,
    *,
    limit: int | None = None,
    render: Callable[[Release, Path], dict[str, str]] = render_release,
    fetch_archive: Callable[[Release, Path], Path] = fetch,
) -> Path:
    """Create ``dest`` (the history) with the base release's tree and one commit per later release; write the spec.

    ``limit`` keeps the first N commits after the base. Returns ``dest/series.json``.
    """
    releases = load_releases()
    base, later = releases.releases[0], releases.releases[1:]
    later = later if limit is None else later[:limit]
    repo = dest / "upstream"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "trunk")
    _write_tree(repo, render(base, fetch_archive(base, cache)))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"chore(deps): gitlab chart {base.version} (app {base.app_version}), the base")
    base_sha = _git(repo, "rev-parse", "HEAD")
    commits = []
    for release in later:
        _write_tree(repo, render(release, fetch_archive(release, cache)))
        _git(repo, "add", "-A")
        _git(
            repo,
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            f"chore(deps): bump gitlab chart to {release.version} (app {release.app_version})\n\n{release.note}",
        )
        commits.append(_git(repo, "rev-parse", "HEAD"))
    spec = {
        "name": "gitlab-chart-10-5",
        "description": (
            f"The upstream GitLab chart releases after {base.version}: "
            + ", ".join(r.version for r in later)
            + ". Images move on the minor release and on patches; the last is chart-only."
        ),
        "source": "https://gitlab.com/gitlab-org/charts/gitlab releases, with digests in apps/gitlab/releases.json",
        "upstream": {"repo": str(repo), "base": base_sha, "commits": commits},
        "mode": "diff",
        "on_conflict": "stop",
    }
    path = dest / "series.json"
    path.write_text(json.dumps(spec, indent=1) + "\n")
    return path
