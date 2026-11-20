"""The GitLab series: one commit per upstream chart release after the base, a diff of exactly what the release moves."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.commit_series import gitlab
from benchmarks.common.replay.replay import replay
from benchmarks.common.replay.series import Series
from benchmarks.sregym.fastloop.gitlab_releases import load_releases

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.gitlab_releases import Release


def git(path: Path, *args: str) -> str:
    base = ["git", "-c", "user.name=t", "-c", "user.email=t@invalid", "-C", str(path)]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def fake_render(release: Release, _archive: Path) -> dict[str, str]:
    """Synthetic objects: the web image moves with every release, the chart label only with the last."""
    last = load_releases().releases[-1].version
    label = "chart-last" if release.version == last else "chart"
    return {
        "k8s/deployment-web.yaml": f"kind: Deployment\nmetadata:\n  name: web\nspec:\n  image: {release.app_version}\n",
        "k8s/service-web.yaml": f"kind: Service\nmetadata:\n  name: web\n  labels: {{chart: {label}}}\n",
        "k8s/configmap-static.yaml": "kind: ConfigMap\nmetadata:\n  name: static\n",
    }


def test_split_objects_names_one_file_per_object_and_drops_source_comments() -> None:
    rendered = (
        "---\n# Source: x/a.yaml\nkind: Service\nmetadata:\n  name: a\n"
        "---\n# Source: x/b.yaml\nkind: Deployment\nmetadata:\n  name: b\n"
    )
    assert sorted(gitlab.split_objects(rendered)) == ["k8s/deployment-b.yaml", "k8s/service-a.yaml"]
    assert "# Source" not in gitlab.split_objects(rendered)["k8s/service-a.yaml"]


def test_the_series_has_one_commit_per_release_after_the_base_and_replays_in_order(tmp_path: Path) -> None:
    releases = load_releases().releases
    spec = gitlab.build(tmp_path / "s", tmp_path / "cache", render=fake_render, fetch_archive=lambda _r, _c: tmp_path)
    series = Series.load(spec)
    assert len(series.commits) == len(releases) - 1
    assert "gitlab.com/gitlab-org/charts/gitlab" in series.source
    # a commit's diff is exactly what the release moved: the static object never appears in one
    for sha in series.commits[:-1]:
        changed = git(tmp_path / "s" / "upstream", "diff", "--name-only", f"{sha}^", sha).splitlines()
        assert "k8s/deployment-web.yaml" in changed
        assert "k8s/configmap-static.yaml" not in changed
    # the last release is chart-only: the same app version, only the chart label moves
    last = git(tmp_path / "s" / "upstream", "diff", "--name-only", f"{series.commits[-1]}^", series.commits[-1])
    assert last.splitlines() == ["k8s/service-web.yaml"]
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "sdo-source", str(remote))
    git(tmp_path / "s" / "upstream", "push", "-q", str(remote), f"{series.base}:refs/heads/sdo-source")
    for index in range(1, len(series.commits) + 1):
        result = replay(series.config(index, str(remote), "sdo-source"))
        assert [r.status for r in result.records] == ["pushed"]
    head = git(remote, "show", "sdo-source:k8s/deployment-web.yaml")
    assert releases[-1].app_version in head
