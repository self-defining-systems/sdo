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


def vcs(path: Path, *args: str) -> str:
    base = ["git", "-c", "user.name=t", "-c", "user.email=t@invalid", "-C", str(path)]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def fake_render(release: Release, _archive: Path, _pack: Path) -> dict[str, str]:
    """A synthetic render: the web image moves with each app version, the chart label with every release."""
    body = (
        f"kind: Deployment\nmetadata:\n  name: web\nspec:\n  image: {release.app_version}\n---\n"
        f"kind: Service\nmetadata:\n  name: web\n  labels:\n    chart: {release.version}\n"
        "---\nkind: ConfigMap\nmetadata:\n  name: static\n"
    )
    return {gitlab.RENDER_FILE: body}


def test_plain_render_drops_hooks_jobs_and_namespaces() -> None:
    rendered = (
        "kind: Service\nmetadata:\n  name: a\n  namespace: fg1\n---\n"
        "kind: Job\nmetadata:\n  name: j\n---\n"
        "kind: ConfigMap\nmetadata:\n  name: h\n  annotations:\n    helm.sh/hook: pre-install\n"
    )
    assert gitlab.plain_render(rendered) == "kind: Service\nmetadata:\n  name: a\n"


def test_derive_changes_counts_images_values_and_workloads() -> None:
    def deployment(name: str, image: str) -> dict[str, object]:
        return {
            "kind": "Deployment",
            "metadata": {"name": name},
            "spec": {"template": {"spec": {"containers": [{"image": image}]}}},
        }

    before = [deployment("web", "web:1"), deployment("old", "old:1")]
    after = [deployment("web", "web:2"), deployment("new", "new:1"), {"kind": "Service"}]
    got = gitlab.derive_changes(before, after, {"a", "a.b"}, {"a", "c"}, schema=True)
    assert got == {
        "images_changed": 2,
        "values_keys_added": 1,
        "values_keys_removed": 1,
        "values_schema": True,
        "workloads_added": ["new"],
        "workloads_removed": ["old"],
        "objects": 3,
    }


def test_the_release_lists_are_real_releases_in_order() -> None:
    nxt = load_releases(gitlab.NEXT_PATH)
    assert nxt.releases[0].version == "10.5.4"
    assert [r.version for r in nxt.releases[1:]] == ["10.5.5", "10.5.7", "10.6.0", "10.6.1"]
    assert any(r.changes and r.changes.images_changed for r in nxt.releases[1:])
    assert any(r.changes and r.changes.values_keys_added for r in nxt.releases[1:])
    assert load_releases().releases[0].version == "10.4.2"


def test_the_series_has_one_commit_per_release_after_the_base_and_replays_in_order(tmp_path: Path) -> None:
    releases = load_releases(gitlab.NEXT_PATH).releases
    spec = gitlab.build(
        tmp_path / "s", tmp_path / "cache", tmp_path, render=fake_render, fetch_archive=lambda _r, _c: tmp_path
    )
    series = Series.load(spec)
    assert series.name == "gitlab-chart-10-5-4"
    assert len(series.commits) == len(releases) - 1
    assert "gitlab.com/gitlab-org/charts/gitlab" in series.source
    remote = tmp_path / "remote.git"
    vcs(tmp_path, "init", "-q", "--bare", "-b", "sdo-source", str(remote))
    vcs(tmp_path / "s" / "upstream", "push", "-q", str(remote), f"{series.base}:refs/heads/sdo-source")
    for index in range(1, len(series.commits) + 1):
        result = replay(series.config(index, str(remote), "sdo-source"))
        assert [r.status for r in result.records] == ["pushed"]
    head = vcs(remote, "show", f"sdo-source:{gitlab.RENDER_FILE}")
    assert releases[-1].app_version in head
    assert f"chart: {releases[-1].version}" in head
