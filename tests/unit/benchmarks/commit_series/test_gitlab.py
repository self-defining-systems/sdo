"""The GitLab series: one commit per upstream chart release after the base, a diff of exactly what the release moves."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import yaml

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


def fake_render(release: Release, _archive: Path, _pack: Path, *, migrations: bool = False) -> dict[str, str]:
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


def test_a_release_carries_the_migration_job_its_upgrade_needs_and_the_base_does_not() -> None:
    """The fuzzops pack's baseline holds no Jobs (its up script ran the base's migrations), but a release's pods wait
    in their init container for the new schema: without the (non-hook) migration Job the deploy never rolls out."""
    rendered = (
        "kind: Service\nmetadata:\n  name: a\n  namespace: fg1\n---\n"
        "kind: Job\nmetadata:\n  name: migrations-1\n  namespace: fg1\n---\n"
        "kind: Job\nmetadata:\n  name: hooked\n  annotations:\n    helm.sh/hook: pre-install\n"
    )
    assert "migrations-1" not in gitlab.plain_render(rendered)
    upgrade = gitlab.plain_render(rendered, migrations=True)
    assert "migrations-1" in upgrade
    assert "hooked" not in upgrade
    assert "namespace" not in upgrade


def _bundle_baseline(root: Path, render: str, plan: str) -> Path:
    """A synthetic baseline directory: a memory repository (render and plan) bundled on branch ``mem``."""
    memory = root / "memory-src"
    (memory / "k8s").mkdir(parents=True)
    (memory / ".sdo" / "deployment").mkdir(parents=True)
    (memory / "k8s" / "render.yaml").write_text(render)
    (memory / ".sdo" / "deployment" / "plan.yaml").write_text(plan)
    vcs(memory, "init", "-q", "-b", "mem")
    vcs(memory, "add", "-A")
    vcs(memory, "commit", "-q", "-m", "memory")
    out = root / "baseline"
    out.mkdir()
    vcs(memory, "bundle", "create", str(out / "memory.bundle"), "mem")
    (out / "baseline.json").write_text(
        json.dumps({"bundle": "memory.bundle", "branch": "mem", "commit": vcs(memory, "rev-parse", "HEAD")})
    )
    return out


def test_stable_job_ids_star_a_generated_suffix_and_keep_a_plain_name() -> None:
    assert gitlab.stable_job_id("gitlab-migrations-147f0eb") == "Job/fg1/gitlab-migrations-*"
    assert gitlab.stable_job_id("gitlab-objectstore-buckets") == "Job/fg1/gitlab-objectstore-buckets"
    assert gitlab.stable_job_id("db-migrate") == "Job/fg1/db-migrate"  # a word with no digit is no hash


def test_a_series_baseline_carries_the_base_jobs_and_names_every_job_of_the_series(tmp_path: Path) -> None:
    web = "kind: Deployment\nmetadata:\n  name: web\nspec:\n  image: v1\n"
    job = "kind: Job\nmetadata:\n  name: db-migrations-1a2b3c4\nspec:\n  image: v1\n"
    tail = "kind: Service\nmetadata:\n  name: web\n"
    repo = tmp_path / "upstream"
    repo.mkdir()
    vcs(repo, "init", "-q", "-b", "trunk")

    def commit(render: str, message: str) -> str:
        (repo / "k8s").mkdir(exist_ok=True)
        (repo / "k8s" / "render.yaml").write_text(render)
        vcs(repo, "add", "-A")
        vcs(repo, "commit", "-q", "-m", message)
        return vcs(repo, "rev-parse", "HEAD")

    base = commit("---\n".join([web, job, tail]), "base")
    release = commit("---\n".join([web.replace("v1", "v2"), job.replace("1a2b3c4", "5d6e7f8"), tail]), "release")
    series = tmp_path / "series.json"
    series.write_text(json.dumps({"upstream": {"repo": str(repo), "base": base, "commits": [release]}}))
    # the baseline's render is the pack's: the same objects, and no Job
    source = _bundle_baseline(
        tmp_path,
        "---\n".join([web, tail + "  annotations:\n    note: instrumented\n"]),
        json.dumps({"expectedWorkloadIds": ["Deployment/fg1/web"]}),
    )

    head = gitlab.derive_baseline(source, tmp_path / "derived", series)

    derived = json.loads((tmp_path / "derived" / "baseline.json").read_text())
    assert derived["commit"] == head
    clone = tmp_path / "clone"
    vcs(tmp_path, "clone", "-q", "-b", "mem", str(tmp_path / "derived" / "memory.bundle"), str(clone))
    assert vcs(clone, "rev-parse", "HEAD") == head
    kinds = [d["kind"] for d in yaml.safe_load_all((clone / "k8s" / "render.yaml").read_text())]
    assert kinds == ["Deployment", "Job", "Service"]  # the Job sits where it does in the release's render
    assert "note: instrumented" in (clone / "k8s" / "render.yaml").read_text()  # what the memory had is kept
    plan = json.loads((clone / ".sdo" / "deployment" / "plan.yaml").read_text())
    assert plan["expectedWorkloadIds"] == ["Deployment/fg1/web", "Job/fg1/db-migrations-*"]
    # the release applies onto the derived memory as an engineer's diff does
    vcs(clone, "checkout", "-q", "-b", "replay")
    vcs(clone, "fetch", "-q", str(repo), release)
    vcs(clone, "cherry-pick", release)
