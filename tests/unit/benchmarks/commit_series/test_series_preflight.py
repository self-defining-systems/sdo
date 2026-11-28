"""The offline preflight finds what a live series attempt would find one release at a time."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.commit_series import preflight

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def deployment(name: str, image: str, init: str = "") -> dict[str, Any]:
    spec: dict[str, Any] = {"containers": [{"image": image}]}
    if init:
        spec["initContainers"] = [{"image": init}]
    return {"kind": "Deployment", "metadata": {"name": name}, "spec": {"template": {"spec": spec}}}


def job(name: str) -> dict[str, Any]:
    return {
        "kind": "Job",
        "metadata": {"name": name},
        "spec": {"template": {"spec": {"containers": [{"image": "j:1"}]}}},
    }


def test_a_release_without_its_migration_job_is_found() -> None:
    assert preflight.migration_problems([deployment("web", "w:1")]) == ["the render has no migration Job"]
    assert preflight.migration_problems([job("gitlab-migrations-1a2b3c4")]) == []


def test_images_cover_init_containers_and_every_pod_kind() -> None:
    cron = {
        "kind": "CronJob",
        "metadata": {"name": "c"},
        "spec": {"jobTemplate": {"spec": {"template": {"spec": {"containers": [{"image": "c:1"}]}}}}},
    }
    assert preflight.images([deployment("web", "w:1", "i:1"), job("j"), cron]) == {"w:1", "i:1", "j:1", "c:1"}


def test_an_image_that_does_not_resolve_is_named_and_a_tag_with_a_digest_is_asked_by_digest() -> None:
    asked: list[str] = []

    def inspect(ref: str) -> bool:
        asked.append(preflight.pullable(ref))
        return "gone" not in ref

    assert preflight.missing_images({"a:1", "gone:2"}, inspect) == ["gone:2"]
    assert preflight.pullable("postgres:17@sha256:abc") == "postgres@sha256:abc"
    assert preflight.pullable("registry.io:5000/x/y:v1") == "registry.io:5000/x/y:v1"


def test_a_dry_run_failure_is_a_problem_but_an_immutable_job_is_a_note(monkeypatch: pytest.MonkeyPatch) -> None:
    stderr = (
        'The Job "buckets" is invalid: spec.template: field is immutable\n'
        "Error from server (Conflict): conflicts\n"
        'error: unable to recognize "STDIN": no matches for kind "Widget"\n'
    )
    monkeypatch.setattr(preflight.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", stderr))
    problems, notes = preflight.dry_run(__import__("pathlib").Path("/k"), "fg1", "x")
    assert problems == ['error: unable to recognize "STDIN": no matches for kind "Widget"']
    assert notes == ['The Job "buckets" is invalid: spec.template: field is immutable']


def test_the_replayer_pushes_every_commit_onto_the_baselines_memory(tmp_path: Path) -> None:
    from benchmarks.commit_series.gitlab import derive_baseline
    from tests.unit.benchmarks.commit_series.test_gitlab import _bundle_baseline, vcs

    web = "kind: Deployment\nmetadata:\n  name: web\nspec:\n  image: v1\n"
    tail = "kind: Service\nmetadata:\n  name: web\n"
    repo = tmp_path / "upstream"
    repo.mkdir()
    vcs(repo, "init", "-q", "-b", "trunk")

    def commit(render: str) -> str:
        (repo / "k8s").mkdir(exist_ok=True)
        (repo / "k8s" / "render.yaml").write_text(render)
        vcs(repo, "add", "-A")
        vcs(repo, "commit", "-q", "-m", "c")
        return vcs(repo, "rev-parse", "HEAD")

    base = commit("---\n".join([web, tail]))
    one = commit("---\n".join([web.replace("v1", "v2"), tail]))
    spec = {
        "name": "demo",
        "description": "d",
        "source": "s",
        "upstream": {"repo": str(repo), "base": base, "commits": [one]},
    }
    (tmp_path / "series.json").write_text(__import__("json").dumps(spec))
    source = _bundle_baseline(tmp_path, "---\n".join([web, tail]), '{"images": []}')
    derive_baseline(source, tmp_path / "derived", tmp_path / "series.json")
    series = preflight.Series.load(tmp_path / "series.json")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    assert preflight.replay_problems(series, tmp_path / "derived", scratch) == {1: []}
    report = preflight.run(series, tmp_path / "derived", check_images=False)
    assert [r for r in report.rows if r[2]] == [("#1", "migrate", ["the render has no migration Job"])]
    assert yaml  # the module is the one the preflight reads renders with
