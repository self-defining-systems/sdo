"""Restore reads the accepted source, never the legacy rendered lock a seed may still carry."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from benchmarks.sregym.fastloop.accepted_source import (
    AcceptedSource,
    manifest_paths_for,
    manifest_paths_for_environment,
)
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    accepted_restore_baselines,
    declared_source_identities,
    save_application_baseline,
)
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import REAL_RUN, commit_accepted_source

ROOT = Path(__file__).resolve().parents[5]
BENCHMARK = ROOT / "benchmarks/sregym"


def _config(conf: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "media-mongodb", "namespace": "app"},
        "data": {"mongod.conf": conf},
    }


def _sealed(path: Path, objects: list[dict[str, Any]]) -> tuple[ApplicationBaseline, ...]:
    from benchmarks.sregym.fastloop.baseline_recovery import load_application_baseline

    path.write_text("{}")
    save_application_baseline(path, (ApplicationBaseline("app", objects),), installed=True)
    baselines = load_application_baseline(path, "app")
    assert baselines is not None
    return baselines


def _write_legacy_lock(workspace: Path, manifests: list[dict[str, Any]]) -> None:
    """A lock as an old seed carries it: rows backed by hash-named files rendered from older source."""
    import hashlib

    rendered = workspace / ".sdo/rendered"
    rendered.mkdir(parents=True)
    rows = []
    for index, manifest in enumerate(manifests):
        payload = yaml.safe_dump(manifest).encode()
        (rendered / f"{index}.yaml").write_bytes(payload)
        ref = {
            "api_version": manifest["apiVersion"],
            "kind": manifest["kind"],
            "namespace": manifest["metadata"]["namespace"],
            "name": manifest["metadata"]["name"],
        }
        rows.append({"ref": ref, "file": f"{index}.yaml", "sha256": hashlib.sha256(payload).hexdigest()})
    (rendered / "lock.json").write_text(json.dumps({"manifest_paths": ["kubernetes"], "objects": rows}))


def test_seed_whose_source_changed_a_configmap_after_its_lock_restores_the_source_content(tmp_path: Path) -> None:
    """Social seed 77faced: source bounded mongod.conf, the lock written earlier still held the unbounded one."""
    workspace = tmp_path / "workspace"
    old, bounded = (
        "storage:\n  wiredTiger: {}\n",
        "storage:\n  wiredTiger:\n    engineConfig:\n      cacheSizeGB: 0.25\n",
    )
    _write_legacy_lock(workspace, [_config(old)])
    source = commit_accepted_source(workspace, [_config(bounded)])

    restored = accepted_restore_baselines(source, _sealed(tmp_path / "baseline.json", [_config(bounded)]))

    assert restored[0].objects == [_config(bounded)]
    # A capture that predates the source change is brought up to the source, not to the lock.
    stale = accepted_restore_baselines(source, _sealed(tmp_path / "stale.json", [_config(old)]))
    assert stale[0].objects[0]["data"] == {"mongod.conf": bounded}


def test_declared_identities_come_from_source_not_from_a_stale_lock(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _write_legacy_lock(workspace, [_config("old")])
    added = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "added", "namespace": "app"}}
    source = commit_accepted_source(workspace, [_config("new"), added])
    assert declared_source_identities(source) == {
        "app": frozenset({("ConfigMap", "media-mongodb"), ("Service", "added")})
    }


def test_render_defaults_namespace_drops_secrets_and_rejects_duplicates(tmp_path: Path) -> None:
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "api"}}
    secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "token"}, "stringData": {"k": "synthetic"}}
    role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "r", "namespace": "x"},
    }
    source = commit_accepted_source(tmp_path / "ok", [deployment, secret, role], namespace="app")
    # The owning production renderer excludes explicitly foreign namespace input.
    assert [(v["kind"], v["metadata"].get("namespace")) for v in source.manifests] == [
        ("Deployment", "app"),
    ]
    with pytest.raises(ValueError, match="duplicate accepted manifest"):
        _ = commit_accepted_source(tmp_path / "dup", [deployment, deployment], namespace="app").manifests


def test_manifest_selection_reads_only_the_selected_files(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    mine = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "mine"}}
    other = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "other"}}
    commit_accepted_source(workspace, [mine], path="kubernetes/astronomy-shop.yaml")
    commit_accepted_source(workspace, [other], path="kubernetes/opentelemetry-demo.yaml")
    selected = AcceptedSource(workspace, "app", manifest_paths_for("opentelemetry-demo-astronomy-shop"))
    assert [v["metadata"]["name"] for v in selected.manifests] == ["mine"]
    assert [v["metadata"]["name"] for v in AcceptedSource(workspace, "app").manifests] == ["mine", "other"]


def _astronomy_environment(tmp_path: Path) -> FastloopEnvironment:
    pdb = {"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": {"name": "opensearch-pdb"}}
    workspace = tmp_path / "workspace"
    commit_accepted_source(workspace, [pdb], path="kubernetes/astronomy-shop.yaml")
    commit_accepted_source(workspace, [pdb], path="kubernetes/opentelemetry-demo.yaml")
    return FastloopEnvironment(
        run_dir=tmp_path / "run",
        cluster="synthetic",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="astronomy-shop",
        application="opentelemetry-demo-astronomy-shop",
        workspace=workspace,
        sregym_dir=tmp_path / "harness",
        private_tmp=None,
    )


def test_shared_attachment_keeps_the_problem_applications_manifests(tmp_path: Path) -> None:
    environment = _astronomy_environment(tmp_path)
    # Shared attach renames the application exactly like shared_memory.attach_shared_memory does.
    environment.parallel_lane = "c4-as-0"
    environment.application = f"{environment.application}-{environment.parallel_lane}"[:30].rstrip("-") + "-" + "a" * 32
    source = AcceptedSource.of(environment)
    assert source.manifest_paths == ("kubernetes/astronomy-shop.yaml",)
    assert [v["metadata"]["name"] for v in source.manifests] == ["opensearch-pdb"]
    assert manifest_paths_for_environment(environment) == source.manifest_paths


def test_attached_environment_without_a_problem_application_raises(tmp_path: Path) -> None:
    environment = _astronomy_environment(tmp_path)
    environment.problem_application = ""
    environment.parallel_lane = "c4-as-0"
    environment.application = "opentelemetry-demo-ast-" + "a" * 32
    with pytest.raises(ValueError, match="problem application"):
        AcceptedSource.of(environment)
    with pytest.raises(ValueError, match="problem application"):
        manifest_paths_for_environment(environment)


def test_invalid_selection_and_namespace_are_rejected(tmp_path: Path) -> None:
    for kwargs in ({"namespace": ""}, {"manifest_paths": ()}, {"manifest_paths": ("../outside",)}):
        with pytest.raises(ValueError, match="accepted"):
            AcceptedSource(tmp_path, **{"namespace": "app", **kwargs})


def test_no_eval_path_reads_the_legacy_rendered_lock() -> None:
    """The lock is an old-seed leftover; any reader of it can disagree with the accepted source."""
    readers = [
        path.relative_to(ROOT).as_posix()
        for path in sorted(BENCHMARK.rglob("*.py"))
        if "rendered/lock" in path.read_text() or "lock.json" in path.read_text()
    ]
    assert readers == []


def test_revision_is_the_checked_out_accepted_commit(tmp_path: Path) -> None:
    source = commit_accepted_source(tmp_path / "workspace", [_config("x")])
    head = REAL_RUN(
        ["git", "-C", str(source.workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert source.revision == head


@pytest.mark.go_integration
def test_file_generated_configmap_restore_uses_production_source_renderer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operator_binary: Path
) -> None:
    workspace = tmp_path / "workspace"
    deployment = workspace / "kubernetes"
    deployment.mkdir(parents=True)
    (deployment / "kustomization.yaml").write_text(
        "configMapGenerator:\n- name: startup\n  files: [init.sh]\n"
        "secretGenerator:\n- name: credential\n  files: [private/password]\n"
        "generatorOptions: {disableNameSuffixHash: true}\n"
    )
    (deployment / "init.sh").write_text("echo accepted\n")
    source = commit_accepted_source(workspace)
    # The missing Secret input is intentional: it must never be read to render
    # public generated configuration, even when accepted source declares it.
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    (deployment / "init.sh").write_text("unaccepted edits\n")
    assert source.manifests == (
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "startup", "namespace": "app"},
            "data": {"init.sh": "echo accepted\n"},
        },
    )
    captured = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "startup", "namespace": "app"},
        "data": {"init.sh": "stale"},
    }
    restored = accepted_restore_baselines(source, (ApplicationBaseline("app", [captured]),))
    assert restored[0].objects[0]["data"]["init.sh"] == "echo accepted\n"
    assert "credential" not in json.dumps(restored, default=str)


@pytest.mark.go_integration
@pytest.mark.requires_sregym
def test_hotel_bootstrap_source_matches_harness_and_remains_restorable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operator_binary: Path
) -> None:
    """Pinned hotel source -> real harness staging -> DEP1's Git renderer."""
    import shutil

    monkeypatch.syspath_prepend(str(ROOT / "third_party/sregym"))

    app = ROOT / "third_party/sregym/SREGym-applications/hotelReservation"
    workspace = tmp_path / "hotel"
    shutil.copytree(app / "kubernetes", workspace / "kubernetes")
    source = commit_accepted_source(workspace, namespace="hotel-reservation")
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    deployed = list(source.manifests)
    expected = {obj["metadata"]["name"]: obj for obj in deployed if obj["kind"] == "ConfigMap"}
    accepted = {obj["metadata"]["name"]: obj for obj in source.manifests if obj["kind"] == "ConfigMap"}
    for component in ("geo", "rate"):
        name, key = f"mongo-{component}-script", f"k8s-{component}-mongo.sh"
        assert (
            accepted[name]["data"]
            == expected[name]["data"]
            == {key: (workspace / "kubernetes/scripts" / key).read_text()}
        )
        assert accepted[name]["metadata"]["namespace"] == "hotel-reservation"
        # Deleting live state does not delete the accepted declaration used by
        # CreateFromSource. Nor can an unaccepted local script change it.
        remaining_live = [obj for obj in deployed if obj["metadata"]["name"] != name]
        assert not any(obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == name for obj in remaining_live)
        (workspace / "kubernetes/scripts" / key).write_text("echo unaccepted\n")
        reread = AcceptedSource(workspace, "hotel-reservation")
        restored = next(
            obj for obj in reread.manifests if obj["kind"] == "ConfigMap" and obj["metadata"]["name"] == name
        )
        assert restored == accepted[name]


def test_plain_manifests_also_use_the_production_renderer(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import accepted_source

    source = commit_accepted_source(tmp_path / "source", [_config("input")])
    rendered = [_config("production render")]
    requests = []

    def render(command, args, **kwargs):
        requests.append((command, json.loads(kwargs["input_text"])))
        return json.dumps(rendered)

    monkeypatch.setattr(accepted_source, "execute_operator", render)
    assert source.manifests == tuple(rendered)
    assert requests == [
        (
            "render-source",
            {
                "repository": str(source.workspace.resolve()),
                "revision": source.revision,
                "namespace": "app",
                "manifest_paths": ["kubernetes"],
            },
        )
    ]


def test_no_caller_keys_manifests_by_the_renamed_application() -> None:
    offenders = [
        f"{path.relative_to(ROOT)}:{node.lineno}"
        for path in (BENCHMARK / "fastloop").rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "manifest_paths_for"
        and any(isinstance(a, ast.Attribute) and a.attr == "application" for a in node.args)
    ]
    assert offenders == []
