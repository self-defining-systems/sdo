"""Restore reads the accepted source, never the legacy rendered lock a seed may still carry."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from benchmarks.sregym.fastloop.accepted_source import (
    CLUSTER_SCOPED_KINDS,
    AcceptedSource,
    manifest_paths_for,
)
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    accepted_restore_baselines,
    declared_source_identities,
    save_application_baseline,
)
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
    assert [(v["kind"], v["metadata"].get("namespace")) for v in source.manifests] == [
        ("Deployment", "app"),
        ("ClusterRole", None),
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


def test_invalid_selection_and_namespace_are_rejected(tmp_path: Path) -> None:
    for kwargs in ({"namespace": ""}, {"manifest_paths": ()}, {"manifest_paths": ("../outside",)}):
        with pytest.raises(ValueError, match="accepted"):
            AcceptedSource(tmp_path, **{"namespace": "app", **kwargs})


def test_cluster_scoped_kinds_match_the_go_renderer() -> None:
    text = (ROOT / "sdo/operator/rendered/scope.go").read_text()
    go_kinds = set(re.findall(r'"([A-Za-z]+)"', re.search(r"case (.*?):\n\t\treturn true", text, re.DOTALL).group(1)))
    assert go_kinds == CLUSTER_SCOPED_KINDS


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
