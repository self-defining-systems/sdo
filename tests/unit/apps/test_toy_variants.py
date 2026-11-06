"""Toy deployment variants render the same Kubernetes objects as the raw manifests."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from apps.toy.variants.assemble import VARIANTS, assemble

TOY = Path(__file__).resolve().parents[3] / "apps" / "toy"
# Metadata that renderers add (managed-by labels, chart annotations). Pod
# template labels and selectors are not ignored: they are part of the shape.
IGNORED_OBJECT_METADATA = ("labels", "annotations")


def _objects(text: str) -> dict[tuple[str, str], dict[str, Any]]:
    objects: dict[tuple[str, str], dict[str, Any]] = {}
    for doc in yaml.safe_load_all(text):
        if not doc:
            continue
        key = (doc["kind"], doc["metadata"]["name"])
        assert key not in objects, f"duplicate object {key}"
        for field in IGNORED_OBJECT_METADATA:
            doc["metadata"].pop(field, None)
        objects[key] = doc
    return objects


def _raw() -> dict[tuple[str, str], dict[str, Any]]:
    return _objects("\n---\n".join(p.read_text() for p in sorted((TOY / "kubernetes").glob("*.yaml"))))


def _render(tool: str, *args: str) -> str:
    if shutil.which(tool) is None:
        # CI runners ship helm and kubectl; a missing tool there must not pass silently.
        if os.environ.get("CI"):
            pytest.fail(f"{tool} is required in CI to render toy variants")
        pytest.skip(f"{tool} is not installed; the render comparison runs in CI")
    return subprocess.run([tool, *args], check=True, capture_output=True, text=True, timeout=60).stdout


def test_raw_manifests_cover_three_services():
    assert sorted(_raw()) == sorted(
        (kind, name) for name in ("frontend", "backend", "catalog") for kind in ("Deployment", "Service")
    )


def test_kustomize_overlay_renders_the_raw_objects():
    rendered = _render("kubectl", "kustomize", str(TOY / "variants/kustomize/deploy/overlays/default"))
    assert _objects(rendered) == _raw()


def test_helm_chart_with_default_values_renders_the_raw_objects():
    rendered = _render("helm", "template", "toy", str(TOY / "variants/helm/chart"))
    assert _objects(rendered) == _raw()


def test_helm_image_and_replicas_are_values():
    rendered = _objects(
        _render(
            "helm", "template", "toy", str(TOY / "variants/helm/chart"), "--set", "image.tag=v2", "--set", "replicas=3"
        )
    )
    for name in ("frontend", "backend", "catalog"):
        deployment = rendered[("Deployment", name)]["spec"]
        assert deployment["replicas"] == 3
        assert deployment["template"]["spec"]["containers"][0]["image"] == "sdo-toy-app:v2"


def test_variant_trees_carry_no_helm_hooks_or_lookups():
    templates = "".join(p.read_text() for p in (TOY / "variants/helm/chart/templates").iterdir())
    for forbidden in ("helm.sh/hook", "lookup", "randAlpha", "randNumeric", "uuidv4", "genCA"):
        assert forbidden not in templates


def test_compose_variant_has_only_compose_and_matches_services():
    compose = yaml.safe_load((TOY / "variants/compose/docker-compose.yml").read_text())
    assert sorted(compose["services"]) == ["backend", "catalog", "frontend"]
    assert compose["services"]["frontend"]["environment"]["DEPENDENCY"] == "http://backend:8080/"
    assert [p.name for p in (TOY / "variants/compose").iterdir()] == ["docker-compose.yml"]


@pytest.mark.parametrize("variant", VARIANTS)
def test_assembled_source_root_is_standalone(tmp_path, variant):
    root = assemble(variant, tmp_path / "root")
    assert (root / "main.go").is_file()
    assert (root / "Dockerfile").is_file()
    files = {p.relative_to(root).parts[0] for p in root.rglob("*") if p.is_file()}
    expected = {"raw": "kubernetes", "kustomize": "deploy", "helm": "chart", "compose": "docker-compose.yml"}[variant]
    assert expected in files
    others = {"kubernetes", "deploy", "chart", "docker-compose.yml"} - {expected}
    assert not files & others, "a variant root must hold only its own deployment files"


def test_assemble_rejects_unknown_variant_and_nonempty_dest(tmp_path):
    with pytest.raises(ValueError, match="unknown toy variant"):
        assemble("jsonnet", tmp_path / "a")
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "x").write_text("")
    with pytest.raises(ValueError, match="not empty"):
        assemble("raw", tmp_path / "b")
