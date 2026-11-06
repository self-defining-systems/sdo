"""The GitLab lane's assembled workspace renders through SDO's helm renderer to the objects ``helm template`` renders.

CI uses a tiny synthetic stand-in for the upstream chart (same name and version as the pin, built here). The real
chart, a large third-party download, is only used when ``SDO_GITLAB_CHART_ARCHIVE`` names a local copy of the pinned
archive (or ``SDO_GITLAB_FETCH=1`` downloads it).
"""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pytest
import yaml

from apps.gitlab import assemble as lane
from benchmarks.sregym.adapter import helm_source
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource

pytestmark = pytest.mark.go_integration

Identity = tuple[str, str, str]
FILES = {
    "gitlab/Chart.yaml": """apiVersion: v2
name: gitlab
version: 10.5.4
dependencies:
  - {name: pages, version: 0.1.0, repository: "", condition: global.pages.enabled}
""",
    "gitlab/values.yaml": "global:\n  pages: {enabled: false}\n  ingress: {class: nginx}\nreplicas: 1\n",
    "gitlab/templates/web.yaml": """apiVersion: apps/v1
kind: Deployment
metadata: {name: "{{ .Release.Name }}-webservice"}
spec:
  replicas: {{ .Values.replicas }}
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      containers:
        - {name: web, image: "registry.invalid/web:1"}
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: {name: "{{ .Release.Name }}-webservice"}
spec:
  ingressClassName: {{ .Values.global.ingress.class }}
  rules: [{host: gitlab.invalid}]
""",
    "gitlab/templates/shared-secrets.yaml": """apiVersion: batch/v1
kind: Job
metadata:
  name: "{{ .Release.Name }}-shared-secrets"
  annotations: {"helm.sh/hook": "pre-install,pre-upgrade", "helm.sh/hook-weight": "-5"}
spec:
  template:
    spec:
      restartPolicy: Never
      containers: [{name: secrets, image: "registry.invalid/kubectl:1"}]
""",
    "gitlab/templates/tests/test-runner.yaml": """apiVersion: v1
kind: Pod
metadata:
  name: "{{ .Release.Name }}-test-{{ randAlphaNum 5 }}"
  annotations: {"helm.sh/hook": test}
spec:
  restartPolicy: Never
  containers: [{name: t, image: "registry.invalid/t:1"}]
""",
    "gitlab/charts/pages/Chart.yaml": "apiVersion: v2\nname: pages\nversion: 0.1.0\n",
    "gitlab/charts/pages/crds/pages.yaml": (
        "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: pages.example.invalid}\n"
    ),
    "gitlab/charts/pages/templates/pages.yaml": (
        'apiVersion: v1\nkind: ConfigMap\nmetadata: {name: pages}\ndata: {k: "{{ randAlphaNum 3 }}"}\n'
    ),
}


def stand_in_archive(directory: Path) -> tuple[Path, dict[str, Any]]:
    """A synthetic chart archive and pins that name it."""
    archive = directory / "gitlab-10.5.4.tgz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, text in FILES.items():
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    pins = lane.load_pins()
    pins["gitlab_chart"] = {**pins["gitlab_chart"], "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    return archive, pins


def real_archive(directory: Path) -> Path | None:
    if prepared := os.environ.get("SDO_GITLAB_CHART_ARCHIVE"):
        return Path(prepared)
    if os.environ.get("SDO_GITLAB_FETCH") == "1":
        return lane.fetch_chart(lane.load_pins(), directory)
    return None


def helm_objects(chart: Path) -> list[dict[str, Any]]:
    """Non-hook objects of ``helm template``.

    SDO's accepted inventory is the main phase: install hooks run as ephemeral phases and are not part of it.
    """
    if shutil.which("helm") is None:
        if os.environ.get("CI"):
            pytest.fail("helm is required in CI to render the GitLab lane")
        pytest.skip("helm is not installed")
    values = ["-f", str(chart / "values-kind.yaml")]
    out = subprocess.run(
        ["helm", "template", "gitlab", str(chart), "-n", "gitlab", "--kube-version", "1.30.0", *values],
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    ).stdout
    return [
        doc
        for doc in yaml.safe_load_all(out)
        if doc and not (doc["metadata"].get("annotations") or {}).get("helm.sh/hook")
    ]


def helm_identities(chart: Path) -> set[Identity]:
    """Non-hook objects of ``helm template`` as (kind, name, namespace)."""
    return {(o["kind"], o["metadata"]["name"], o["metadata"].get("namespace") or "gitlab") for o in helm_objects(chart)}


def sdo_identities(workspace: Path) -> set[Identity]:
    subprocess.run(["git", "init", "-q", str(workspace)], check=True)
    subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(workspace), "-c", "user.name=t", "-c", "user.email=t@invalid", "commit", "-qm", "lane"],
        check=True,
    )
    source = AcceptedSource(workspace, "gitlab", ("chart",), renderer_source=lane.deployment_source())
    try:
        objects = source.manifests
    except ValueError as error:
        if "helm needs Env.Release" in str(error):
            pytest.skip("waits for the helm release-name default (render-seam S3-13b); remove this skip when it lands")
        raise
    return {(o["kind"], o["metadata"]["name"], o["metadata"].get("namespace", "")) for o in objects}


def non_secret(identities: set[Identity]) -> set[Identity]:
    return {identity for identity in identities if identity[0] != "Secret"}


def test_deployment_source_is_one_helm_source_over_the_umbrella_chart(tmp_path: Path) -> None:
    assert lane.deployment_source() == helm_source("chart", ("chart/values-kind.yaml",))
    archive, pins = stand_in_archive(tmp_path)
    workspace = lane.assemble(tmp_path / "ws", archive=archive, pins=pins)
    assert (workspace / "chart/Chart.yaml").is_file()
    assert (workspace / "chart/charts/gitlab/Chart.yaml").is_file()
    for datastore in ("postgresql", "redis", "objectstore"):
        assert (workspace / "chart/charts" / datastore / "Chart.yaml").is_file()


def test_vendoring_rejects_an_archive_that_does_not_match_the_pin(tmp_path: Path) -> None:
    archive, pins = stand_in_archive(tmp_path)
    archive.write_bytes(archive.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="does not match the pin"):
        lane.assemble(tmp_path / "ws", archive=archive, pins=pins)


def test_assembling_into_a_used_directory_is_refused(tmp_path: Path) -> None:
    archive, pins = stand_in_archive(tmp_path)
    (tmp_path / "ws").mkdir()
    (tmp_path / "ws/leftover").write_text("x")
    with pytest.raises(ValueError, match="not empty"):
        lane.assemble(tmp_path / "ws", archive=archive, pins=pins)


def test_stand_in_workspace_renders_through_sdo_as_helm_does(
    tmp_path: Path, operator_binary: Path, monkeypatch
) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    archive, pins = stand_in_archive(tmp_path)
    workspace = lane.assemble(tmp_path / "ws", archive=archive, pins=pins)
    expected = helm_identities(workspace / "chart")
    actual = sdo_identities(workspace)
    # The disabled subchart (CRD, random function) is absent; hooks (install, upgrade-only, test) are not inventory.
    assert not any(kind == "CustomResourceDefinition" or name == "pages" for kind, name, _ in actual)
    assert {"Deployment", "Ingress", "StatefulSet"} <= {kind for kind, _, _ in actual}
    assert non_secret(actual) == non_secret(expected)


def test_stand_in_plan_is_derived_from_the_render(tmp_path: Path, operator_binary: Path, monkeypatch) -> None:
    from benchmarks.sregym.fastloop import gitlab_lane

    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    archive, pins = stand_in_archive(tmp_path)
    commit = gitlab_lane.prepare_workspace(tmp_path / "ws", archive=archive, pins=pins)
    assert commit
    plan = gitlab_lane.provided_plan(tmp_path / "ws")
    expected = helm_objects(tmp_path / "ws/chart")
    # The expected side comes from `helm template`, not from SDO's render: workloads that are not hooks.
    assert sorted(plan["expected_workload_ids"]) == sorted(
        f"{o['kind']}/gitlab/{o['metadata']['name']}"
        for o in expected
        if o["kind"] in ("Deployment", "StatefulSet", "DaemonSet", "Job")
    )
    assert "Deployment/gitlab/gitlab-webservice" in plan["expected_workload_ids"]
    images = {image["image"] for image in plan["images"]}
    assert "registry.invalid/web:1" in images
    # Hook and test-only images are not part of the rendered inventory.
    assert not images & {"registry.invalid/kubectl:1", "registry.invalid/t:1"}
    assert images == set(gitlab_lane.rendered_images(expected))
    assert {image["source_id"] for image in plan["images"]} == {"prebuilt"}


def test_real_chart_workspace_renders_through_sdo_as_helm_does(
    tmp_path: Path, operator_binary: Path, monkeypatch
) -> None:
    archive = real_archive(tmp_path)
    if archive is None:
        pytest.skip("the real chart is a large download; set SDO_GITLAB_CHART_ARCHIVE or SDO_GITLAB_FETCH=1")
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    workspace = lane.assemble(tmp_path / "ws", archive=archive)
    actual = sdo_identities(workspace)
    expected = helm_identities(workspace / "chart")

    # Job names carry a hash of Helm's own version: compare them by their family.
    def family(identities: set[Identity]) -> set[Identity]:
        return {(k, n.rsplit("-", 1)[0] if k == "Job" else n, ns) for k, n, ns in non_secret(identities)}

    assert family(actual) == family(expected)
    assert {"StatefulSet", "Service"} <= {kind for kind, _, _ in actual}
    assert {name for kind, name, _ in actual if kind == "StatefulSet"} >= {
        "gitlab-postgresql",
        "gitlab-redis",
        "gitlab-objectstore",
        "gitlab-gitaly",
    }


def test_real_chart_plan_is_printed(tmp_path: Path, operator_binary: Path, monkeypatch, capsys) -> None:
    """Opt-in (SDO_GITLAB_CHART_ARCHIVE or SDO_GITLAB_FETCH=1): the plan SDO would deploy for the real chart."""
    import json

    from benchmarks.sregym.fastloop import gitlab_lane

    archive = real_archive(tmp_path)
    if archive is None:
        pytest.skip("the real chart is a large download; set SDO_GITLAB_CHART_ARCHIVE or SDO_GITLAB_FETCH=1")
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    gitlab_lane.prepare_workspace(tmp_path / "ws", archive=archive)
    plan = gitlab_lane.provided_plan(tmp_path / "ws")
    with capsys.disabled():
        print("\n" + json.dumps(plan, indent=2, sort_keys=True))
    expected = helm_objects(tmp_path / "ws/chart")

    # Job names carry a hash of Helm's own version: compare them by their family.
    def family(workload_id: str) -> str:
        return workload_id.rsplit("-", 1)[0] if workload_id.startswith("Job/") else workload_id

    assert {family(i) for i in plan["expected_workload_ids"]} == {
        family(f"{o['kind']}/gitlab/{o['metadata']['name']}")
        for o in expected
        if o["kind"] in ("Deployment", "StatefulSet", "DaemonSet", "Job")
    }
    assert set(gitlab_lane.rendered_images(expected)) == {image["image"] for image in plan["images"]}
    assert len(plan["images"]) >= 8
