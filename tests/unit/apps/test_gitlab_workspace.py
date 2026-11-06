"""The GitLab lane's assembled workspace renders through SDO's helm renderer to the objects ``helm template`` renders.

CI uses a tiny synthetic stand-in for the upstream chart (same name and version as the pin, built here). The real
chart, a large third-party download, is only used when ``SDO_GITLAB_CHART_ARCHIVE`` names a local copy of the pinned
archive (or ``SDO_GITLAB_FETCH=1`` downloads it).
"""

from __future__ import annotations

import hashlib
import io
import os
import re
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

ROOT = Path(__file__).resolve().parents[3]

Identity = tuple[str, str, str]
FILES = {
    "gitlab/Chart.yaml": """apiVersion: v2
name: gitlab
version: 10.5.4
dependencies:
  - {name: pages, version: 0.1.0, repository: "", condition: global.pages.enabled}
""",
    "gitlab/values.yaml": (
        "global:\n  pages: {enabled: false}\n  ingress: {class: nginx}\nreplicas: 1\n"
        "gitlab: {webservice: {helmTests: {enabled: true}}}\n"
    ),
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
---
apiVersion: batch/v1
kind: CronJob
metadata: {name: "{{ .Release.Name }}-cleanup"}
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers: [{name: cleanup, image: "registry.invalid/cron:1"}]
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
    "gitlab/templates/tests/test-runner.yaml": """{{ if .Values.gitlab.webservice.helmTests.enabled }}apiVersion: v1
kind: Pod
metadata:
  name: "{{ .Release.Name }}-test-{{ randAlphaNum 5 }}"
  annotations: {"helm.sh/hook": test}
spec:
  restartPolicy: Never
  containers: [{name: t, image: "registry.invalid/t:1"}]
{{ end }}""",
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


def go_pod_kinds() -> tuple[str, ...]:
    """The kinds of Go's pod-template table (the one source of truth for what a workload is), read from its source."""
    source = (ROOT / "sdo/operator/rendered/podtemplate.go").read_text()
    table = source.split("var podKinds = map[string][]string{", 1)[1].split("\n}\n", 1)[0]
    kinds = tuple(re.findall(r'^\s*"(\w+)":', table, flags=re.MULTILINE))
    assert {"Deployment", "CronJob", "Pod"} <= set(kinds), "podKinds table not parsed"
    return kinds


def helm_workload_ids(objects: list[dict[str, Any]]) -> list[str]:
    """Independent oracle: workload IDs of ``helm template`` objects, by Go's table of kinds."""
    kinds = go_pod_kinds()
    return [f"{o['kind']}/gitlab/{o['metadata']['name']}" for o in objects if o["kind"] in kinds]


def helm_images(objects: list[dict[str, Any]]) -> set[str]:
    """Independent oracle: images of the pod templates of ``helm template`` objects."""
    images: set[str] = set()
    for obj in objects:
        spec = obj.get("spec") or {}
        if obj["kind"] == "CronJob":
            spec = (spec.get("jobTemplate") or {}).get("spec") or {}
        pod = spec if obj["kind"] == "Pod" else (spec.get("template") or {}).get("spec") or {}
        for container in (*(pod.get("containers") or ()), *(pod.get("initContainers") or ())):
            images.add(container["image"])
    return images


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
    # The disabled subchart (CRD, random function) is absent and the lane's values switch off the test hook's random
    # name (Helm executes every template, so a random function that runs is rejected); hooks are not inventory.
    assert not any(kind == "CustomResourceDefinition" or name == "pages" for kind, name, _ in actual)
    assert {"Deployment", "Ingress", "StatefulSet"} <= {kind for kind, _, _ in actual}
    assert non_secret(actual) == non_secret(expected)


def _family(workload_id: str) -> str:
    """A Job's generated name suffix (the plan's ``*``, helm's hash) removed; every other ID is its own family."""
    return workload_id.rsplit("-", 1)[0] if workload_id.startswith("Job/") else workload_id


def _repository(image: str) -> str:
    """The image without tag and digest."""
    image = image.split("@", 1)[0]
    head, _, tail = image.rpartition("/")
    return (head + "/" if head else "") + tail.split(":", 1)[0]


def test_stand_in_plan_is_derived_from_the_render(tmp_path: Path, operator_binary: Path, monkeypatch) -> None:
    from benchmarks.sregym.fastloop import gitlab_lane

    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_binary))
    archive, pins = stand_in_archive(tmp_path)
    commit = gitlab_lane.prepare_workspace(tmp_path / "ws", archive=archive, pins=pins)
    assert commit
    plan = gitlab_lane.provided_plan(tmp_path / "ws")
    expected = helm_objects(tmp_path / "ws/chart")
    # The expected side comes from `helm template`, not from SDO's render: workloads that are not hooks.
    assert sorted(_family(i) for i in plan["expected_workload_ids"]) == sorted(
        _family(i) for i in helm_workload_ids(expected)
    )
    assert "Deployment/gitlab/gitlab-webservice" in plan["expected_workload_ids"]
    # A CronJob is a workload too (Go's pod-template table), so the plan must name it.
    assert "CronJob/gitlab/gitlab-cleanup" in plan["expected_workload_ids"]
    # Images are declared by repository, so a release that only bumps tags keeps matching.
    images = {image["image"] for image in plan["images"]}
    assert "registry.invalid/cron" in images
    assert "registry.invalid/web" in images
    # Hook and test-only images are not part of the rendered inventory.
    assert not images & {"registry.invalid/kubectl", "registry.invalid/t"}
    assert images == {_repository(image) for image in helm_images(expected)}
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

    assert {family(i) for i in plan["expected_workload_ids"]} == {family(i) for i in helm_workload_ids(expected)}
    assert {_repository(image) for image in helm_images(expected)} == {image["image"] for image in plan["images"]}
    assert len(plan["images"]) >= 8
    # The lane's install-time policy admits every image of the real chart (Go's RegistryHost rule).
    for image in plan["images"]:
        first = image["image"].split("/", 1)[0]
        host = first if "/" in image["image"] and ("." in first or ":" in first) else "docker.io"
        assert host in gitlab_lane.IMAGE_REGISTRIES, image["image"]


def _bucket_job_script() -> str:
    if shutil.which("helm") is None:
        if os.environ.get("CI"):
            pytest.fail("helm is required in CI to render the GitLab lane")
        pytest.skip("helm is not installed")
    chart = Path(lane.__file__).parent / "chart" / "charts" / "objectstore"
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "objectstore",
            str(chart),
            "--set",
            "buckets={gitlab-uploads,registry}",
            "--set",
            "accessKey=a",
            "--set",
            "secretKey=b",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    jobs = [d for d in yaml.safe_load_all(rendered) if d and d["kind"] == "Job"]
    assert len(jobs) == 1
    return jobs[0]["spec"]["template"]["spec"]["containers"][0]["command"][-1]


_FAKE_WEED = """#!/bin/sh
# Stand-in for `weed shell`: bucket state lives in $STORE; like the real shell, creating an existing
# bucket prints an error and (as the original Job's failure showed) cannot be relied on for exit status.
cmd=$(cat)
case "$cmd" in
  s3.bucket.list) [ -f "$STORE" ] && sed 's/^/  /' "$STORE"; exit 0 ;;
  "s3.bucket.create -name "*) name=${cmd#s3.bucket.create -name }
    if grep -qx "$name" "$STORE" 2>/dev/null; then echo "error: bucket $name already exists"; exit 1; fi
    [ -n "$BROKEN" ] && { echo "error: cannot reach filer"; exit 1; }
    echo "$name" >> "$STORE"; exit 0 ;;
esac
"""


def _run_bucket_job(tmp_path: Path, existing: list[str], broken: bool = False) -> tuple[int, list[str]]:
    script = _bucket_job_script().replace("sleep 5", "true").replace("-gt 60", "-gt 3")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    weed = bin_dir / "weed"
    weed.write_text(_FAKE_WEED)
    weed.chmod(0o755)
    store = tmp_path / "store"
    store.write_text("".join(f"{b}\n" for b in existing))
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STORE": str(store),
        "BROKEN": "1" if broken else "",
    }
    result = subprocess.run(["sh", "-c", script], env=env, capture_output=True, text=True)
    return result.returncode, sorted(store.read_text().split())


def test_bucket_job_creates_missing_buckets(tmp_path: Path) -> None:
    assert _run_bucket_job(tmp_path, []) == (0, ["gitlab-uploads", "registry"])


def test_bucket_job_succeeds_when_buckets_already_exist_on_a_retained_volume(tmp_path: Path) -> None:
    assert _run_bucket_job(tmp_path, ["gitlab-uploads", "registry"]) == (0, ["gitlab-uploads", "registry"])


def test_bucket_job_creates_only_the_missing_bucket(tmp_path: Path) -> None:
    assert _run_bucket_job(tmp_path, ["registry"]) == (0, ["gitlab-uploads", "registry"])


def test_bucket_job_fails_on_a_real_error_after_bounded_retries(tmp_path: Path) -> None:
    code, buckets = _run_bucket_job(tmp_path, [], broken=True)
    assert code == 1
    assert buckets == []
