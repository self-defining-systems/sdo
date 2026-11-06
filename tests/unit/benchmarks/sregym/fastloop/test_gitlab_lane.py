"""GitLab lane harness: provided deployment source, prerequisites, workspace and install configuration."""

from __future__ import annotations

import hashlib
import io
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from google.protobuf import json_format

from apps.gitlab import assemble
from benchmarks.sregym.fastloop import gitlab_lane
from benchmarks.sregym.fastloop.cli import build_parser
from tests.unit.apps.test_gitlab_workspace import stand_in_archive

ROOT = Path(__file__).resolve().parents[5]


def test_deployment_source_json_is_a_valid_helm_deployment_source() -> None:
    from sdo.contracts.proto import DeploymentSource, deployment_source_pb2

    message = json_format.ParseDict(gitlab_lane.deployment_source(), DeploymentSource())
    assert message.renderer_id == "helm"
    assert message.config.TypeName() == "sdodev.contracts.v1alpha1.HelmRenderConfig"
    config = deployment_source_pb2.HelmRenderConfig()
    assert message.config.Unpack(config)
    assert config.chart_path == "chart"
    assert list(config.value_files) == ["chart/values-kind.yaml"]


def test_workspace_is_one_commit_with_the_umbrella_chart_and_vendored_upstream(tmp_path: Path) -> None:
    archive, pins = stand_in_archive(tmp_path)
    workspace = tmp_path / "ws"
    commit = gitlab_lane.prepare_workspace(workspace, archive=archive, pins=pins)
    tracked = subprocess.run(
        ["git", "-C", str(workspace), "ls-files"], check=True, capture_output=True, text=True
    ).stdout.split()
    assert "chart/Chart.yaml" in tracked
    assert "chart/charts/gitlab/Chart.yaml" in tracked
    assert len(commit) == 40


def test_ingress_manifest_must_match_its_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gitlab_lane.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"tampered"))
    with pytest.raises(ValueError, match="does not match the pin"):
        gitlab_lane.ingress_manifest()
    pins = assemble.load_pins()
    pins["prerequisites"]["ingress_nginx"]["sha256"] = hashlib.sha256(b"ok").hexdigest()
    monkeypatch.setattr(gitlab_lane.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"ok"))
    assert gitlab_lane.ingress_manifest(pins) == b"ok"


def test_runtime_config_installs_the_provided_source_without_manifest_paths(tmp_path: Path) -> None:
    environment = SimpleNamespace(
        workspace=tmp_path,
        namespace=gitlab_lane.NAMESPACE,
        application=gitlab_lane.APPLICATION,
        images=SimpleNamespace(controller="controller:x", responder="responder:x"),
        cluster="fastloop-w0",
    )
    config = gitlab_lane.lane_runtime_config(environment)
    assert config.deployment_source == gitlab_lane.deployment_source()
    assert config.deployment_source == assemble.deployment_source()
    assert config.control_namespace == "gitlab-sdo"
    assert config.persistent


def test_up_accepts_the_gitlab_app() -> None:
    parser = build_parser()
    args = parser.parse_args(["up", "--run-dir", "/tmp/x", "--app", "gitlab", "--goal", "g"])
    assert args.app == "gitlab"


def test_default_goal_fits_the_deployer_budget() -> None:
    assert len(gitlab_lane.DEFAULT_GOAL) <= 256


def test_resource_claim_covers_the_measured_requests() -> None:
    claim = gitlab_lane.RESOURCE_CLAIM
    assert claim.cores >= 4
    assert claim.memory_gib >= 8
    assert claim.disk_gib >= 30


def test_pins_agree_with_the_chart_values_and_the_umbrella_dependency() -> None:
    pins = assemble.load_pins()
    chart = ROOT / "apps/gitlab/chart"
    for name, image in pins["datastore_images"].items():
        values = yaml.safe_load((chart / "charts" / name / "values.yaml").read_text())
        assert values["image"] == image, f"{name} image drifted from pins.json"
    umbrella = yaml.safe_load((chart / "Chart.yaml").read_text())
    upstream = next(dep for dep in umbrella["dependencies"] if dep["name"] == pins["gitlab_chart"]["name"])
    assert upstream["version"] == pins["gitlab_chart"]["version"]
    assert pins["namespace"] == gitlab_lane.NAMESPACE


def test_onboarding_refuses_until_the_host_admits_a_provided_plan(tmp_path: Path) -> None:
    environment = SimpleNamespace(namespace="gitlab", workspace=tmp_path)
    with pytest.raises(gitlab_lane.ProvidedPlanUnavailable, match="deployment-plan"):
        gitlab_lane.onboard(environment, object(), "goal")
