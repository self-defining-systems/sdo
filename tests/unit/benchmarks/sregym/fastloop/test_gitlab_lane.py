"""GitLab lane harness: provided deployment source, prerequisites, workspace and install configuration."""

from __future__ import annotations

import hashlib
import io
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from google.protobuf import json_format

from apps.gitlab import assemble
from benchmarks.sregym.fastloop import gitlab_lane
from benchmarks.sregym.fastloop.accepted_source import RenderedWorkloads
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
    plan = gitlab_lane.plan_from_workloads(_workloads())
    config = gitlab_lane.lane_runtime_config(environment, plan)
    assert config.deployment_plan == plan
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


def _workloads() -> RenderedWorkloads:
    return RenderedWorkloads(
        ("Deployment/gitlab/web", "StatefulSet/other/db", "CronJob/gitlab/tick", "Job/gitlab/migrate"),
        ("busybox:1", "postgres:17@sha256:" + "a" * 64, "registry.invalid/web:1"),
    )


def test_plan_declares_every_rendered_image_prebuilt_and_every_workload() -> None:
    from sdo.contracts.proto import DeploymentPlan

    plan = gitlab_lane.plan_from_workloads(_workloads())
    message = json_format.ParseDict(plan, DeploymentPlan())
    assert plan["source"] == gitlab_lane.deployment_source()
    assert [image.image for image in message.images] == [
        "busybox:1",
        "postgres:17@sha256:" + "a" * 64,
        "registry.invalid/web:1",
    ]
    assert {image.source_id for image in message.images} == {"prebuilt"}
    assert all(image.config.TypeName().endswith(".PrebuiltImageConfig") for image in message.images)
    assert list(message.expected_workload_ids) == [
        "Deployment/gitlab/web",
        "StatefulSet/other/db",
        "CronJob/gitlab/tick",
        "Job/gitlab/migrate",
    ]


def test_plan_with_no_workloads_is_rejected() -> None:
    with pytest.raises(ValueError, match="workload"):
        gitlab_lane.plan_from_workloads(RenderedWorkloads((), ()))


def test_onboarding_hands_the_plan_to_the_native_lifecycle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from benchmarks.sregym.adapter import driver

    monkeypatch.delenv("KUBECONFIG", raising=False)
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    seen: dict[str, object] = {}

    def launch(args: SimpleNamespace, **kwargs: object) -> dict[str, str]:
        seen["kubeconfig"] = os.environ.get("KUBECONFIG")
        seen["plan"] = args.deployment_plan
        seen["objective"] = kwargs["health_objective"]
        return {"judge_commit": "c"}

    monkeypatch.setattr(driver, "_launch_native_lifecycle", launch)
    environment = SimpleNamespace(
        workspace=tmp_path,
        namespace=gitlab_lane.NAMESPACE,
        application=gitlab_lane.APPLICATION,
        images=SimpleNamespace(controller="controller:x", responder="responder:x"),
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        apply_operator_env=lambda environ: None,
    )
    plan = gitlab_lane.plan_from_workloads(_workloads())
    config = gitlab_lane.lane_runtime_config(environment, plan)
    assert gitlab_lane.onboard(environment, config, "goal") == {"judge_commit": "c"}
    assert seen == {"kubeconfig": str(tmp_path / "kubeconfig"), "plan": plan, "objective": "goal"}


def test_onboarding_without_a_plan_is_refused(tmp_path: Path) -> None:
    environment = SimpleNamespace(namespace="gitlab", workspace=tmp_path)
    with pytest.raises(ValueError, match="provided deployment plan"):
        gitlab_lane.onboard(environment, SimpleNamespace(deployment_plan=None), "goal")


def test_pull_missing_images_pulls_only_what_the_host_lacks() -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 1 if command[-1] == "absent" and command[1] == "image" else 0)

    assert gitlab_lane.pull_missing_images(["present", "absent"], run) == ["absent"]
    assert ["docker", "pull", "absent"] in calls
    assert ["docker", "pull", "present"] not in calls


def test_ingress_controller_gets_its_node_label_first_and_is_awaited_by_rollout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(gitlab_lane, "ingress_manifest", lambda pins=None: b"manifest")
    monkeypatch.setattr(
        gitlab_lane.subprocess,
        "run",
        lambda command, **_: commands.append(command) or subprocess.CompletedProcess(command, 0),
    )
    gitlab_lane.install_prerequisites(tmp_path / "kubeconfig")
    verbs = [next(word for word in command[3:] if not word.startswith("-")) for command in commands]
    assert verbs[:2] == ["label", "apply"]
    assert "ingress-ready=true" in commands[0]
    assert commands[-1][-3:-1] == ["status", "deployment/ingress-nginx-controller"]
