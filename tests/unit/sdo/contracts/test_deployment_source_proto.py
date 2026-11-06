"""DeploymentSource messages round-trip as ProtoJSON and validate their identifiers."""

from __future__ import annotations

import pytest
from google.protobuf import any_pb2, json_format

from sdo.contracts import proto as contracts


def _packed(message: object) -> any_pb2.Any:
    packed = any_pb2.Any()
    packed.Pack(message)  # type: ignore[attr-defined]
    return packed


def test_deployment_plan_carries_source_images_and_workloads() -> None:
    plan = contracts.DeploymentPlan(
        source=contracts.DeploymentSource(
            renderer_id="raw", config=_packed(contracts.RawRenderConfig(paths=["deploy", "extra.yaml"]))
        ),
        images=[
            contracts.ImageSource(
                image="registry/app:dev",
                source_id="buildkit",
                config=_packed(contracts.BuildKitImageConfig(context_path=".", dockerfile_path="Dockerfile")),
            ),
            contracts.ImageSource(
                image="redis:7", source_id="prebuilt", config=_packed(contracts.PrebuiltImageConfig())
            ),
        ],
        expected_workload_ids=["Deployment/app/web"],
    )
    parsed = json_format.Parse(json_format.MessageToJson(plan), contracts.DeploymentPlan())
    assert parsed == plan
    raw = contracts.RawRenderConfig()
    assert parsed.source.config.Unpack(raw)
    assert list(raw.paths) == ["deploy", "extra.yaml"]


def test_controller_config_carries_deployment_source() -> None:
    config = contracts.ControllerConfig(
        deployment_source=contracts.DeploymentSource(
            renderer_id="kustomize", config=_packed(contracts.KustomizeRenderConfig(path="overlay"))
        )
    )
    assert json_format.Parse(json_format.MessageToJson(config), contracts.ControllerConfig()) == config


@pytest.mark.parametrize("renderer_id", ["Raw", "1raw", "raw_v1", ""])
def test_renderer_id_pattern_is_declared(renderer_id: str) -> None:
    from buf.validate import validate_pb2  # type: ignore[import-not-found]

    field = contracts.DeploymentSource.DESCRIPTOR.fields_by_name["renderer_id"]
    pattern = field.GetOptions().Extensions[validate_pb2.field].string.pattern
    import re

    assert re.fullmatch(pattern, renderer_id) is None
