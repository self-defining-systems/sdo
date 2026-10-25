"""Image builds consume the native schema pin, independently verified by Go."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("ci_schema_identity", ROOT / "scripts/ci_schema_identity.py")
assert SPEC is not None
assert SPEC.loader is not None
identity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(identity)


def test_native_pin_is_available_without_compiling_controller() -> None:
    assert identity.read_identity(ROOT).startswith("sdo.dev/v1alpha1+sdk.")
    # The authoritative Go assertion still runs in PR CI's static gate and the
    # built-image smoke independently checks the runtime against that identity.
    static = (ROOT / "scripts/check_errors.sh").read_text()
    assert "TestSchemaIdentityMatchesCommittedSDK" in static
    smoke = (ROOT / "scripts/build_sdo_images.sh").read_text()
    assert 'validate --app /fixture --expect-schema "${validator_schema_identity}"' in smoke


@pytest.mark.parametrize(
    "source",
    ["", 'const sdkSchemaIdentity = "bad"', 'const sdkSchemaIdentity = "sdo.dev/v1alpha1+sdk.0000000000000000"\n' * 2],
)
def test_missing_invalid_or_ambiguous_pin_fails_closed(tmp_path: Path, source: str) -> None:
    path = tmp_path / "sdo/operator/operatorcli/controller.go"
    path.parent.mkdir(parents=True)
    path.write_text(source)
    with pytest.raises(ValueError, match="exactly one native SDK schema pin"):
        identity.read_identity(tmp_path)


def test_image_jobs_need_no_host_go_toolchain() -> None:
    for name in ("ci.yml", "nightly.yml"):
        workflow = yaml.safe_load((ROOT / ".github/workflows" / name).read_text())
        steps = workflow["jobs"]["images"]["steps"]
        assert not any("actions/setup-go" in step.get("uses", "") for step in steps)
        assert not any("Cache image schema-identity build" == step.get("name") for step in steps)
        inputs = next(step for step in steps if step.get("id") == "image-inputs")
        assert "scripts/ci_schema_identity.py" in inputs["run"]
        assert "SDO_VALIDATOR_SCHEMA_IDENTITY" in inputs["run"]
