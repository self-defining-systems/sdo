from pathlib import Path


def test_runtime_smoke_runs_native_lifecycle_with_typed_fixture_workers() -> None:
    script = (Path(__file__).parents[5] / "scripts/smoke_sdo_runtime_kind.sh").read_text()

    assert '"${workspace}/sdo-operator" host' in script
    assert "tests/fixtures/sdo/native_smoke/registrations" in script
    assert ".goal_commit" in script
    assert ".judge_commit" in script
    assert "sdo.controller_install" not in script
    assert "run_initial_lifecycle" not in script
    assert "ensure_operational_memory" not in script


def test_image_build_checks_the_benchmark_adapter_as_the_runtime_uid() -> None:
    script = (Path(__file__).parents[5] / "scripts/build_sdo_images.sh").read_text()

    assert "--target sregym-responder" in script
    assert "--user 65532:65532" in script
    assert "benchmarks.sregym.adapter.submission --help" in script


def test_image_build_tag_is_overridable_without_touching_the_shared_default() -> None:
    script = (Path(__file__).parents[5] / "scripts/build_sdo_images.sh").read_text()

    assert 'tag="${SDO_IMAGE_TAG:-v0.1.0}"' in script
    assert ":v0.1.0" not in script
