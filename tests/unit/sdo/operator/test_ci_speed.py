"""Required CI keeps all coverage while selecting only affected image builds."""

from __future__ import annotations

import fnmatch
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]


def workflow() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())


@pytest.mark.parametrize(
    ("path", "images"),
    [
        ("docs/architecture.md", set()),
        ("tests/unit/test_example.py", set()),
        ("sdo/agents/responder/worker.py", {"responder", "benchmark"}),
        ("libs/agent_cli/cli.py", {"responder", "benchmark"}),
        ("sdo/operator/runtime/controller.go", {"controller", "validator", "responder", "benchmark"}),
        ("sdo/agents/registrations/responder.yaml", {"controller", "responder", "benchmark"}),
        ("benchmarks/sregym/adapter/submission.py", {"benchmark"}),
        ("benchmarks/sregym/fastloop/recording_worker.py", {"benchmark"}),
        ("benchmarks/sregym/analysis/report.py", set()),
        ("scripts/build_sdo_images.sh", {"controller", "validator", "responder", "benchmark"}),
    ],
)
def test_image_filters_follow_runtime_inputs(path: str, images: set[str]) -> None:
    filters = yaml.safe_load(workflow()["jobs"]["changes"]["steps"][1]["with"]["filters"])
    selected = {
        image
        for image in ("controller", "validator", "responder", "benchmark")
        if any(fnmatch.fnmatchcase(path, pattern) for pattern in filters[f"image_{image}"])
    }
    assert selected == images


@pytest.mark.parametrize("event", ["pull_request", "merge_group"])
def test_merge_queue_forces_every_image_and_test_area(event: str, tmp_path: Path) -> None:
    step = next(s for s in workflow()["jobs"]["changes"]["steps"] if s.get("id") == "resolve")
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, **dict.fromkeys(step["env"], "false"), "EVENT": event, "GITHUB_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    resolved = dict(line.split("=", 1) for line in output.read_text().splitlines())
    for name in ("python", "go", "image_controller", "image_validator", "image_responder", "image_benchmark"):
        assert resolved[name] == str(event == "merge_group").lower()


def test_required_ci_still_accounts_for_every_job() -> None:
    jobs = workflow()["jobs"]
    aggregate = jobs["required-pr-ci"]
    assert aggregate["name"] == "Required PR CI"
    assert aggregate["if"] == "always()"
    assert set(aggregate["needs"]) == jobs.keys() - {"required-pr-ci"}


@pytest.mark.parametrize(
    ("images", "expected"),
    [
        ("controller", {"sdo-controller"}),
        ("validator", {"sdo-detector-validator"}),
        ("responder", {"sdo-responder"}),
        ("benchmark", {"sdo-sregym-responder"}),
        ("responder benchmark", {"sdo-responder", "sdo-sregym-responder"}),
    ],
)
def test_selected_images_are_built_and_smoked(tmp_path: Path, images: str, expected: set[str]) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls"
    for name, body in {
        "docker": 'printf "%s\\n" "$*" >> "$SDO_TEST_CALLS"',
        "go": 'printf "schema-test\\n"',
    }.items():
        command = bin_dir / name
        command.write_text(f"#!/bin/sh\n{body}\n")
        command.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/build_sdo_images.sh")],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_IMAGES": images,
            "SDO_IMAGE_TAG": "ci-speed-test",
            "SDO_SKIP_IMAGE_BUILD": "0",
            "SDO_IMAGE_PREFLIGHT": "0",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    lines = calls.read_text().splitlines()
    builds = [shlex.split(line) for line in lines if line.startswith("buildx bake ")]
    assert builds == [["buildx", "bake", "--file", "scripts/sdo-images.hcl", "--load", *images.split()]]
    graph = (ROOT / builds[0][3]).read_text()
    exported = set()
    for target in images.split():
        block = re.search(rf'^target "{target}" \{{(.*?)^\}}', graph, re.MULTILINE | re.DOTALL)
        assert block is not None, f"missing Bake target: {target}"
        tag = re.search(r'tags = \["([^:]+):\$\{SDO_IMAGE_TAG\}"\]', block[1])
        assert tag is not None, f"missing tagged output: {target}"
        exported.add(tag[1])
        if target == "benchmark":
            # Bake consumes the responder internally; it is exported only if selected.
            assert 'responder-base = "target:responder"' in block[1]
    assert exported == expected
    smoked = {
        token.removesuffix(":ci-speed-test")
        for line in lines
        if line.startswith("run --rm --user 65532:65532 ")
        for token in line.split()
        if token.endswith(":ci-speed-test")
    }
    assert smoked == expected


@pytest.mark.parametrize(
    ("result", "accepted"), [("success", True), ("skipped", True), ("failure", False), ("cancelled", False)]
)
def test_required_ci_rejects_any_applicable_unsuccessful_job(result: str, accepted: bool) -> None:
    import json

    steps = workflow()["jobs"]["required-pr-ci"]["steps"]
    step = next(step for step in steps if "JOB_RESULTS" in step.get("env", {}))
    assert "if" not in step
    assert "continue-on-error" not in step
    completed = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "JOB_RESULTS": json.dumps({"images": {"result": result}})},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (completed.returncode == 0) is accepted


def test_ci_bounds_nested_go_builds_and_routes_compiling_contracts() -> None:
    configuration = workflow()
    assert configuration["env"]["GOFLAGS"] == "-p=2"
    assert configuration["env"]["GOMAXPROCS"] == "2"
    jobs = configuration["jobs"]
    assert all(0 < job.get("timeout-minutes", 0) <= 5 for job in jobs.values())
    assert "go-integration" in jobs["required-pr-ci"]["needs"]
    steps = jobs["go-integration"]["steps"]
    assert any("scripts/test_go_integration.sh" in step.get("run", "") for step in steps)
    python_runs = [row["run"] for row in jobs["python"]["strategy"]["matrix"]["include"]]
    assert any("not go_integration" in run for run in python_runs)
    assert len(python_runs) == 2  # check_errors already owns types and architecture.


def test_go_shards_restore_the_completed_preparation_cache() -> None:
    jobs = workflow()["jobs"]
    warm = jobs["prepare-go"]
    assert warm["timeout-minutes"] == 5
    assert any("scripts/prepare_go_ci.sh" in step.get("run", "") for step in warm["steps"])
    for name in ("python", "controller", "go-integration", "replay", "chaos"):
        assert "prepare-go" in jobs[name]["needs"]
        caches = [step for step in jobs[name]["steps"] if step.get("uses", "").startswith("actions/cache")]
        assert any("ci-prepared" in step["with"]["key"] for step in caches), name


def test_image_jobs_use_the_compilation_owners_schema_identity() -> None:
    jobs = workflow()["jobs"]
    images = jobs["images"]
    assert "prepare-go" in images["needs"]
    assert images["env"]["SDO_SOURCE_REVISION"] == "${{ github.sha }}"
    assert images["env"]["SDO_VALIDATOR_SCHEMA_IDENTITY"] == "${{ needs.prepare-go.outputs.schema_identity }}"
    assert set(images["strategy"]["matrix"]["family"]) == {"controller", "validator", "responder", "benchmark"}
    partitions = [row["operator"] for row in jobs["controller"]["strategy"]["matrix"]["include"] if "operator" in row]
    assert sorted(partitions) == ["acceptance", "acceptance-second", "broker", "compiler", "remaining"]


@pytest.mark.parametrize("mode", ["normal", "race"])
def test_preparation_warms_build_graphs_without_linking_or_running_tests(tmp_path: Path, mode: str) -> None:
    """Preparation shares archives; each shard owns its test binary and execution."""
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s:%s\\n" "$PWD" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    calls = tmp_path / "calls"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/prepare_go_ci.sh"), mode],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "SDO_TEST_CALLS": str(calls)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    invocations = [line.split(":", 1) for line in calls.read_text().splitlines()]
    for module in ("sdo/operator", "sdo/operator/runtime"):
        commands = [shlex.split(args) for directory, args in invocations if directory == str(ROOT / module)]
        assert commands == [["build", *(["-race"] if mode == "race" else []), "./..."]]
    assert all(shlex.split(args)[0] == "build" for _, args in invocations)


def test_normal_and_race_preparation_are_independent_bounded_jobs() -> None:
    jobs = workflow()["jobs"]
    preparation = jobs["prepare-go"]
    assert preparation["strategy"]["matrix"]["mode"] == ["normal", "race"]
    cache = next(step for step in preparation["steps"] if step.get("uses", "").startswith("actions/cache"))
    assert "${{ matrix.mode }}" in cache["with"]["key"]
    for name in ("controller", "replay", "chaos"):
        keys = [step["with"]["key"] for step in jobs[name]["steps"] if step.get("uses", "").startswith("actions/cache")]
        assert any("ci-prepared-normal-" in key for key in keys)
        assert any("ci-prepared-race-" in key for key in keys)


def test_image_builder_refuses_dirty_source_before_docker(tmp_path: Path) -> None:
    command = tmp_path / "git"
    command.write_text('#!/bin/sh\nprintf "?? sdo/operator/uncommitted.go\\n"\n')
    command.chmod(0o755)
    docker = tmp_path / "docker"
    docker.write_text("#!/bin/sh\necho unexpected-docker >&2\nexit 99\n")
    docker.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/build_sdo_images.sh")],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "image source has uncommitted changes" in result.stderr
    assert "unexpected-docker" not in result.stderr
