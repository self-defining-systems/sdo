"""PR CI has a small fixed job graph; extended coverage remains nightly."""

from __future__ import annotations

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


def test_pr_ci_has_at_most_three_jobs_without_matrix_or_path_skips() -> None:
    config = workflow()
    assert len(config["jobs"]) <= 3
    assert "merge_group" in config.get("on", config.get(True, {}))
    for name, job in config["jobs"].items():
        assert "matrix" not in job.get("strategy", {})
        if name != "required-pr-ci":
            assert "if" not in job
            assert job["timeout-minutes"] <= 5


def test_extended_checks_have_scheduled_and_manual_coverage() -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    triggers = config.get("on", config.get(True, {}))
    assert "schedule" in triggers
    assert "workflow_dispatch" in triggers
    assert "images" in config["jobs"]
    commands = "\n".join(step.get("run", "") for step in config["jobs"]["extended"]["steps"])
    assert "scripts/test_go_ci.sh nightly" in commands
    assert commands.count("-fuzztime=30s") == 6


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
    ("result", "accepted"), [("success", True), ("skipped", False), ("failure", False), ("cancelled", False)]
)
def test_required_ci_rejects_any_applicable_unsuccessful_job(result: str, accepted: bool) -> None:
    step = workflow()["jobs"]["required-pr-ci"]["steps"][0]
    completed = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "CHECKS": result, "TESTS": "success"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (completed.returncode == 0) is accepted


def assert_native_jobs_use_local_builds(configuration: dict) -> None:
    for name, job in configuration["jobs"].items():
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        if "go test " not in commands and "scripts/run_ci_contracts.py" not in commands:
            continue
        assert not any(
            step.get("uses", "").startswith(("actions/download-artifact@", "actions/upload-artifact@"))
            for step in job["steps"]
        ), f"{name}: native tests must build locally from Go caches; artifact transfers consume their deadline"


def test_native_test_jobs_do_not_transfer_prepared_compiler_artifacts() -> None:
    assert_native_jobs_use_local_builds(workflow())


@pytest.mark.parametrize("command", ["go test -race ./...", "python scripts/run_ci_contracts.py --bin 0"])
@pytest.mark.parametrize("artifact", ["prepared-normal", "compiled-go-test-plan", "renamed-build"])
@pytest.mark.parametrize("action", ["download", "upload"])
def test_native_artifact_guard_rejects_every_artifact_name(command: str, artifact: str, action: str) -> None:
    configuration = {
        "jobs": {
            "native": {
                "steps": [
                    {"uses": f"actions/{action}-artifact@pinned", "with": {"name": artifact}},
                    {"run": command},
                ]
            }
        }
    }
    with pytest.raises(AssertionError, match="native tests must build locally"):
        assert_native_jobs_use_local_builds(configuration)


def test_nightly_compile_integrations_keep_fast_pr_contracts() -> None:
    pr_commands = "\n".join(step.get("run", "") for step in workflow()["jobs"]["checks"]["steps"])
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    nightly_commands = "\n".join(step.get("run", "") for step in nightly["jobs"]["extended"]["steps"])
    moved = "tests/unit/benchmarks/sregym/fastloop/test_generated_worker_contract.py"
    assert f"--ignore={moved}" in pr_commands
    for path in re.findall(r"--ignore=([^\s]+)", pr_commands):
        assert (ROOT / path).is_file()
        assert path in shlex.split(nightly_commands)
    for filename in ("test_reflection_script.py", "test_responder_script.py"):
        path = f"tests/unit/benchmarks/sregym/fastloop/{filename}"
        assert (ROOT / path).is_file()
        assert f"--ignore={path}" not in pr_commands


def test_go_job_saves_its_own_cache_without_racing_python_checks() -> None:
    jobs = workflow()["jobs"]
    keys = {}
    for name in ("checks", "tests"):
        saves = [s for s in jobs[name]["steps"] if s.get("uses", "").startswith("actions/cache/save@")]
        assert len(saves) == 1
        keys[name] = saves[0]["with"]["key"]
    assert keys["checks"] != keys["tests"], "immutable caches must retain generated Go test builds"
    restores = [s for s in jobs["tests"]["steps"] if s.get("uses", "").startswith("actions/cache/restore@")]
    assert restores[0]["with"]["key"] == keys["tests"]
    assert restores[0]["with"]["restore-keys"].splitlines()[0].startswith("ci-tests-")


@pytest.mark.parametrize("mode", ["pr", "nightly"])
def test_go_runner_keeps_integration_coverage_in_one_package_scheduler(tmp_path: Path, mode: str) -> None:
    calls = tmp_path / "calls"
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    subprocess.run(
        ["bash", str(ROOT / "scripts/test_go_ci.sh"), mode],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "SDO_TEST_CALLS": str(calls)},
        check=True,
    )
    invocations = [shlex.split(line) for line in calls.read_text().splitlines()]
    assert len(invocations) == (1 if mode == "pr" else 2)
    for args in invocations:
        assert "-tags=integration" in args
    assert "./sdo/operator/..." in invocations[0]
    nightly_names = [
        line
        for line in (ROOT / "scripts/ci_nightly_go_tests.txt").read_text().splitlines()
        if line and not line.startswith("#")
    ]
    if mode == "pr":
        skip = invocations[0][invocations[0].index("-skip") + 1]
        for name in nightly_names:
            assert re.search(skip, name)
        for name in (
            "TestBuildWorkspaceCompilesApplicationDetectors",
            "TestCompileReportsTheFailingStage",
            "TestSimTrustedFastPathNeverRollsBackACorrectFix",
            "TestBuildMemoryHostUsesStagedMemory",
        ):
            assert not re.search(skip, name)
    else:
        skip = invocations[0][invocations[0].index("-skip") + 1]
        assert all(not re.search(skip, name) for name in nightly_names)
