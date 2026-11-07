"""PR CI has a small fixed job graph; extended coverage remains nightly."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(autouse=True)
def _no_selection_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    """The CI runner exports SDO_CI_PLAN for the job; the runner scripts under test must not read it."""
    monkeypatch.delenv("SDO_CI_PLAN", raising=False)


def workflow() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())


def test_pr_ci_shards_are_fixed_small_and_never_skipped_by_path() -> None:
    config = workflow()
    assert set(config["jobs"]) == {"checks", "python", "tests", "required-pr-ci"}
    assert "merge_group" in config.get("on", config.get(True, {}))
    assert config["jobs"]["python"]["strategy"]["matrix"]["shard"] == ["core", "adapter", "fastloop"]
    assert config["jobs"]["tests"]["strategy"]["matrix"]["shard"] == ["validate", "gate", "cli", "rest"]
    for name, job in config["jobs"].items():
        assert job.get("strategy", {}).get("fail-fast", False) is False
        if name != "required-pr-ci":
            assert "if" not in job
            assert job["timeout-minutes"] <= 5


def test_extended_checks_have_scheduled_and_manual_coverage() -> None:
    config = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    triggers = config.get("on", config.get(True, {}))
    assert "schedule" in triggers
    assert "workflow_dispatch" in triggers
    assert "images" in config["jobs"]
    commands = "\n".join(step.get("run", "") for step in config["jobs"]["go"]["steps"])
    assert "scripts/test_go_ci.sh nightly" in commands
    assert commands.count("-fuzztime=30s") == 2


def test_required_ci_still_accounts_for_every_job() -> None:
    jobs = workflow()["jobs"]
    aggregate = jobs["required-pr-ci"]
    assert aggregate["name"] == "Required PR CI"
    assert aggregate["if"] == "always()"
    assert set(aggregate["needs"]) == jobs.keys() - {"required-pr-ci"}


def test_required_ci_aggregate_step_runs_on_every_event() -> None:
    steps = workflow()["jobs"]["required-pr-ci"]["steps"]
    assert "if" not in next(step for step in steps if "CHECKS" in step.get("env", {}))


def test_python_shards_cover_every_test_path_and_gate_combined_coverage() -> None:
    """Core is everything outside fastloop and adapter; the combined floor is enforced after the shards finish."""
    script = (ROOT / "scripts/test_python.sh").read_text()
    assert (
        "--ignore=tests/unit/benchmarks/sregym/fastloop --ignore=tests/unit/benchmarks/sregym/adapter tests" in script
    )
    assert "paths=(tests/unit/benchmarks/sregym/fastloop)" in script
    assert "paths=(tests/unit/benchmarks/sregym/adapter)" in script
    steps = workflow()["jobs"]["required-pr-ci"]["steps"]
    commands = "\n".join(step.get("run", "") for step in steps)
    assert "coverage combine" in commands
    assert "--cov-fail-under=0" in script
    assert "coverage report" in commands
    assert "--fail-under" not in commands, "pyproject owns the floor"
    uploads = [
        s for s in workflow()["jobs"]["python"]["steps"] if s.get("uses", "").startswith("actions/upload-artifact@")
    ]
    assert uploads[0]["with"]["name"] == "coverage-${{ matrix.shard }}"


@pytest.mark.parametrize("shard", ["validate", "gate", "cli", "rest"])
def test_go_shards_partition_the_pr_packages(tmp_path: Path, shard: str) -> None:
    calls = tmp_path / "calls"
    command = tmp_path / "go"
    command.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$SDO_TEST_CALLS"\n'
        '[ "$1" = list ] && printf "sdo.dev/operator/memory/validate %s\\nsdo.dev/operator/runtime %s\\n'
        'sdo.dev/operator/operatorcli %s\\n" "$SDO_ROOT/sdo/operator/memory/validate" "$SDO_ROOT/sdo/operator/runtime" '
        '"$SDO_ROOT/sdo/operator/operatorcli"\nexit 0\n'
    )
    command.chmod(0o755)
    subprocess.run(
        ["bash", str(ROOT / "scripts/test_go_ci.sh"), "pr", shard],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_ROOT": str(ROOT),
        },
        check=True,
    )
    test = [shlex.split(line) for line in calls.read_text().splitlines() if line.startswith("test ")]
    assert len(test) == 1
    packages = [a for a in test[0] if a.startswith(("./", "sdo.dev/"))]
    if shard in ("validate", "gate"):
        assert packages == ["./sdo/operator/memory/validate"]
        gate = json.loads((ROOT / ".github/ci_selection.json").read_text())["go_validate_gate_tests"]
        flag = "-run" if shard == "gate" else "-skip"
        assert test[0][test[0].index(flag) + 1] == f"^({'|'.join(gate)})$"
    elif shard == "cli":
        assert packages == ["sdo.dev/operator/operatorcli"]
    else:
        assert packages == ["sdo.dev/operator/runtime"]
    assert "-short" in test[0]
    assert "-tags=integration" in test[0]


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
    step = next(step for step in workflow()["jobs"]["required-pr-ci"]["steps"] if "CHECKS" in step.get("env", {}))
    completed = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "CHECKS": result, "PYTHON": "success", "TESTS": "success"},
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
    pr_commands = "\n".join(step.get("run", "") for step in workflow()["jobs"]["python"]["steps"])
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    nightly_commands = "\n".join(
        step.get("run", "") for job in nightly["jobs"].values() for step in job.get("steps", [])
    )
    moved = "tests/unit/benchmarks/sregym/fastloop/test_generated_worker_contract.py"
    assert f"--ignore={moved}" in pr_commands
    for path in re.findall(r"--ignore=([^\s]+)", pr_commands):
        assert (ROOT / path).is_file()
        assert path in shlex.split(nightly_commands)
    for filename in ("test_reflection_script.py",):
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
    fallbacks = restores[0]["with"]["restore-keys"].splitlines()
    assert fallbacks[0].startswith("ci-tests-")
    # go.sum discovery can change during a job (e.g. submodule initialization).
    # Content-addressed compiler entries remain safe across dependency changes.
    assert "ci-tests-v2-${{ runner.os }}-${{ runner.arch }}-1.26.8-" in fallbacks
    assert "ci-shared-v1-${{ runner.os }}-${{ runner.arch }}-1.26.8-" in fallbacks


def test_envtest_binaries_are_cached_by_pinned_version_in_pr_and_nightly_go_jobs() -> None:
    """Conformance runs on every PR, so the apiserver download must be a cache miss only."""
    version = re.search(
        r'KubernetesVersion = "([^"]+)"', (ROOT / "sdo/operator/runtime/envtestio/envtestio.go").read_text()
    )
    assert version is not None
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    for config, job in ((workflow(), "tests"), (nightly, "go")):
        steps = [s for s in config["jobs"][job]["steps"] if s.get("with", {}).get("path") == "~/.cache/sdo-envtest"]
        assert len(steps) == 1, job
        assert steps[0]["uses"].startswith("actions/cache@"), "must save after a miss"
        assert version[1] in steps[0]["with"]["key"]
    # Only the skip of a shard with no selected packages may condition it, never the event or branch.
    cache = [s for s in workflow()["jobs"]["tests"]["steps"] if "sdo-envtest" in str(s.get("with"))][0]
    assert cache.get("if", "env.SDO_CI_SKIP != 'true'") == "env.SDO_CI_SKIP != 'true'"


def test_envtest_binaries_are_installed_by_the_pinned_script_not_by_a_test() -> None:
    """A test-time `go run setup-envtest` is a network call per run; CI installs once, verified."""
    script = (ROOT / "scripts/install_envtest.sh").read_text()
    version = re.search(
        r'KubernetesVersion = "([^"]+)"', (ROOT / "sdo/operator/runtime/envtestio/envtestio.go").read_text()
    )
    assert version is not None
    assert f'version="{version[1]}"' in script
    assert len(re.findall(r"sha512=[0-9a-f]{128}", script)) == 4
    assert "--retry" in script
    nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
    for config, job in ((workflow(), "tests"), (nightly, "go"), (nightly, "controller-e2e")):
        runs = [s.get("run", "") for s in config["jobs"][job]["steps"]]
        assert any("KUBEBUILDER_ASSETS=$(bash scripts/install_envtest.sh)" in r for r in runs), job
        assert not any("setup-envtest" in r for r in runs), job


def test_kube_conformance_runs_on_every_pr() -> None:
    defined = "\n".join(p.read_text() for p in (ROOT / "sdo/operator/runtime/sim").glob("*_test.go"))
    match = re.search(r"^func TestKubeConformance\(.*?^}", defined, re.MULTILINE | re.DOTALL)
    assert match, "TestKubeConformance moved"
    assert "testing.Short()" not in match[0], "PR CI runs -short; conformance must not skip itself"


@pytest.mark.parametrize("mode", ["pr", "nightly"])
def test_go_runner_keeps_integration_coverage_in_one_package_scheduler(tmp_path: Path, mode: str) -> None:
    calls = tmp_path / "calls"
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    subprocess.run(
        ["bash", str(ROOT / "scripts/test_go_ci.sh"), mode],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_ROOT": str(ROOT),
        },
        check=True,
    )
    invocations = [shlex.split(line) for line in calls.read_text().splitlines()]
    assert len(invocations) == 1
    for args in invocations:
        assert "-tags=integration" in args
    assert "./sdo/operator/..." in invocations[0]
    assert ("-short" in invocations[0]) == (mode == "pr")
    assert ("-race" in invocations[0]) == (mode == "nightly")
    assert "-skip" not in invocations[0], "tests choose their own tier with testing.Short; there are no name lists"
    assert not (ROOT / "scripts/ci_nightly_go_tests").exists()


def test_short_skips_are_only_for_tests_that_name_nightly() -> None:
    """A -short skip must say nightly runs the test, and no test may skip silently."""
    for path in ROOT.glob("sdo/**/*_test.go"):
        text = path.read_text()
        for match in re.finditer(r"if testing\.Short\(\) \{\n\s*t\.Skip\((.*)\)", text):
            assert "nightly CI runs it without -short" in match[1], f"{path}: {match[1]}"


@pytest.mark.parametrize("script", ["run_tests.sh"])
def test_full_and_hermetic_runners_retain_integration_tests(tmp_path: Path, script: str) -> None:
    calls = tmp_path / "calls"
    for name, body in {
        "go": '#!/bin/sh\nprintf "%s|%s\\n" "$PWD" "$*" >> "$SDO_TEST_CALLS"\n',
        "uv": "#!/bin/sh\nexit 0\n",
    }.items():
        command = tmp_path / name
        command.write_text(body)
        command.chmod(0o755)
    subprocess.run(
        ["bash", str(ROOT / "scripts" / script)],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_ROOT": str(ROOT),
        },
        check=True,
    )
    invocations = [line.split("|", 1) for line in calls.read_text().splitlines()]
    assert invocations
    for _, command in invocations:
        if shlex.split(command)[0] != "test":
            continue
        assert "-tags=integration" in shlex.split(command), "full runners must not silently omit process contracts"
    if script == "run_tests.sh":
        assert any(Path(directory).name == "replay_go" for directory, _ in invocations)
    else:
        # Stale alternatives silently match nothing in go test -run.
        selected = shlex.split(invocations[0][1])
        names = set(re.findall(r"Test\w+", selected[selected.index("-run") + 1]))
        available = set()
        for path in (ROOT / "sdo/operator/runtime").glob("*_test.go"):
            available.update(re.findall(r"^func (Test\w+)\(", path.read_text(), re.MULTILINE))
        assert names <= available, f"retired hermetic selectors: {sorted(names - available)}"


def test_python_runner_builds_protocol_tools_once_before_workers(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    for name, body in {
        "go": 'printf "go %s\\n" "$*" >> "$SDO_TEST_CALLS"',
        "uv": '''test -f "$SDO_TEST_CALLS"
 test -n "$SDO_NATIVE_RECEIPT_BIN"
 test -n "$SDO_SUBMISSION_GUARD_BIN"
 test -n "$SDO_RBAC_HELPER_BIN"
 printf 'uv %s\\n' "$*" >> "$SDO_TEST_CALLS"''',
    }.items():
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\nset -eu\n{body}\n")
        command.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh"), "--durations=30"],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_SKIP_OPERATOR_PREBUILD": "1",  # the prebuild would write to the call log from the background
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    lines = calls.read_text().splitlines()
    assert len([line for line in lines if line.startswith("go build ")]) == 3  # receipt, submission guard, RBAC helper
    assert lines[-1].startswith("uv run --extra test pytest ")
    assert "--durations=30" in lines[-1]


def test_python_runner_starts_the_operator_build_before_collection() -> None:
    """The operator CLI build is serial work every xdist worker waits on; it must overlap collection."""
    script = (ROOT / "scripts/test_python.sh").read_text()
    prebuild = script.index("scripts/operator_test_binary.py")
    assert script.index("go build -o") < prebuild < script.index("uv run --extra test pytest")
    assert script[prebuild:].split("\n", 1)[0].rstrip().endswith("&"), "the prebuild must run in the background"


def test_architecture_delegation_requires_the_full_python_suite() -> None:
    jobs = workflow()["jobs"]
    commands = "\n".join(step.get("run", "") for step in jobs["checks"]["steps"] + jobs["python"]["steps"])
    assert "check_errors.sh --python-suite" in commands
    assert "scripts/test_python.sh --durations=30" in commands
    assert "--tach" not in commands
    assert "--ignore=tests/unit/test_architecture.py" not in commands


def test_checks_job_delegates_duplicated_work_to_the_suites_that_run_it() -> None:
    """--python-suite must not repeat what the full Python suite or the Go tests job runs."""
    script = (ROOT / "scripts/check_errors.sh").read_text()
    ignored = {
        Path(arg.removeprefix("--ignore=")).name
        for step in workflow()["jobs"]["python"]["steps"]
        for arg in step.get("run", "").replace("\\\n", " ").split()
        if arg.startswith("--ignore=")
    }
    ratchets = {Path(path.split("::")[0]).name for path in re.findall(r"tests/unit/\S+\.py(?:::\w+)?", script)}
    assert ratchets, "the fast ratchet selection moved"
    assert not ratchets & ignored, "a ratchet delegated to the full suite must not be ignored there"
    go_errors = (ROOT / "scripts/check_go_errors.sh").read_text()
    assert '--python-suite) check bash "$SCRIPT_DIR/check_go_errors.sh" --skip-go-tests' in script
    assert "run_parity=0" in go_errors  # the Go tests job runs the parity tests
    assert "go list -export ./..." in (ROOT / "scripts/check_go_mains.sh").read_text(), (
        "go build ./... links every command"
    )


def test_a_selected_file_the_caller_ignores_is_not_run(tmp_path: Path) -> None:
    """pytest runs an explicitly named file despite --ignore; the nightly-tier suites must not leak into PR runs."""
    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "changed": [],
                "go_packages": [],
                "mode": "selected",
                "python_tests": ["tests/unit/a_test.py", "tests/unit/slow_test.py"],
                "reasons": [],
            }
        )
    )
    calls = tmp_path / "calls"
    for name, body in {
        "go": ":",
        "uv": 'printf "%s\\n" "$*" >> "$SDO_TEST_CALLS"',
    }.items():
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\nset -eu\n{body}\n")
        command.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "SDO_TEST_CALLS": str(calls),
        "SDO_SKIP_OPERATOR_PREBUILD": "1",
        "SDO_CI_PLAN": str(plan),
        "SDO_PYTEST_SHARD": "core",
    }

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(ROOT / "scripts/test_python.sh"), *args], env=env, capture_output=True, text=True, check=False
        )

    result = run("--ignore=tests/unit/slow_test.py")
    assert result.returncode == 0, result.stderr
    pytest_line = calls.read_text().splitlines()[-1]
    assert "tests/unit/a_test.py" in pytest_line.split()
    assert "tests/unit/slow_test.py" not in pytest_line.replace("--ignore=tests/unit/slow_test.py", "")

    calls.unlink()
    result = run("--ignore=tests/unit/slow_test.py", "--ignore=tests/unit/a_test.py")
    assert result.returncode == 0, result.stderr
    assert "no Python tests selected" in result.stdout
    assert not calls.exists(), "nothing selected after the exclusions: pytest must not start"


def test_the_controller_e2e_fetches_every_workspace_module_before_it_goes_offline(tmp_path: Path) -> None:
    """The sandbox builds detector modules with GOPROXY=off, so the host module cache must already hold their graph.

    A hosted runner's cache lacked it and every nightly e2e failed with "module lookup disabled by GOPROXY=off".
    """
    calls = tmp_path / "calls"
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s|%s\\n" "$PWD" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    subprocess.run(
        ["bash", str(ROOT / "scripts/prefetch_go_modules.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SDO_TEST_CALLS": str(calls),
            "SDO_ROOT": str(ROOT),
        },
        check=True,
    )
    modules = json.loads((ROOT / ".github/ci_selection.json").read_text())["go_workspace_modules"]
    downloaded = {Path(line.split("|")[0]).relative_to(ROOT).as_posix() for line in calls.read_text().splitlines()}
    assert downloaded == {m for m in modules if m.startswith("sdo/operator")}
    assert all(line.endswith("|mod download all") for line in calls.read_text().splitlines()), "tidy needs test deps"
    assert "scripts/prefetch_go_modules.sh" in (ROOT / "scripts/test_controller_e2e.sh").read_text()


def test_files_pull_requests_ignore_still_collect() -> None:
    """Nightly is the only run of an ignored file, so rot must at least surface as a collection error here.

    A nightly red is noticed hours after the change that caused it; a file that cannot import fails the PR.
    """
    commands = "\n".join(step.get("run", "") for step in workflow()["jobs"]["python"]["steps"])
    ignored = re.findall(r"--ignore=(tests/\S+\.py)", commands)
    assert ignored, "the PR-ignored test list moved"
    result = subprocess.run(
        ["uv", "run", "--extra", "test", "pytest", "--collect-only", "-q", "-p", "no:tach", "--no-cov", *ignored],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
