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
    ("images", "expected"),
    [
        ("controller", {"sdo-controller"}),
        ("validator", {"sdo-detector-validator"}),
        ("responder", {"sdo-responder"}),
        ("benchmark", {"sdo-sregym-responder"}),
        ("responder benchmark", {"sdo-responder", "sdo-sregym-responder"}),
    ],
)
def test_selected_images_are_built_and_smoked(
    tmp_path: Path, image_build_source: Path, images: str, expected: set[str]
) -> None:
    source = image_build_source
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
        ["bash", str(source / "scripts/build_sdo_images.sh")],
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
    assert "-p=2" in shlex.split(configuration["env"]["GOFLAGS"])
    assert configuration["env"]["GOMAXPROCS"] == "2"
    jobs = configuration["jobs"]
    assert all(0 < job.get("timeout-minutes", 0) <= 5 for job in jobs.values())
    assert "contracts" in jobs["required-pr-ci"]["needs"]
    steps = jobs["contracts"]["steps"]
    assert any("scripts/run_ci_contracts.py" in step.get("run", "") for step in steps)
    python_runs = [row["run"] for row in jobs["python"]["strategy"]["matrix"]["include"]]
    assert any("not go_integration" in run for run in python_runs)
    assert len(python_runs) == 2  # check_errors already owns types and architecture.


def test_go_shards_restore_the_completed_preparation_cache() -> None:
    jobs = workflow()["jobs"]
    warm = jobs["prepare-go"]
    assert warm["timeout-minutes"] == 4
    assert any("scripts/prepare_go_ci.sh" in step.get("run", "") for step in warm["steps"])
    for name in ("python", "contracts"):
        assert "prepare-go" in jobs[name]["needs"]
        caches = [step for step in jobs[name]["steps"] if step.get("uses", "").startswith("actions/cache")]
        assert any("ci-prepared" in step["with"]["key"] for step in caches), name


def test_pull_requests_restore_a_prepared_cache_from_their_base_branch() -> None:
    """PR caches are private to refs/pull/N; only base-branch pushes can seed them."""
    configuration = workflow()
    # PyYAML reads the bare `on:` key as boolean True.
    pushed = configuration[True]["push"]["branches"]
    for base in ("main", "vic-lsh/dev/sdo-improve"):
        assert any(fnmatch.fnmatch(base, pattern.replace("**", "*")) for pattern in pushed), base
    # A cancelled base-branch run saves nothing, so only PR runs cancel each other.
    assert configuration["concurrency"]["cancel-in-progress"] == "${{ github.event_name == 'pull_request' }}"


def test_a_cold_preparation_keeps_its_progress_within_the_job_deadline() -> None:
    preparation = workflow()["jobs"]["prepare-go"]
    steps = preparation["steps"]
    compile_step = next(step for step in steps if "scripts/prepare_go_ci.sh" in step.get("run", ""))
    # The compile step stops early enough to save its archives before the job is cancelled.
    assert 0 < compile_step["timeout-minutes"] < preparation["timeout-minutes"]
    for mode in ("normal", "race"):
        publisher = next(
            step
            for step in steps
            if step.get("if") == f"matrix.mode == '{mode}'" and step.get("uses", "").startswith("actions/cache@")
        )
        progress = [
            step
            for step in steps
            if step.get("uses", "").startswith("actions/cache/save@") and f"matrix.mode == '{mode}'" in step["if"]
        ]
        assert len(progress) == 1, mode
        assert f"steps.{compile_step['id']}.outcome == 'failure'" in progress[0]["if"]
        # Same paths, or the next run's restore cannot match the archive.
        assert progress[0]["with"]["path"].strip() == publisher["with"]["path"].strip()
        # A rerun resumes from the progress, but consumers only ever see a completed preparation.
        newest_fallback = publisher["with"]["restore-keys"].split()[0]
        assert progress[0]["with"]["key"].startswith(newest_fallback)
        assert "github.sha" not in progress[0]["with"]["key"]
        assert publisher["with"]["key"].endswith("-${{ github.sha }}")


def test_go_test_jobs_never_download_modules(tmp_path: Path) -> None:
    """Controller jobs run offline on the module graph that preparation caches."""
    controller = workflow()["jobs"]["contracts"]
    assert controller["env"]["GOPROXY"] == "off"
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s:%s\\n" "$PWD" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    (tmp_path / "python3").write_text("#!/bin/sh\nexit 0\n")
    (tmp_path / "python3").chmod(0o755)
    calls = tmp_path / "calls"
    subprocess.run(
        ["bash", str(ROOT / "scripts/prepare_go_ci.sh"), "normal"],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "SDO_TEST_CALLS": str(calls)},
        check=True,
    )
    downloaded = {
        directory
        for directory, args in (line.split(":", 1) for line in calls.read_text().splitlines())
        if args == "mod download all"
    }
    modules = {str(ROOT / "sdo/operator" / name) for name in ("sdk", "core", "runtime", "contracts")} | {
        str(ROOT / "sdo/operator")
    }
    # The generated inventory covers both production Go modules. Foundation
    # module graphs are still prepared alongside them.
    assert controller["strategy"]["matrix"]["bin"] == [0, 1]
    tested = {str(ROOT / "sdo/operator"), str(ROOT / "sdo/operator/runtime")}
    foundation = (ROOT / "scripts/test_go_foundation.sh").read_text()
    tested |= {str(ROOT / module) for module in re.findall(r"\b(benchmarks/\S+_go)\b", foundation)}
    modules |= {str(ROOT / "benchmarks/sregym/protocol" / name) for name in ("native_receipt_go", "submission_go")}
    assert tested <= modules
    assert downloaded == modules


def test_image_jobs_use_the_compilation_owners_schema_identity() -> None:
    jobs = nightly_fuzz()["jobs"]
    images = jobs["images"]
    assert "prepare-go" in images["needs"]
    assert images["env"]["SDO_SOURCE_REVISION"] == "${{ github.sha }}"
    assert images["env"]["SDO_VALIDATOR_SCHEMA_IDENTITY"] == "${{ needs.prepare-go.outputs.schema_identity }}"
    assert set(images["strategy"]["matrix"]["family"]) == {"controller", "validator", "responder", "benchmark"}
    assert jobs["prepare-go"]["outputs"]["schema_identity"] == "${{ steps.schema.outputs.value }}"


@pytest.mark.parametrize("mode", ["normal", "race"])
def test_preparation_compiles_shared_race_test_archive_without_running_tests(tmp_path: Path, mode: str) -> None:
    """Preparation shares archives; each shard owns its test binary and execution."""
    command = tmp_path / "go"
    command.write_text('#!/bin/sh\nprintf "%s:%s\\n" "$PWD" "$*" >> "$SDO_TEST_CALLS"\n')
    command.chmod(0o755)
    python = tmp_path / "python3"
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)
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
        assert not any(args[:1] == ["build"] and args[-1:] == ["./..."] for args in commands)
        if mode == "race":
            assert commands == [["test", "-race", "-list", ".", "-json", "./..."]]
        else:
            assert commands[0][:3] == ["test", "-c", "-o"]
            assert commands[0][-1] == "./..."
            if module.endswith("/runtime"):
                assert commands[1] == ["build", "./prober"]


def test_normal_and_race_preparation_are_independent_bounded_jobs() -> None:
    jobs = workflow()["jobs"]
    preparation = jobs["prepare-go"]
    assert preparation["strategy"]["matrix"]["mode"] == ["normal", "race"]
    caches = [step for step in preparation["steps"] if step.get("uses", "").startswith("actions/cache@")]
    assert {step["if"] for step in caches} == {"matrix.mode == 'normal'", "matrix.mode == 'race'"}
    assert sum("~/go/pkg/mod" in step["with"]["path"] for step in caches) == 1
    for name in ("contracts",):
        keys = [step["with"]["key"] for step in jobs[name]["steps"] if step.get("uses", "").startswith("actions/cache")]
        assert any("ci-prepared-v4-normal-" in key for key in keys)
        assert any("ci-prepared-v4-race-" in key for key in keys)


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


def test_downstream_cache_archives_never_unpack_into_the_same_path() -> None:
    for name in ("python", "contracts"):
        steps = workflow()["jobs"][name]["steps"]
        restored = [
            line.strip()
            for step in steps
            if step.get("uses", "").startswith("actions/cache")
            for line in step["with"]["path"].splitlines()
            if line.strip()
        ]
        assert len(restored) == len(set(restored)), (name, restored)
        assert restored.count("~/go/pkg/mod") == 1


def test_compiler_cache_union_preserves_existing_and_adds_race_entries(tmp_path: Path) -> None:
    normal, race = tmp_path / "normal", tmp_path / "race"
    for directory in (normal, race):
        (directory / "hash").mkdir(parents=True)
        (directory / "hash/shared-a").write_bytes(b"immutable shared archive")
        (directory / "hash/shared-a").chmod(0o444)
    (race / "hash/race-only-a").write_bytes(b"race archive")
    existing_inode = (normal / "hash/shared-a").stat().st_ino
    subprocess.run(["bash", str(ROOT / "scripts/merge_go_ci_caches.sh"), str(normal), str(race)], check=True)
    assert (normal / "hash/shared-a").stat().st_ino == existing_inode
    assert (normal / "hash/race-only-a").read_bytes() == b"race archive"
    assert (normal / "hash/race-only-a").stat().st_ino == (race / "hash/race-only-a").stat().st_ino


def test_consumers_restore_the_paths_published_by_each_mode_owner() -> None:
    jobs = workflow()["jobs"]
    published = {
        mode: next(
            step["with"]["path"] for step in jobs["prepare-go"]["steps"] if step.get("if") == f"matrix.mode == '{mode}'"
        )
        for mode in ("normal", "race")
    }
    for name in ("python", "contracts"):
        for step in jobs[name]["steps"]:
            if step.get("uses", "").startswith("actions/cache"):
                mode = "race" if "v4-race-" in step["with"]["key"] else "normal"
                assert step["with"]["path"].strip() == published[mode].strip(), name


def test_hosted_image_builds_use_native_store_without_transport_compression() -> None:
    steps = nightly_fuzz()["jobs"]["images"]["steps"]
    builder = next(step for step in steps if step.get("uses", "").startswith("docker/setup-buildx-action@"))
    assert builder["with"]["driver"] == "docker"
    engine = next(step for step in steps if step.get("name") == "Enable the native containerd image store")
    assert '"containerd-snapshotter": true' in engine["run"]
    bake = next(step for step in steps if step.get("uses", "").startswith("docker/bake-action@"))
    assert "*.output=type=image,compression=uncompressed" in bake["with"]["set"]
    assert "cache-to=" not in bake["with"]["set"]
    assert not bake["with"].get("load", False)


def test_image_assembly_warms_archives_without_linking_test_binaries() -> None:
    dockerfile = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    assert "go test -run" not in dockerfile
    assert "go vet ./..." in dockerfile


def test_bake_overrides_do_not_replace_an_earlier_cache_source() -> None:
    bake = next(
        step
        for step in nightly_fuzz()["jobs"]["images"]["steps"]
        if step.get("uses", "").startswith("docker/bake-action@")
    )
    keys = [line.split("=", 1)[0] for line in bake["with"]["set"].splitlines() if line.strip()]
    assert len(keys) == len(set(keys)), "repeated Bake --set assignments replace earlier cache sources"


def test_prepared_builds_include_the_portable_prober_and_source_stable_inputs() -> None:
    jobs = workflow()
    assert "-trimpath" in shlex.split(jobs["env"]["GOFLAGS"])
    prepare = (ROOT / "scripts/prepare_go_ci.sh").read_text()
    assert "CGO_ENABLED=0" in prepare
    assert "TestAssuranceSeedsPassHealthJudgeCheck" not in prepare
    assert "operator_test_binary.py" in prepare
    for name in ("prepare-go", "python", "contracts"):
        normal = [
            step
            for step in jobs["jobs"][name]["steps"]
            if step.get("uses", "").startswith("actions/cache") and "normal-" in step["with"]["key"]
        ]
        assert normal
        assert all(".fastloop/test-bin" in step["with"]["path"] for step in normal)


@pytest.mark.go_integration
def test_generated_packages_share_only_equal_source_archives(tmp_path: Path) -> None:
    """The actual Go cache must survive workspace relocation and reject edits."""
    import json

    env = {**os.environ, "GOFLAGS": workflow()["env"]["GOFLAGS"]}
    exports = []
    for name in ("first", "second"):
        module = tmp_path / name
        module.mkdir()
        (module / "go.mod").write_text("module fixture.test/seed\n\ngo 1.26.0\n")
        (module / "seed.go").write_text("package seed\nconst Value = 1\n")
        output = subprocess.check_output(["go", "list", "-export", "-json", "."], cwd=module, env=env, text=True)
        exports.append(json.loads(output)["Export"])
    assert exports[0] == exports[1], "equal seed source was recompiled after workspace relocation"
    (module / "seed.go").write_text("package seed\nconst Value = 2\n")
    changed = subprocess.check_output(["go", "list", "-export", "-json", "."], cwd=module, env=env, text=True)
    assert json.loads(changed)["Export"] != exports[0], "changed seed reused a stale compiler archive"


def test_preparation_publishes_only_the_verified_current_test_executable(tmp_path: Path) -> None:
    step = next(
        step
        for step in workflow()["jobs"]["prepare-go"]["steps"]
        if step.get("name") == "Keep only the source-bound test executable"
    )
    assert step["if"] == "matrix.mode == 'normal'"
    cache = tmp_path / ".fastloop/test-bin"
    cache.mkdir(parents=True)
    current = cache / "sdo-operator-current"
    current.write_text("current")
    (cache / "sdo-operator-stale").write_text("obsolete large executable")
    setup = cache / "setup"
    setup.write_text("bootstrap fixture")
    command = tmp_path / "python3"
    command.write_text(f"#!/bin/sh\nprintf '%s\\n' '{current}'\n")
    command.chmod(0o755)
    subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        check=True,
    )
    assert current.read_text() == "current"
    assert setup.read_text() == "bootstrap fixture"
    assert not (cache / "sdo-operator-stale").exists()


# Coverage-guided fuzzing compiles instrumented packages (~90 s) and then runs
# for its whole budget, so it never fits beside a test partition in a 5-minute
# PR job. PR CI replays every fuzz target's seed corpus through plain `go test`;
# the nightly workflow owns the campaigns.
FUZZ_FLAG = re.compile(r"(?<![\w-])-fuzz(?:\s|=)")
FUZZ_TIME = re.compile(r"-fuzztime[= ](\d+)s")
NIGHTLY_FUZZ_TARGETS = {
    "sdo/operator/sdk": {"FuzzObjectRefParseFormat", "FuzzObjectRefTypedRoundTrip"},
    "sdo/operator/outcome_derivation": {"FuzzControllerObservationBoundary"},
    "benchmarks/sregym/protocol/native_receipt_go": {"FuzzNativeReceiptBoundary"},
    "benchmarks/sregym/protocol/submission_go": {"FuzzSubmissionPhaseBoundary"},
}


def nightly_fuzz() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/nightly-fuzz.yml").read_text())


def test_pr_ci_replays_fuzz_seeds_without_coverage_guided_campaigns() -> None:
    runs = [step.get("run", "") for job in workflow()["jobs"].values() for step in job.get("steps", [])]
    assert not [run for run in runs if FUZZ_FLAG.search(run)]
    foundation = (ROOT / "scripts/test_go_foundation.sh").read_text()
    for directory in NIGHTLY_FUZZ_TARGETS:
        # Seeds run in the foundation modules or the exhaustive operator partitions.
        assert directory in foundation or directory.startswith("sdo/operator/")


def test_nightly_fuzz_campaigns_cover_each_target_within_one_job_budget() -> None:
    configuration = nightly_fuzz()
    assert "schedule" in configuration[True]  # YAML 1.1 reads the bare `on` key as True.
    job = configuration["jobs"]["fuzz"]
    assert 0 < job["timeout-minutes"] <= 5
    rows = job["strategy"]["matrix"]["include"]
    assert {row["directory"]: set(row["targets"].split()) for row in rows} == NIGHTLY_FUZZ_TARGETS
    run = next(step["run"] for step in job["steps"] if FUZZ_FLAG.search(step.get("run", "")))
    per_target = int(FUZZ_TIME.search(run).group(1))
    # One instrumented compile (~90 s) plus every campaign stays under 4 minutes.
    assert all(90 + per_target * len(row["targets"].split()) <= 240 for row in rows)
    # A missing target passes silently ("no fuzz tests to fuzz"), so pin each name.
    for directory, targets in NIGHTLY_FUZZ_TARGETS.items():
        sources = "".join(path.read_text() for path in (ROOT / directory).glob("*_test.go"))
        assert all(f"func {target}(" in sources for target in targets)


def test_all_integration_contracts_use_fixed_shared_capacity() -> None:
    jobs = workflow()["jobs"]
    integration = jobs["contracts"]
    assert integration["strategy"]["matrix"]["bin"] == [0, 1]
    assert any(step.get("with", {}).get("name") == "compiled-python-test-plan" for step in integration["steps"])
    assert set(jobs) == {"prepare-go", "python", "contracts", "required-pr-ci"}
    assert integration["timeout-minutes"] == 5
