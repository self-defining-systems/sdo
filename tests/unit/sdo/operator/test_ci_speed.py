"""PR CI has a small fixed job graph; extended coverage remains nightly."""

from __future__ import annotations

<<<<<<< HEAD
=======
import fnmatch
import json
>>>>>>> origin/vic-lsh/dev/sdo-improve
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


<<<<<<< HEAD
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


=======
>>>>>>> origin/vic-lsh/dev/sdo-improve
@pytest.mark.parametrize(
    ("images", "expected"),
    [
        ("controller", {"sdo-controller"}),
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
    ("result", "accepted"), [("success", True), ("skipped", False), ("failure", False), ("cancelled", False)]
)
def test_required_ci_rejects_any_applicable_unsuccessful_job(result: str, accepted: bool) -> None:
<<<<<<< HEAD
    step = workflow()["jobs"]["required-pr-ci"]["steps"][0]
=======
    import json

    steps = workflow()["jobs"]["required-pr-ci"]["steps"]
    step = next(step for step in steps if "JOB_RESULTS" in step.get("env", {}))
    assert "if" not in step
    assert "continue-on-error" not in step
>>>>>>> origin/vic-lsh/dev/sdo-improve
    completed = subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "CHECKS": result, "TESTS": "success"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (completed.returncode == 0) is accepted


<<<<<<< HEAD
=======
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
    assert warm["timeout-minutes"] == 5
    assert any("scripts/prepare_go_ci.sh" in step.get("run", "") for step in warm["steps"])
    assert jobs["python"]["needs"] == "prepare-go"
    assert any(step.get("with", {}).get("name") == "prepared-python" for step in jobs["python"]["steps"])
    assert set(jobs["contracts"]["needs"]) == {"prepare-go", "prepare-go-race"}


def test_pull_requests_restore_a_prepared_cache_from_their_base_branch() -> None:
    """PR caches are private to refs/pull/N; only base-branch pushes can seed them."""
    configuration = workflow()
    # PyYAML reads the bare `on:` key as boolean True.
    pushed = configuration[True]["push"]["branches"]
    for base in ("main", "vic-lsh/dev/sdo-improve"):
        assert any(fnmatch.fnmatch(base, pattern.replace("**", "*")) for pattern in pushed), base
    # A cancelled base-branch run saves nothing, so only PR runs cancel each other.
    assert configuration["concurrency"]["cancel-in-progress"] == "${{ github.event_name == 'pull_request' }}"


def _saves_cache(step: dict) -> bool:
    uses = step.get("uses", "")
    return uses.startswith(("actions/cache@", "actions/cache/save@"))


def test_only_durable_base_pushes_save_go_caches() -> None:
    configuration = workflow()
    for job in configuration["jobs"].values():
        for step in job["steps"]:
            if _saves_cache(step) and not step.get("id", "").startswith("run-"):
                condition = step.get("if", "")
                assert "github.event_name == 'push'" in condition
                assert "github.ref == 'refs/heads/main'" in condition
                assert "github.ref == 'refs/heads/vic-lsh/dev/sdo-improve'" in condition
    prepare = [step for name in ("prepare-go", "prepare-go-race") for step in configuration["jobs"][name]["steps"]]
    publishers = [step for step in prepare if _saves_cache(step) and not step.get("id", "").startswith("run-")]
    assert len(publishers) == 2
    for publisher in publishers:
        mode = "race" if "race" in publisher["name"] else "normal"
        restore = next(step for step in prepare if step.get("id") == f"base-{mode}")
        assert publisher["with"]["key"] == f"${{{{ steps.base-{mode}.outputs.cache-primary-key }}}}"
        assert publisher["with"]["path"].strip() == restore["with"]["path"].strip()
        assert restore["with"]["key"].startswith(restore["with"]["restore-keys"].split()[0])


@pytest.mark.parametrize(
    ("job", "mode", "published", "downloaded"),
    [
        ("python", "python", "prepared-python", "prepared-python"),
    ],
)
def test_consumers_require_complete_run_artifacts_even_on_a_cold_base(
    job: str,
    mode: str,
    published: str,
    downloaded: str,
) -> None:
    jobs = workflow()["jobs"]
    upload = next(step for step in jobs["prepare-go"]["steps"] if step.get("with", {}).get("name") == published)
    assert upload["uses"].startswith("actions/upload-artifact@")
    assert upload["with"]["if-no-files-found"] == "error"
    steps = jobs[job]["steps"]
    assert any(
        (step.get("with", {}).get("pattern") or step.get("with", {}).get("name")) == downloaded for step in steps
    )
    assert not any(step.get("uses", "").startswith("actions/cache") for step in steps)
    assert any(f"go_ci_artifacts.sh unpack {mode}" in step.get("run", "") for step in steps)
    if mode == "python":
        assert "compiler" not in upload["with"]["path"]
        assert "modules.tar.zst" in upload["with"]["path"]
        assert "tools.tar.zst" in upload["with"]["path"]


def test_go_test_jobs_never_download_modules(tmp_path: Path) -> None:
    """Controller jobs run offline on the module graph that preparation caches."""
    controller = workflow()["jobs"]["contracts"]
    run = next(step for step in controller["steps"] if step.get("name") == "Run the fixed regression set")
    assert run["env"]["GOPROXY"] == "off"
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
    download_commands = [
        shlex.split(args)
        for line in calls.read_text().splitlines()
        for _, args in [line.split(":", 1)]
        if shlex.split(args)[:2] == ["mod", "download"]
    ]
    assert download_commands
    assert all(args[2] == "-modfile" and args[3].endswith(".mod") for args in download_commands)
    downloaded = {
        directory
        for directory, args in (line.split(":", 1) for line in calls.read_text().splitlines())
        if shlex.split(args)[:2] == ["mod", "download"] and shlex.split(args)[-1] == "all"
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
        if mode == "race":
            assert all(args[:2] == ["mod", "download"] for args in commands)
            assert any(
                shlex.split(args) == ["run", str(ROOT / "scripts/go_test_inventory.go"), str(ROOT / module)]
                for _, args in invocations
            )
        else:
            assert commands[0] == ["build", f"-p={os.environ.get('GOMAXPROCS', '2')}", "./..."]
            assert not any(args[0] == "test" for args in commands)
            if module.endswith("/runtime"):
                assert ["build", "./prober"] in commands
                assert any(args[0] == "vet" for args in commands)


def test_normal_and_race_preparation_are_independent_bounded_jobs() -> None:
    jobs = workflow()["jobs"]
    preparation = jobs["prepare-go"]
    assert "strategy" not in preparation
    assert "strategy" not in jobs["prepare-go-race"]
    caches = [
        step
        for owner in ("prepare-go", "prepare-go-race")
        for step in jobs[owner]["steps"]
        if step.get("uses", "").startswith("actions/cache/save@") and not step.get("id", "").startswith("run-")
    ]
    assert len(caches) == 2
    assert all("github.event_name == 'push'" in step["if"] for step in caches)
    assert sum("~/go/pkg/mod" in step["with"]["path"] for step in caches) == 1


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
    for name in ("prepare-go",):
        normal = [
            step
            for step in jobs["jobs"][name]["steps"]
            if step.get("uses", "").startswith("actions/cache") and "ci-prepared-v4-normal-" in step["with"]["key"]
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
        if name == "second":
            env["GOFLAGS"] = env["GOFLAGS"].replace("-p=2", "-p=4")
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
    assert "if" not in step
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
    assert any(
        "python_cost_plan.py --inventory-only" in step.get("run", "") for step in jobs["prepare-go-race"]["steps"]
    )
    assert set(jobs) == {"prepare-go", "prepare-go-race", "python", "contracts", "required-pr-ci"}
    assert integration["timeout-minutes"] == 4


def test_preparation_uses_available_compiler_cpu_without_changing_build_identity() -> None:
    configuration = workflow()
    preparation = configuration["jobs"]["prepare-go"]
    assert preparation["env"]["GOMAXPROCS"] == "4"
    assert "GOFLAGS" not in preparation["env"]
    assert configuration["env"]["GOFLAGS"] == "-p=2 -trimpath"
    scheduling = next(
        step
        for step in configuration["jobs"]["prepare-go-race"]["steps"]
        if step.get("name") == "Use all preparation CPUs for the race graph"
    )
    assert "if" not in scheduling
    assert "GOFLAGS=-p=4 -trimpath" in scheduling["run"]


def test_static_python_job_cannot_compile_go_and_go_checks_run_on_prepared_race_graph() -> None:
    jobs = workflow()["jobs"]
    static = next(step for step in jobs["prepare-go"]["steps"] if step.get("name") == "Check static contracts")
    assert "--python-only" in static["run"]
    assert "if" not in static
    guard = next(step for step in jobs["python"]["steps"] if step.get("name") == "Enforce compiler-free Python jobs")
    assert "scripts/unit-tools" in guard["run"]
    assert "SDO_UNIT_REAL_GO" in guard["run"]
    assert not any("scripts/link_cache.sh" in step.get("run", "") for step in jobs["python"]["steps"])
    go_checks = next(
        step
        for step in jobs["prepare-go-race"]["steps"]
        if step.get("name") == "Check Go static contracts and prepare selected executables"
    )
    assert "if" not in go_checks
    assert "--go-only" in go_checks["run"]
    assert 'GOFLAGS="$GOFLAGS -race"' in go_checks["run"]
    assert 'wait "$quality_pid"' in go_checks["run"]
    assert 'eval "$(bash scripts/link_cache.sh --shell)"' in go_checks["run"]


def test_contracts_consume_prepared_inventories_without_rebuilding() -> None:
    contracts = workflow()["jobs"]["contracts"]
    assert set(contracts["needs"]) == {"prepare-go", "prepare-go-race"}
    commands = "\n".join(step.get("run", "") for step in contracts["steps"])
    assert "prepare_go_ci.sh" not in commands
    assert "--prepared" in commands
    preparation = "\n".join(step.get("run", "") for step in workflow()["jobs"]["prepare-go-race"]["steps"])
    assert "go_cost_plan.py --prepared" in preparation
    assert "python_cost_plan.py --inventory-only" in preparation
    assert "scripts/prepared_contracts.py" in preparation


>>>>>>> origin/vic-lsh/dev/sdo-improve
def assert_native_jobs_use_local_builds(configuration: dict) -> None:
    for name, job in configuration["jobs"].items():
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        if "go test " not in commands and "scripts/run_ci_contracts.py" not in commands:
            continue
        assert not any(
            step.get("uses", "").startswith(("actions/download-artifact@", "actions/upload-artifact@"))
<<<<<<< HEAD
            for step in job["steps"]
        ), f"{name}: native tests must build locally from Go caches; artifact transfers consume their deadline"
=======
            and step.get("with", {}).get("name") != "prepared-contract-${{ matrix.bin }}"
            for step in job["steps"]
        ), f"{name}: native tests must build locally from prepared Go caches; artifact transfers consume their deadline"
>>>>>>> origin/vic-lsh/dev/sdo-improve


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


<<<<<<< HEAD
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
    fallbacks = restores[0]["with"]["restore-keys"].splitlines()
    assert fallbacks[0].startswith("ci-tests-")
    # go.sum discovery can change during a job (e.g. submodule initialization).
    # Content-addressed compiler entries remain safe across dependency changes.
    assert "ci-tests-v2-${{ runner.os }}-${{ runner.arch }}-1.26.8-" in fallbacks
    assert "ci-shared-v1-${{ runner.os }}-${{ runner.arch }}-1.26.8-" in fallbacks


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
            "TestCompileStageDiagnostics",
            "TestSimTrustedFastPathNeverRollsBackACorrectFix",
            "TestMemoryHostWorkspaceStagesAcceptedMemory",
        ):
            assert not re.search(skip, name)
    else:
        skip = invocations[0][invocations[0].index("-skip") + 1]
        assert all(not re.search(skip, name) for name in nightly_names)


@pytest.mark.parametrize("script", ["test_fastloop_hermetic.sh", "run_tests.sh"])
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
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "SDO_TEST_CALLS": str(calls)},
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
 printf 'uv %s\\n' "$*" >> "$SDO_TEST_CALLS"''',
    }.items():
        command = tmp_path / name
        command.write_text(f"#!/bin/sh\nset -eu\n{body}\n")
        command.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/test_python.sh"), "--durations=30"],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "SDO_TEST_CALLS": str(calls)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    lines = calls.read_text().splitlines()
    assert len([line for line in lines if line.startswith("go build ")]) == 2
    assert lines[-1].startswith("uv run --extra test pytest ")
    assert "--durations=30" in lines[-1]


def test_architecture_delegation_requires_the_full_python_suite() -> None:
    commands = "\n".join(step.get("run", "") for step in workflow()["jobs"]["checks"]["steps"])
    assert "check_errors.sh --python-suite" in commands
    assert "scripts/test_python.sh --durations=30" in commands
    assert "--tach" not in commands
    assert "--ignore=tests/unit/test_architecture.py" not in commands
=======
def test_shared_quality_checks_do_not_consume_python_test_deadlines() -> None:
    jobs = workflow()["jobs"]
    quality_names = {"Check formatting", "Check static contracts"}
    assert not any(step.get("name") in quality_names for step in jobs["python"]["steps"])
    quality = [step for step in jobs["prepare-go"]["steps"] if step.get("name") in quality_names]
    assert {step["name"] for step in quality} == quality_names
    assert all("if" not in step for step in quality)
    assert any("--python-only" in step.get("run", "") for step in quality)


def test_contract_preparation_does_not_repeat_full_normal_graph() -> None:
    commands = "\n".join(step.get("run", "") for step in workflow()["jobs"]["contracts"]["steps"])
    assert "prepare_go_ci.sh normal" not in commands
    assert "prepare_go_ci.sh" not in commands


def test_contract_cache_restores_overlap() -> None:
    steps = workflow()["jobs"]["contracts"]["steps"]
    assert not any(step.get("uses", "").startswith("actions/cache/restore@") for step in steps)
    assert any(step.get("uses") == "./.github/actions/restore-go-caches" for step in steps)


def test_parallel_cache_client_contracts() -> None:
    subprocess.run(["node", "--test", str(ROOT / ".github/actions/restore-go-caches/restore.test.cjs")], check=True)


def test_cache_union_accepts_a_cold_or_evicted_archive(tmp_path: Path) -> None:
    normal, race = tmp_path / "normal", tmp_path / "race"
    subprocess.run(["bash", str(ROOT / "scripts/merge_go_ci_caches.sh"), str(normal), str(race)], check=True)
    assert normal.is_dir()
    assert race.is_dir()


def assert_contracts_use_exact_preparation(configuration: dict) -> None:
    jobs = configuration["jobs"]
    assert set(jobs["contracts"].get("needs", [])) == {"prepare-go", "prepare-go-race"}
    consumers = json.loads(
        next(
            step["with"]["caches"]
            for step in jobs["contracts"]["steps"]
            if step.get("uses") == "./.github/actions/restore-go-caches"
        )
    )
    publishers = [step for step in jobs["prepare-go"]["steps"] if step.get("id", "").startswith("run-")]
    assert len(publishers) == len(consumers) == 1
    assert "sdo-go-build-race" not in consumers[0]["path"]
    for consumer in consumers:
        assert consumer["required"] is True
        assert not consumer.get("restore-keys")
        assert "github.run_id" in consumer["key"]
        assert "github.sha" in consumer["key"]
        assert any(step["with"] == {"path": consumer["path"], "key": consumer["key"]} for step in publishers)
    commands = "\n".join(step.get("run", "") for step in jobs["contracts"]["steps"])
    assert "prepare_go_ci.sh" not in commands
    restore = next(
        step for step in jobs["contracts"]["steps"] if step.get("uses") == "./.github/actions/restore-go-caches"
    )
    assert "if" not in restore


def test_contracts_require_exact_same_run_preparation_without_rebuilding() -> None:
    assert_contracts_use_exact_preparation(workflow())


@pytest.mark.parametrize("mutation", ["dependency", "miss", "fallback", "key", "path", "rebuild", "cold"])
def test_exact_preparation_guard_rejects_cache_dependent_contracts(mutation: str) -> None:
    configuration = workflow()
    contracts = configuration["jobs"]["contracts"]
    restore = next(step for step in contracts["steps"] if step.get("uses") == "./.github/actions/restore-go-caches")
    consumers = json.loads(restore["with"]["caches"])
    if mutation == "dependency":
        del contracts["needs"]
    elif mutation == "rebuild":
        contracts["steps"].append({"run": "bash scripts/prepare_go_ci.sh normal"})
    elif mutation == "cold":
        restore["if"] = "inputs.cold != true"
    else:
        if mutation == "miss":
            consumers[0]["required"] = False
        elif mutation == "fallback":
            consumers[0]["restore-keys"] = "older-run"
        else:
            consumers[0][mutation] += "-different"
        restore["with"]["caches"] = json.dumps(consumers)
    with pytest.raises(AssertionError):
        assert_contracts_use_exact_preparation(configuration)


def test_python_waits_only_for_its_normal_producer_without_growing_jobs() -> None:
    from scripts.check_ci_budget import cardinality

    jobs = workflow()["jobs"]
    assert sum(cardinality(job) for job in jobs.values()) == 7
    assert cardinality(jobs["prepare-go"]) == 1
    assert jobs["python"]["needs"] == "prepare-go"
    assert set(jobs["contracts"]["needs"]) == {"prepare-go", "prepare-go-race"}
    assert "prepare-go-race" in jobs["required-pr-ci"]["needs"]


@pytest.mark.parametrize(
    ("quality_status", "inventory_status", "compile_status"), [(0, 0, 0), (23, 0, 0), (0, 29, 0), (0, 0, 31)]
)
def test_race_archives_overlap_quality_but_cannot_publish_after_failure(
    tmp_path, quality_status, inventory_status, compile_status
):
    """Execute the workflow shell; a failing check still rejects finished archives."""
    steps = workflow()["jobs"]["prepare-go-race"]["steps"]
    command = next(
        step["run"]
        for step in steps
        if step.get("name") == "Check Go static contracts and prepare selected executables"
    )
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "link_cache.sh").write_text("#!/bin/bash\n")
    # The quality check can finish only once both archives exist. A bounded
    # read makes a serial-wait regression fail without leaving a child behind.
    fifo = tmp_path / "packed"
    os.mkfifo(fifo)
    started = tmp_path / "quality-started"
    os.mkfifo(started)
    discovered = tmp_path / "discovery-started"
    os.mkfifo(discovered)
    # Run the real readiness producer into the real workflow consumer. Only
    # the compiler boundary is synthetic: runtime compilation waits for the
    # quality worker to start, so losing overlap fails deterministically.
    (scripts / "prepare_go_ci.sh").write_bytes((ROOT / "scripts/prepare_go_ci.sh").read_bytes())
    (scripts / "check_errors.sh").write_text(
        f'#!/bin/bash\necho started > "{started}"\n'
        'read -r -t 3 ready < "$SDO_GO_RUNTIME_READY" || exit 96\n'
        f'read -r -t 3 marker <> "{fifo}" || exit 99\nexit {quality_status}\n'
    )
    (tmp_path / "nightly-tests.md").write_text("synthetic manifest")
    for index in range(2):
        (tmp_path / "prepared-contracts" / str(index)).mkdir(parents=True)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name in ("uv", "python3"):
        path = bindir / name
        path.write_text(
            '#!/bin/bash\ncase "$*" in *python_cost_plan.py*) '
            f'echo discovered > "{discovered}"; exit {inventory_status};; esac\n'
        )
        path.chmod(0o755)
    go = bindir / "go"
    go.write_text(
        '#!/bin/bash\ncase "$*" in *go_test_inventory.go*runtime*)\n'
        f'read -r -t 3 marker <> "{started}" || exit 98\ntouch "{tmp_path}/graph-finished"\nexit {compile_status};;\n'
        f'*go_test_inventory.go*) read -r -t 3 marker <> "{discovered}" || exit 97;;\nesac\n'
    )
    go.chmod(0o755)
    # Compression itself is exercised by the real tar/zstd. Signal quality
    # only after the workflow's second du, which follows the second tar.
    du = bindir / "du"
    du.write_text(f'#!/bin/bash\nif [[ "$*" == *contract-1.tar.zst* ]]; then echo packed 1<> "{fifo}"; fi\n')
    du.chmod(0o755)
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", command],
        cwd=tmp_path,
        env={**os.environ, "RUNNER_TEMP": str(tmp_path), "PATH": f"{bindir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == (compile_status or inventory_status or quality_status), result.stderr
    assert (tmp_path / "graph-finished").is_file()
    for index in range(2):
        assert (tmp_path / f"contract-{index}.tar.zst").is_file() == (not compile_status and not inventory_status)
>>>>>>> origin/vic-lsh/dev/sdo-improve
