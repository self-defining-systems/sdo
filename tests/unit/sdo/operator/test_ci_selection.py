"""Affected-test selection for pull requests: the mapping from changed files to Go packages and Python tests.

Every selection miss (a test that fails on the merge queue or main but that the pull request run skipped)
gets a case here, in ``test_recorded_misses``.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

CI_SCOPED = "walks its own synthetic repositories and the selector itself"

ROOT = Path(__file__).resolve().parents[4]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


select = load_script("ci_select")

CONFIG = {
    "full_suite": [
        "go.work",
        "**/go.mod",
        "**/go.sum",
        "uv.lock",
        "pyproject.toml",
        ".github/**",
        "scripts/**",
        "**/*.proto",
        "**/*.pb.go",
        "tests/conftest.py",
        "tests/*.py",
        "tach.toml",
    ],
    "documentation": ["README.md"],
    "go_workspace_modules": ["goa", "gob"],
    "go_binaries": ["gob/cmd/tool"],
    "go_consumer_modules": [],
    "python_go_consumers": ["tests/unit/test_tool.py"],
    "python_shards": {"slow": ["tests/unit/slow/**"]},
    "go_cli_shard": ["goa/cli"],
    "go_validate_gate_tests": ["TestHeavy", "TestOther"],
}

FILES = {
    "goa/go.mod": "module m/a\n\ngo 1.26\n",
    "goa/a.go": "package a\n\nfunc A() int { return 1 }\n",
    "goa/a_test.go": 'package a\n\nimport "testing"\n\nfunc TestA(t *testing.T) { _ = A() }\n',
    "goa/leaf/leaf.go": "package leaf\n",
    "shared/schema.json": "{}\n",
    "gob/go.mod": "module m/b\n\ngo 1.26\n\nrequire m/a v0.0.0\n",
    "gob/b.go": 'package b\n\nimport "m/a"\n\nfunc B() int { return a.A() }\n',
    "gob/b_test.go": (
        'package b\n\nimport (\n\t"os"\n\t"testing"\n)\n\n'
        'func TestB(t *testing.T) {\n\t_, _ = os.ReadFile("../shared/schema.json")\n}\n'
    ),
    "gob/cmd/tool/main.go": 'package main\n\nimport "m/b"\n\nfunc main() { _ = b.B() }\n',
    "gob/other/other.go": "package other\n",
    "pkg/__init__.py": "",
    "pkg/x.py": "VALUE = 1\n",
    "pkg/y.py": "from pkg import x\n",
    "pkg/lonely.py": "LONE = 1\n",
    "pkg/by_name.py": "NAME = 1\n",
    "pkg/data.txt": "data\n",
    "tests/__init__.py": "",
    "tests/conftest.py": "",
    "tests/unit/__init__.py": "",
    "tests/unit/test_y.py": "import pkg.y\n",
    "tests/unit/test_by_string.py": 'SUBPROCESS = ["python", "-m", "pkg.by_name"]\n',
    "tests/unit/test_reads_data.py": 'PATH = "pkg/data.txt"\n',
    "tests/unit/test_other.py": "def test_nothing():\n    pass\n",
    "tests/unit/test_guard.py": "CI_WHOLE_TREE = True\n\n\ndef test_guard():\n    pass\n",
    "gob/other/guard_test.go": (
        'package other\n\nimport "testing"\n\n//ci:whole-tree\nfunc TestGuardScansTheTree(t *testing.T) {}\n\n'
        "func TestOther(t *testing.T) {}\n"
    ),
    "tests/unit/test_tool.py": "def test_tool():\n    pass\n",
    "tests/unit/slow/__init__.py": "",
    "tests/unit/slow/test_slow.py": "import pkg.lonely\n",
}


@pytest.fixture(scope="module")
def repo(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("selection-repo")
    for name, text in FILES.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    (root / "go.work").write_text("go 1.26\n\nuse (\n\t./goa\n\t./gob\n)\n")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    return root


@pytest.fixture(scope="module")
def config() -> select.Config:
    return select.Config(**CONFIG)


def plan(repo: Path, config: select.Config, *changed: str, deleted: tuple[str, ...] = ()) -> select.Plan:
    return select.plan_for(repo, config, list(changed), list(deleted))


@pytest.mark.parametrize(
    "path",
    [
        "go.work",
        "gob/go.mod",
        "goa/go.sum",
        "uv.lock",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "scripts/test_go_ci.sh",
        "proto/x.proto",
        "goa/gen/x.pb.go",
        "tests/conftest.py",
        "tests/helpers.py",
        "tach.toml",
    ],
)
def test_shared_inputs_select_the_full_suite(repo: Path, config: select.Config, path: str) -> None:
    result = plan(repo, config, path)
    assert result.full
    assert path in result.reasons[0]


def test_an_unattributable_file_selects_the_full_suite(repo: Path, config: select.Config) -> None:
    result = plan(repo, config, "docs/unknown.bin")
    assert result.full
    assert "cannot attribute" in result.reasons[0]


def test_deleting_a_non_test_file_selects_the_full_suite(repo: Path, config: select.Config) -> None:
    assert plan(repo, config, deleted=("pkg/x.py",)).full
    assert not plan(repo, config, deleted=("goa/a_test.go",)).full


def test_go_changes_select_reverse_dependencies_across_modules(repo: Path, config: select.Config) -> None:
    result = plan(repo, config, "goa/a.go")
    assert result.go_packages == ["goa", "gob", "gob/cmd/tool"]
    assert plan(repo, config, "goa/leaf/leaf.go").go_packages == ["goa/leaf"]
    assert plan(repo, config, "gob/other/other.go").go_packages == ["gob/other"]


def test_a_changed_go_test_selects_only_its_package(repo: Path, config: select.Config) -> None:
    assert plan(repo, config, "goa/a_test.go").go_packages == ["goa"]


def test_a_file_a_go_test_reads_selects_that_package(repo: Path, config: select.Config) -> None:
    assert plan(repo, config, "shared/schema.json").go_packages == ["gob"]


def test_python_changes_select_importers_and_name_mentions(repo: Path, config: select.Config) -> None:
    result = plan(repo, config, "pkg/x.py")
    assert "tests/unit/test_y.py" in result.python_tests
    assert "tests/unit/test_other.py" not in result.python_tests
    assert "tests/unit/test_by_string.py" not in result.python_tests
    assert "tests/unit/test_by_string.py" in plan(repo, config, "pkg/by_name.py").python_tests
    assert "tests/unit/test_reads_data.py" in plan(repo, config, "pkg/data.txt").python_tests


def test_marked_python_guards_run_with_every_change(repo: Path, config: select.Config) -> None:
    for changed in ("pkg/lonely.py", "goa/a_test.go", "README.md"):
        assert "tests/unit/test_guard.py" in plan(repo, config, changed).python_tests, changed


def test_marked_go_guards_run_in_packages_the_selection_skips(repo: Path, config: select.Config) -> None:
    result = plan(repo, config, "goa/leaf/leaf.go")
    assert "gob/other" not in result.go_packages
    assert result.go_guards == {"gob/other": ["TestGuardScansTheTree"]}
    assert select.shard_go_guards(result, config, "rest") == ("^(TestGuardScansTheTree)$", ["./gob/other"])
    assert select.shard_go_guards(result, config, "cli") == ("", [])
    assert select.shard_go_guards(result, config, "gate") == ("", [])
    # a package the plan already runs in full needs no separate guard run
    assert select.shard_go_guards(plan(repo, config, "gob/other/other.go"), config, "rest")[1] == []
    # documentation-only changes still run the guards
    assert plan(repo, config, "README.md").go_guards == result.go_guards


def test_the_plan_round_trips_its_guards(repo: Path, config: select.Config, tmp_path: Path) -> None:
    result = plan(repo, config, "goa/leaf/leaf.go")
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(result.to_json()))
    assert select.load_plan(path).go_guards == result.go_guards


def test_an_unmarked_tree_walker_fails_the_mark_check(tmp_path: Path) -> None:
    files = {
        "m/walk_test.go": 'package m\n\nfunc TestW(t *testing.T) { filepath.WalkDir("..", nil) }\n',
        "m/marked_test.go": 'package m\n\n//ci:whole-tree\nfunc TestM(t *testing.T) { filepath.WalkDir("..", nil) }\n',
        "m/scoped_test.go": 'package m\n\n//ci:scoped fixture\nfunc TestS() { filepath.WalkDir("..", nil) }\n',
        "m/tmp_test.go": "package m\n\nfunc TestT(t *testing.T) { filepath.WalkDir(t.TempDir(), nil) }\n",
        "tests/test_walk.py": 'def test_w():\n    subprocess.run(["git", "ls-files"])\n',
        "tests/test_marked.py": 'CI_WHOLE_TREE = True\ndef test_m():\n    subprocess.run(["git", "ls-files"])\n',
        "tests/test_scoped.py": 'CI_SCOPED = "temp repo"\ndef test_s():\n    subprocess.run(["git", "ls-files"])\n',
        "tests/test_plain.py": "def test_p():\n    pass\n",
    }
    for name, text in files.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text)
    bad = select.unmarked_tree_walkers(tmp_path, sorted(files))
    assert [b.split(":")[0] for b in bad] == ["m/walk_test.go", "tests/test_walk.py"]


def test_every_tree_walking_test_in_the_repository_is_marked() -> None:
    assert select.unmarked_tree_walkers(ROOT, select.tracked_files(ROOT)) == []


def test_a_change_only_under_operatorcli_still_runs_the_safefs_writer_guard() -> None:
    """The merge-queue ejection this class fix closes: PR CI skipped the whole-tree writer guard."""
    config = select.Config.load(ROOT)
    result = select.plan_for(ROOT, config, ["sdo/operator/operatorcli/memory.go"], [])
    assert not result.full
    assert "sdo/operator/safefs" not in result.go_packages
    assert "TestCheckoutWritersGoThroughSafefs" in result.go_guards["sdo/operator/safefs"]
    regex, packages = select.shard_go_guards(result, config, "rest")
    assert "TestCheckoutWritersGoThroughSafefs" in regex
    assert "./sdo/operator/safefs" in packages


def test_go_executable_inputs_select_the_python_tests_that_run_them(repo: Path, config: select.Config) -> None:
    assert "tests/unit/test_tool.py" in plan(repo, config, "goa/a.go").python_tests
    assert "tests/unit/test_tool.py" not in plan(repo, config, "gob/other/other.go").python_tests
    assert "tests/unit/test_tool.py" not in plan(repo, config, "goa/a_test.go").python_tests


def test_shards_partition_the_selected_tests(repo: Path, config: select.Config) -> None:
    result = plan(repo, config, "pkg/lonely.py")
    assert select.shard_python_tests(result, config, "slow") == ["tests/unit/slow/test_slow.py"]
    assert "tests/unit/slow/test_slow.py" not in select.shard_python_tests(result, config, "core")
    leaf = plan(repo, config, "goa/leaf/leaf.go")
    assert select.shard_go_packages(leaf, config, "rest") == ["./goa/leaf"]
    assert select.shard_go_packages(leaf, config, "validate") == []
    assert select.shard_go_packages(leaf, config, "cli") == []


def test_go_shards_partition_every_package_exactly_once(config: select.Config) -> None:
    dirs = ["goa/cli", "goa/cli/sub", "goa/clix", "goa/leaf", select.VALIDATE_DIR]
    owners = {d: select.go_shard_of(d, config) for d in dirs}
    assert owners == {
        "goa/cli": "cli",
        "goa/cli/sub": "cli",
        "goa/clix": "rest",
        "goa/leaf": "rest",
        select.VALIDATE_DIR: "validate",
    }
    root = Path("/repo")
    listing = "".join(f"m/{d} {root / d}\n" for d in dirs)
    chosen = [select.partition_go_packages(listing, root, config, s) for s in ("validate", "cli", "rest")]
    assert sorted(sum(chosen, [])) == sorted(f"m/{d}" for d in dirs)


def test_validator_package_is_split_by_test_between_validate_and_gate(config: select.Config) -> None:
    validator = select.Plan(go_packages=[select.VALIDATE_DIR])
    for shard in ("validate", "gate"):
        assert select.shard_go_packages(validator, config, shard) == [f"./{select.VALIDATE_DIR}"]
        assert select.shard_go_packages(validator, config, "rest") == []
    assert select.go_test_filter(config, "gate") == ("-run", "^(TestHeavy|TestOther)$")
    assert select.go_test_filter(config, "validate") == ("-skip", "^(TestHeavy|TestOther)$")
    assert select.go_test_filter(config, "rest") is None
    assert select.go_test_filter(config, "cli") is None


def test_the_real_gate_tests_exist_in_the_validator_package() -> None:
    names = select.Config.load(ROOT).go_validate_gate_tests
    assert names
    source = "\n".join(p.read_text() for p in (ROOT / select.VALIDATE_DIR).glob("*_test.go"))
    for name in names:
        assert re.search(rf"^func {name}\(t \*testing\.T\)", source, re.MULTILINE), name


def test_the_real_cli_shard_names_existing_directories() -> None:
    for directory in select.Config.load(ROOT).go_cli_shard:
        assert (ROOT / directory).is_dir(), directory


def test_misses_report_failures_the_pull_request_run_skipped(tmp_path: Path) -> None:
    log = tmp_path / "pytest.log"
    log.write_text("FAILED tests/unit/test_a.py::test_x - boom\nFAILED tests/unit/test_b.py::test_y\n")
    assert select.failed_python_tests(log) == {"tests/unit/test_a.py", "tests/unit/test_b.py"}
    go = tmp_path / "go.json"
    go.write_text(
        json.dumps({"Action": "fail", "Package": "m/a", "Test": "TestA"})
        + "\n"
        + json.dumps({"Action": "fail", "Package": "m/b"})
        + "\nnot json\n"
    )
    assert select.failed_go_packages(go) == {"m/a"}


def test_recorded_misses() -> None:
    """Add one case per selection miss: the changed file and a test it must select."""
    recorded: list[tuple[str, str]] = []
    assert recorded == [], "replace this with parametrized cases once a miss is recorded"


def test_the_shipped_rules_cover_the_shared_inputs() -> None:
    rules = select.Config.load(ROOT)
    for path in [
        "go.work",
        "uv.lock",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "scripts/ci_select.py",
        "sdo/operator/go.mod",
        "sdo/operator/go.sum",
        "sdo/contracts/foo.proto",
        "sdo/operator/contracts/gen/x/y.pb.go",
        "sdo/contracts/foo_pb2.py",
        "tests/conftest.py",
        "tests/reflection_memory.py",
        "tests/fixtures/x/y.txt",
        ".golangci.yml",
        "tach.toml",
        "buf.yaml",
    ]:
        assert select.matches(path, rules.full_suite), path


def test_the_shipped_go_rules_match_the_workspace_the_ci_script_tests() -> None:
    script = (ROOT / "scripts/test_go_ci.sh").read_text()
    block = re.search(r"modules=\((.*?)\)", script, re.DOTALL)
    assert block is not None
    rules = select.Config.load(ROOT)
    assert block.group(1).split() == rules.go_workspace_modules
    packages = {p.directory for p in select.go_packages(ROOT, rules.go_workspace_modules).values()}
    for binary in rules.go_binaries:
        assert binary in packages


def test_real_go_tests_that_read_files_outside_their_package_are_selected_by_them() -> None:
    rules = select.Config.load(ROOT)
    packages = select.go_packages(ROOT, rules.go_workspace_modules)
    by_dir = {p.directory: p for p in packages.values()}
    files = set(select.tracked_files(ROOT))
    mentions = select.go_file_mentions(ROOT, packages, files)
    for path, owner in [
        ("sdo/agents/responder/command_report_schema.json", "sdo.dev/operator/runtime"),
        ("tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod", "sdo.dev/operator/memory/validate"),
        ("sdo/operator/sdk/sdktest/recording.go", "sdo.dev/operator/memory/layout"),
    ]:
        assert owner in select.go_mentions_of(path, mentions, by_dir), path


def test_the_workflow_selects_on_pull_requests_and_keeps_one_aggregate_check() -> None:
    import yaml

    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    for name in ("python", "tests"):
        steps = jobs[name]["steps"]
        names = [step.get("name", "") for step in steps]
        planned = names.index("Select the tests this change can affect")
        assert "ci_select.py plan --event" in steps[planned]["run"]
    assert jobs["required-pr-ci"]["needs"] == ["checks", "python", "tests"]
    assert jobs["checks"]["name"] == "static checks"
    assert not any("ci_select" in step.get("run", "") for step in jobs["checks"]["steps"])


def test_the_planner_never_writes_the_shared_go_caches(tmp_path: Path) -> None:
    """The plan runs before the job restores its Go cache; files it left in the module cache break the restore."""
    env = select.private_go_caches(tmp_path)
    assert env["GOPROXY"] == "off"
    dirs = {Path(env["GOMODCACHE"]), Path(env["GOCACHE"])}
    assert {d.parent for d in dirs} == {tmp_path}
    assert not dirs & {Path.home() / "go/pkg/mod", Path.home() / ".cache/go-build"}
