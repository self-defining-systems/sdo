from __future__ import annotations

import configparser
import re
import subprocess
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
LEGACY_PATTERNS = (
    re.compile(r"\bSDS\b"),
    re.compile(r"Self-Defining Systems"),
    re.compile(r"\.sds(?:/|\b)"),
    re.compile(r"\bsds[-_.]", re.IGNORECASE),
    re.compile(r"\bSDS_"),
)


def _repository_files() -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
    )
    return [
        REPOSITORY_ROOT / raw.decode()
        for raw in completed.stdout.split(b"\0")
        if raw and not raw.startswith((b"third_party/sregym/",))
    ]


def test_tracked_repository_has_no_legacy_sds_names() -> None:
    violations: list[str] = []
    for path in _repository_files():
        if not path.is_file() or path == Path(__file__).resolve():
            continue
        relative = path.relative_to(REPOSITORY_ROOT).as_posix()
        for pattern in LEGACY_PATTERNS:
            if pattern.search(relative):
                violations.append(f"{relative}: legacy name in path")
                break
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            if any(pattern.search(line) for pattern in LEGACY_PATTERNS):
                violations.append(f"{relative}:{line_number}: {line.strip()}")
    assert not violations, "legacy SDS names remain:\n" + "\n".join(violations)


def test_ci_references_only_existing_repository_scripts() -> None:
    ci = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    referenced = sorted(set(re.findall(r"\./(scripts/[A-Za-z0-9_./-]+\.sh)", ci)))
    assert referenced, "CI workflow references no repository scripts"
    missing = [relative for relative in referenced if not (REPOSITORY_ROOT / relative).is_file()]
    assert not missing, f"CI references missing scripts: {missing}"


def test_python_implementation_uses_the_canonical_sdo_namespace() -> None:
    legacy_package = "app" + "_operator"
    required_packages = (
        "sdo/agents/lifecycle",
        "sdo/agents/responder",
        "sdo/agents/deployer",
        "sdo/agents/health_judge",
        "sdo/contracts",
    )

    legacy_paths = [
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in _repository_files()
        if path.exists() and legacy_package in path.relative_to(REPOSITORY_ROOT).parts
    ]

    assert not legacy_paths, f"legacy Python package paths remain: {legacy_paths}"
    assert all((REPOSITORY_ROOT / package).is_dir() for package in required_packages)

    violations: list[str] = []
    for path in _repository_files():
        if not path.is_file() or path == Path(__file__).resolve():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if legacy_package in text:
            violations.append(path.relative_to(REPOSITORY_ROOT).as_posix())
    assert not violations, f"legacy Python package references remain: {violations}"


def test_sregym_wrapper_uses_the_shared_benchmark_launcher() -> None:
    wrapper = (REPOSITORY_ROOT / "scripts/run_sregym.sh").read_text(encoding="utf-8")

    assert "python -m benchmarks.sregym.run" in wrapper
    assert not (REPOSITORY_ROOT / "scripts/run_sregym.py").exists()


def test_sregym_owned_code_has_one_explicit_benchmark_boundary() -> None:
    required = (
        "benchmarks/sregym/adapter",
        "benchmarks/sregym/analysis",
        "benchmarks/sregym/experiments",
        "benchmarks/sregym/protocol",
        "benchmarks/sregym/runner",
    )

    assert all((REPOSITORY_ROOT / path).is_dir() for path in required)
    assert not (REPOSITORY_ROOT / "benchmarks/sregym/agents").exists()
    assert not (REPOSITORY_ROOT / "sregym_agents").exists()
    assert not (REPOSITORY_ROOT / "libs/sregym_lib").exists()
    assert not (REPOSITORY_ROOT / "bench/sregym_analysis").exists()


def test_external_sregym_harness_is_a_third_party_submodule() -> None:
    modules = configparser.ConfigParser()
    modules.read(REPOSITORY_ROOT / ".gitmodules")

    section = 'submodule "third_party/sregym"'
    assert section in modules
    assert modules[section]["path"] == "third_party/sregym"
    assert 'submodule "bench/sregym"' not in modules

    tracked = subprocess.run(
        ["git", "ls-files", "--stage", "--", "third_party/sregym", "bench/sregym"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    assert len(tracked) == 1
    metadata, path = tracked[0].split("\t", maxsplit=1)
    assert metadata.split(maxsplit=1)[0] == "160000"
    assert path == "third_party/sregym"
