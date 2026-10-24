"""The runtime images copy only the ``benchmarks`` modules they import, not the answer key.

``sdo/operator/Dockerfile.runtime`` used to ``COPY benchmarks /opt/sdo/benchmarks`` wholesale
into the ``sregym-responder`` image. The responder runs ``codex exec`` with
``danger-full-access`` inside SREGym incidents, so that copy let it read the answer key:
``benchmarks/sregym/experiments/assurance/composites.toml`` (``correct_mitigation``,
``expected_diff``, ``partial_fix_trap``), the assurance plan, and
``benchmarks/sregym/assurance/scripted_codex/faults.py`` (repair commands).

These tests are static: they parse the Dockerfiles' ``COPY`` lines (no image is built) and
cross-check them against what the responder's entry points actually import, so a future
change that widens the copy — or narrows it below what the responder needs — fails here
instead of leaking the answer key or breaking the image.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
RUNTIME_DOCKERFILE = REPO_ROOT / "sdo" / "operator" / "Dockerfile.runtime"
BENCHMARK_DOCKERFILE = REPO_ROOT / "benchmarks" / "sregym" / "Dockerfile.worker"
SCRIPTED_DOCKERFILE = REPO_ROOT / "benchmarks" / "sregym" / "assurance" / "Dockerfile.scripted"

#: Directories the answer key lives under; no runtime image's copied tree may contain them.
FORBIDDEN_PREFIXES = (
    "benchmarks/sregym/experiments",
    "benchmarks/sregym/assurance",
    "docs/",
    "tests/",
)

ALLOWED_FASTLOOP_FILES = {
    "benchmarks/sregym/fastloop/__init__.py",
    "benchmarks/sregym/fastloop/model_recording.py",
    "benchmarks/sregym/fastloop/file_edits.py",
    "benchmarks/sregym/fastloop/recording_worker.py",
}
SCRIPTED_REPLAY_FILES = {
    "benchmarks/sregym/adapter/scripted_worker.py",
    "benchmarks/sregym/adapter/scripted_replay.py",
    "benchmarks/sregym/adapter/scripted_replay_catalog.py",
}
SCRIPTED_PACKAGE_FILES = {
    "benchmarks/__init__.py",
    "benchmarks/sregym/__init__.py",
    "benchmarks/sregym/adapter/__init__.py",
}
SCRIPTED_PROGRAM_FILES = {
    "benchmarks/sregym/fastloop/scripted_program/README.md",
    "benchmarks/sregym/fastloop/scripted_program/diagnose.go",
    "benchmarks/sregym/fastloop/scripted_program/mitigate.go",
    "benchmarks/sregym/fastloop/scripted_networkpolicy_program/README.md",
    "benchmarks/sregym/fastloop/scripted_networkpolicy_program/diagnose.go",
}

SCRIPTED_TOY_FILES = {
    "apps/toy/seed/.sdo/goal.md",
    "apps/toy/seed/.sdo/lifecycle-provenance.yaml",
    "apps/toy/seed/.sdo/diagnostics/detectors/health/toy-endpoints/detector.go",
    "apps/toy/seed/.sdo/diagnostics/detectors/health/toy-endpoints/manifest.yaml",
    "apps/toy/kubernetes/frontend.yaml",
    "apps/toy/kubernetes/backend.yaml",
    "apps/toy/kubernetes/catalog.yaml",
}

#: The scripted-Codex test wrapper is allowed to add exactly these, and nothing else that
#: FORBIDDEN_PREFIXES would otherwise catch. ``scripted_codex`` installs the program from
#: ``scripted_program`` (``programs.py`` ``PROGRAM_SOURCE``).
SCRIPTED_ALLOWANCES = (
    "benchmarks/sregym/assurance/scripted_codex",
    "benchmarks/sregym/fastloop/scripted_program",
)

_COPY_LOCAL = re.compile(r"^COPY\s+(?!--from=)(\S.*?)\s+(\S+)\s*$", re.MULTILINE)


def _local_copy_sources(dockerfile: Path) -> list[str]:
    """Every local (non ``--from=``) COPY source path in *dockerfile*, repo-root relative."""

    text = dockerfile.read_text(encoding="utf-8")
    sources: list[str] = []
    for match in _COPY_LOCAL.finditer(text):
        # A COPY line may list several sources before the final destination.
        tokens = match.group(1).split()
        sources.extend(tokens)
    return sources


def _stage_copy_sources(dockerfile: Path, stage: str) -> list[str]:
    text = dockerfile.read_text(encoding="utf-8").split(f" AS {stage}\n", 1)[1].split("\nFROM ", 1)[0]
    return [token for match in _COPY_LOCAL.finditer(text) for token in match.group(1).split()]


def _expand(paths: list[str]) -> set[str]:
    """*paths* (files or directories, repo-root relative) expanded to a flat set of files."""

    files: set[str] = set()
    for rel in paths:
        absolute = REPO_ROOT / rel
        if absolute.is_dir():
            files.update(
                str(p.relative_to(REPO_ROOT))
                for p in absolute.rglob("*")
                if p.is_file() and "__pycache__" not in p.parts  # git-ignored, never in the build context
            )
        elif absolute.is_file():
            files.add(rel)
        else:
            raise AssertionError(f"Dockerfile COPY source {rel!r} does not exist in this checkout")
    return files


def test_controller_and_plain_responder_stages_copy_no_benchmarks_code() -> None:
    """Only the ``sregym-responder`` stage may reference ``benchmarks``; production stays clean."""

    text = RUNTIME_DOCKERFILE.read_text(encoding="utf-8")
    before_sregym_responder, _, _ = text.partition("FROM responder AS sregym-responder")

    early_sources = [token for line in _COPY_LOCAL.findall(before_sregym_responder) for token in line[0].split()]
    benchmarks_before = [src for src in early_sources if src.startswith("benchmarks")]
    assert benchmarks_before == [], f"controller/responder stages must not copy benchmarks/: {benchmarks_before}"

    assert "sregym-responder" not in text
    assert "scripted-worker" not in text
    all_sources = _local_copy_sources(BENCHMARK_DOCKERFILE)
    assert any(src.startswith("benchmarks") for src in all_sources), "sregym-responder stage copies no benchmarks/"


def test_controller_image_is_separate_from_python_agent_runtime() -> None:
    text = RUNTIME_DOCKERFILE.read_text(encoding="utf-8")
    go_stage, controller = text.split("FROM runtime-sources AS controller", 1)
    controller = controller.split("FROM runtime-sources AS validator", 1)[0]
    assert "COPY --from=kubectl" in go_stage.split("AS runtime-base", 1)[1]
    assert "python3" not in controller
    assert "npm " not in controller
    assert "FROM worker-runtime AS responder" in text


def test_runtime_dockerfile_never_copies_the_whole_benchmarks_tree() -> None:
    sources = _local_copy_sources(RUNTIME_DOCKERFILE)

    assert "benchmarks" not in sources, "a bare 'COPY benchmarks ...' would copy the whole tree, answer key included"


def test_sregym_responder_image_copies_no_forbidden_paths() -> None:
    sources = [
        src for src in _stage_copy_sources(BENCHMARK_DOCKERFILE, "sregym-responder") if src.startswith("benchmarks")
    ]
    copied = _expand(sources)

    leaked = {path for path in copied if any(path.startswith(prefix) for prefix in FORBIDDEN_PREFIXES)}

    assert leaked == set(), f"sregym-responder image would copy answer-key paths: {sorted(leaked)}"
    assert {path for path in copied if path.startswith("benchmarks/sregym/fastloop/")} == ALLOWED_FASTLOOP_FILES
    scripted = _expand(_stage_copy_sources(BENCHMARK_DOCKERFILE, "scripted-worker"))
    assert scripted == SCRIPTED_REPLAY_FILES | SCRIPTED_PROGRAM_FILES | SCRIPTED_TOY_FILES
    assert SCRIPTED_REPLAY_FILES.isdisjoint(copied)
    guard_build = _expand(_stage_copy_sources(BENCHMARK_DOCKERFILE, "submission-guard-build"))
    assert {path for path in guard_build if path.startswith("benchmarks/")} == {
        "benchmarks/sregym/protocol/submission_go/go.mod",
        "benchmarks/sregym/protocol/submission_go/go.sum",
        "benchmarks/sregym/protocol/submission_go/guard.go",
        "benchmarks/sregym/protocol/submission_go/cmd/sdo-submission-guard/main.go",
    }
    assert all(path.startswith(("benchmarks/", "sdo/operator/contracts/")) for path in guard_build)
    assert guard_build.isdisjoint(copied)
    assert (
        "COPY --from=submission-guard-build /usr/local/bin/sdo-submission-guard /usr/local/bin/sdo-submission-guard"
        in BENCHMARK_DOCKERFILE.read_text(encoding="utf-8")
    )


def test_scripted_test_wrapper_adds_only_the_scripted_codex_and_nothing_from_experiments() -> None:
    sources = [src for src in _local_copy_sources(SCRIPTED_DOCKERFILE) if src.startswith("benchmarks")]
    copied = _expand(sources)

    leaked = {
        path
        for path in copied
        if any(path.startswith(prefix) for prefix in FORBIDDEN_PREFIXES) and not path.startswith(SCRIPTED_ALLOWANCES)
    }

    assert leaked == set(), f"the :assure scripted wrapper would copy answer-key paths: {sorted(leaked)}"
    assert copied, "the scripted wrapper copies nothing"
    assert all(
        path.startswith(SCRIPTED_ALLOWANCES) or path in SCRIPTED_REPLAY_FILES | SCRIPTED_PACKAGE_FILES
        for path in copied
    ), f"the scripted wrapper should add only {SCRIPTED_ALLOWANCES}, not {sorted(copied)}"
    assert {path for path in copied if path.startswith("benchmarks/sregym/fastloop/")} == {
        "benchmarks/sregym/fastloop/scripted_program/README.md",
        "benchmarks/sregym/fastloop/scripted_program/diagnose.go",
        "benchmarks/sregym/fastloop/scripted_program/mitigate.go",
    }


def _entry_point_module_closure(entry_points: tuple[str, ...]) -> set[str]:
    """Repo-relative files under ``benchmarks/`` a fresh interpreter loads to import *entry_points*.

    Runs in a subprocess (not the test process) so modules other tests already imported into
    ``sys.modules`` cannot inflate the closure and hide a missing or extra Dockerfile COPY.
    """

    script = (
        "import importlib, json, sys\n"
        f"for name in {entry_points!r}:\n"
        "    importlib.import_module(name)\n"
        "files = sorted(\n"
        "    m.__file__ for n, m in sys.modules.items()\n"
        "    if (n == 'benchmarks' or n.startswith('benchmarks.')) and getattr(m, '__file__', None)\n"
        ")\n"
        "print(json.dumps(files))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return {
        str(Path(f).resolve().relative_to(REPO_ROOT)) for f in json.loads(completed.stdout.strip().splitlines()[-1])
    }


def test_sregym_responder_copy_matches_exactly_what_its_entry_points_import() -> None:
    """The responder image's ``benchmarks`` COPY set is neither short nor padded with extras.

    ``python3 -m benchmarks.sregym.adapter.submission`` is the responder's submission smoke
    test (``scripts/build_sdo_images.sh``); ``adapter.submission_relay`` is what the responder
    image runs as the in-cluster submission bridge (``benchmarks.sregym.adapter.runtime``'s
    ``_submission_bridge_resources``, which points the bridge Deployment at ``responder_image``).
    """

    needed = _entry_point_module_closure(
        (
            "benchmarks.sregym.adapter.submission",
            "benchmarks.sregym.adapter.submission_relay",
            "benchmarks.sregym.fastloop.recording_worker",
        )
    )
    copied = _expand(
        [src for src in _stage_copy_sources(BENCHMARK_DOCKERFILE, "sregym-responder") if src.startswith("benchmarks")]
    )

    assert needed - copied == set(), f"the image is missing modules its entry points import: {needed - copied}"
    assert copied - needed == set(), f"the image copies files its entry points never import: {copied - needed}"


def test_scripted_worker_stage_contains_only_entry_points_programs_and_toy_health_fixture() -> None:
    copied = _expand(_stage_copy_sources(BENCHMARK_DOCKERFILE, "scripted-worker"))
    assert copied == {
        *SCRIPTED_REPLAY_FILES,
        *SCRIPTED_TOY_FILES,
        "benchmarks/sregym/fastloop/scripted_program/README.md",
        "benchmarks/sregym/fastloop/scripted_program/diagnose.go",
        "benchmarks/sregym/fastloop/scripted_program/mitigate.go",
        "benchmarks/sregym/fastloop/scripted_networkpolicy_program/README.md",
        "benchmarks/sregym/fastloop/scripted_networkpolicy_program/diagnose.go",
    }
