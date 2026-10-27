"""Hermetic import closure checks for image entrypoints used by benchmarks."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

BENCHMARK_IMAGE_ENTRY_POINTS = (
    "benchmarks.sregym.fastloop.recording_worker",
    "benchmarks.sregym.adapter.submission",
    "benchmarks.sregym.adapter.submission_relay",
)


def image_entrypoint_script(modules: Sequence[str]) -> str:
    return (
        "import importlib, importlib.metadata as m, json\n"
        "error = None\n"
        f"for name in {list(modules)!r}:\n"
        "    try:\n"
        "        importlib.import_module(name)\n"
        "    except Exception as exc:\n"
        "        error = f'{name}: {type(exc).__name__}: {exc}'\n"
        "        break\n"
        "try:\n"
        "    version = m.version('agentshim')\n"
        "except m.PackageNotFoundError:\n"
        "    version = None\n"
        "print(json.dumps({'agentshim': version, 'import_error': error}))\n"
    )


def _source_mount_args(source_root: Path, run: Callable[..., Any]) -> list[str]:
    """Use the exact production worker projection, including its file allowlist."""
    helper = Path(__file__).with_name("devmount_go") / "main.go"
    result = run(
        ["go", "run", str(helper)],
        cwd=source_root / "sdo/operator",
        check=True,
        capture_output=True,
        text=True,
        timeout=wait_for(WaitKind.SLOW_COMMAND),
    )
    paths = json.loads(result.stdout)
    args: list[str] = []
    for mount in paths:
        relative, target = mount["source"], mount["target"]
        if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ValueError("eval: invalid production worker source projection")
        source = source_root.resolve() / relative
        if not source.exists():
            raise ValueError(f"eval: worker source projection is missing: {source}")
        args.extend(["--mount", f"type=bind,source={source},target={target},readonly"])
    return args


def assert_image_entrypoints(
    ref: str,
    modules: Sequence[str],
    *,
    source_root: Path | None = None,
    run: Callable[..., Any] = subprocess.run,
) -> None:
    if not modules:
        result = run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--workdir",
                "/opt/sdo/sdo/operator",
                "--entrypoint",
                "/usr/local/bin/sdo-operator",
                ref,
                "schema-identity",
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
        if result.returncode or not result.stdout.strip().startswith("sdo.dev/"):
            raise ValueError(
                f"eval: image {ref} entrypoint smoke failed: {result.stderr.strip() or 'invalid Go schema identity'}"
            )
        return
    mounts = _source_mount_args(source_root, run) if source_root is not None else []
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            *mounts,
            "--entrypoint",
            "python3",
            ref,
            "-c",
            image_entrypoint_script(modules),
        ],
        check=False,
        text=True,
        capture_output=True,
        timeout=wait_for(WaitKind.SLOW_COMMAND),
    )
    try:
        error = json.loads(result.stdout.strip().splitlines()[-1]).get("import_error")
    except (IndexError, ValueError):
        error = result.stderr.strip() or "entrypoint import probe returned no valid result"
    if result.returncode or error:
        raise ValueError(f"eval: image {ref} entrypoint smoke failed: {error or result.stderr.strip()}")
