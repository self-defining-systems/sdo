"""Reject test retry counts whose sleep budget is too small for loaded CI."""

from __future__ import annotations

import ast
import io
import subprocess
import tokenize
from pathlib import Path

import pytest


def _number(node: ast.AST) -> float | None:
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        value = _number(node.operand)
        return -value if value is not None else None
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return float(node.value)
    return None


def short_counted_polls(source: str) -> list[int]:
    tree = ast.parse(source)
    sleep_names = {"time.sleep", "asyncio.sleep"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"time", "asyncio"}:
                    sleep_names.add(f"{alias.asname or alias.name}.sleep")
        elif isinstance(node, ast.ImportFrom) and node.module in {"time", "asyncio"}:
            sleep_names.update(alias.asname or alias.name for alias in node.names if alias.name == "sleep")

    allowed = set()
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            _, marker, reason = token.string.partition("test-polling: allow -- ")
            if marker and reason.strip():
                allowed.update((token.start[0], token.start[0] + 1))

    def sleep_budget(node: ast.AST) -> float:
        if isinstance(node, (ast.For, ast.While, ast.AsyncFor, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return 0
        if isinstance(node, ast.Call) and ast.unparse(node.func) in sleep_names and len(node.args) == 1:
            return max(0, _number(node.args[0]) or 0)
        return sum(sleep_budget(child) for child in ast.iter_child_nodes(node))

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or node.lineno in allowed:
            continue
        call = node.iter
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "range":
            continue
        values = [_number(arg) for arg in call.args]
        if not 1 <= len(values) <= 3 or any(value is None or not value.is_integer() for value in values):
            continue
        counts = [int(value) for value in values if value is not None]
        try:
            count = len(range(*counts))
        except (ValueError, OverflowError):
            continue
        sleep = sum(sleep_budget(child) for child in node.body)
        if 0 < count * sleep < 5:
            found.append(node.lineno)
    return found


@pytest.mark.parametrize(
    "source",
    [
        "import time\nfor _ in range(100):\n if ready(): break\n time.sleep(.01)",
        "import asyncio as aio\nasync def f():\n for _ in range(200):\n  await aio.sleep(.001)",
        "from time import sleep as pause\nfor _ in range(1, 101):\n pause(.01)",
        "import time\nfor _ in range(100, 0, -1):\n time.sleep(.01)",
        "import time\n# test-polling: allow --\nfor _ in range(100):\n time.sleep(.01)",
    ],
)
def test_counted_polling_negative_controls(source: str) -> None:
    assert short_counted_polls(source)


@pytest.mark.parametrize(
    "source",
    [
        "import time\nfor _ in range(300):\n time.sleep(.1)",
        "import time\nwhile not ready():\n time.sleep(.01)",
        "import time\nfor _ in range(20):\n while not ready():\n  time.sleep(.01)",
        "import time\n# test-polling: allow -- deliberate event pacing\nfor _ in range(100):\n time.sleep(.01)",
        "for _ in range(100):\n step()",
    ],
)
def test_counted_polling_safe_controls(source: str) -> None:
    assert not short_counted_polls(source)


def test_no_short_counted_polling_in_python_tests() -> None:
    root = Path(__file__).resolve().parents[2]
    tracked = subprocess.run(
        ["git", "ls-files", "--", "*.py"], cwd=root, check=True, text=True, capture_output=True
    ).stdout.splitlines()
    failures = []
    for name in tracked:
        path = Path(name)
        if not (path.name.startswith("test_") or path.name.endswith("_test.py")) or "third_party" in path.parts:
            continue
        failures.extend(f"{name}:{line}" for line in short_counted_polls((root / path).read_text()))
    assert not failures, (
        "Counted sleep budget below 5s; wait for the asserted condition with a generous ceiling, "
        "or use test-polling: allow -- <reason> for intentional timing:\n" + "\n".join(failures)
    )
