"""One budget source for eval waits: ``wait_for`` is monotone in the owner's bound and the queue ahead."""

import ast
import re
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from benchmarks.sregym.adapter import SdoBounds, WaitKind, wait_for
from benchmarks.sregym.adapter.budgets import DEFAULT_BOUNDS, bound

ROOT = Path(__file__).resolve().parents[5]
SCANNED = ("benchmarks/sregym/fastloop", "benchmarks/sregym/adapter")
BUDGET_MODULE = "benchmarks/sregym/adapter/budgets.py"
WAIT_NAME = re.compile(r"(timeout|seconds|deadline|interval|poll|settle|wait|delay|backoff|ttl|grace)", re.IGNORECASE)

# The count of numeric seconds literals used as a wait or timeout. It may only decrease: lowering a
# value here is the review step that records a literal being moved onto ``wait_for``. What remains are
# the argparse defaults of the run CLIs and the image-bound modules that are COPYed into the worker image.
SECONDS_LITERAL_CEILING = 39


def _constant(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return isinstance(node.value, (int, float)) and not isinstance(node.value, bool)
    if isinstance(node, ast.UnaryOp):
        return _constant(node.operand)
    if isinstance(node, ast.BinOp):
        return _constant(node.left) and _constant(node.right)
    return False


def _zero(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value == 0


def _named(name: str | None) -> bool:
    return bool(name and WAIT_NAME.search(name))


def _target_name(target: ast.AST) -> str | None:
    return target.id if isinstance(target, ast.Name) else getattr(target, "attr", None)


def seconds_literals(source: str) -> list[tuple[int, str]]:
    """Every numeric literal (other than 0) that sets a wait, timeout, poll interval or deadline."""
    found: list[tuple[int, str]] = []

    def note(node: ast.AST, value: ast.AST, label: str) -> None:
        if _constant(value) and not _zero(value):
            found.append((value.lineno, f"{label}={ast.unparse(value)}"))

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if _named(keyword.arg):
                    note(node, keyword.value, keyword.arg)
            func = ast.unparse(node.func)
            if func in ("time.sleep", "sleep", "asyncio.sleep") and node.args:
                note(node, node.args[0], func)
            flag = node.args[0] if node.args else None
            if func.endswith("add_argument") and isinstance(flag, ast.Constant) and _named(str(flag.value)):
                for keyword in node.keywords:
                    if keyword.arg == "default":
                        note(node, keyword.value, str(flag.value))
            attribute = node.args[1] if len(node.args) == 3 else None
            if func == "getattr" and isinstance(attribute, ast.Constant) and _named(str(attribute.value)):
                note(node, node.args[2], str(attribute.value))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            positional = args.posonlyargs + args.args
            for arg, default in zip(positional[len(positional) - len(args.defaults) :], args.defaults, strict=True):
                if _named(arg.arg):
                    note(node, default, arg.arg)
            for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
                if default is not None and _named(arg.arg):
                    note(node, default, arg.arg)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if _named(_target_name(target)):
                    note(node, node.value, _target_name(target))
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            if _named(_target_name(node.target)):
                note(node, node.value, _target_name(node.target))
    return found


def _repo_literals() -> list[str]:
    found = []
    for directory in SCANNED:
        for path in sorted((ROOT / directory).rglob("*.py")):
            relative = path.relative_to(ROOT).as_posix()
            if relative == BUDGET_MODULE:
                continue
            found += [f"{relative}:{line} {label}" for line, label in seconds_literals(path.read_text())]
    return found


def test_seconds_literals_in_waits_only_decrease():
    literals = _repo_literals()
    assert len(literals) <= SECONDS_LITERAL_CEILING, (
        f"{len(literals)} seconds literals in fastloop/adapter, ceiling {SECONDS_LITERAL_CEILING}: "
        "derive new waits from wait_for(WaitKind...) instead\n" + "\n".join(literals)
    )
    assert len(literals) >= SECONDS_LITERAL_CEILING - 3, (
        f"only {len(literals)} literals remain: lower SECONDS_LITERAL_CEILING to lock in the gain"
    )


def test_the_scanner_sees_each_wait_shape():
    source = (
        "import time\n"
        "def run(command_timeout=30, *, poll_seconds=2.0, other=7): ...\n"
        "time.sleep(5)\n"
        "subprocess.run(cmd, timeout=60)\n"
        "deadline = 90\n"
        "parser.add_argument('--timeout', default=300)\n"
        "getattr(args, 'wait_seconds', 10)\n"
        "width = 80\n"
        "run(timeout=0)\n"
        "run(timeout=wait_for(WaitKind.POLL))\n"
    )
    labels = sorted(label for _, label in seconds_literals(source))
    assert labels == sorted(
        [
            "command_timeout=30",
            "poll_seconds=2.0",
            "time.sleep=5",
            "timeout=60",
            "deadline=90",
            "--timeout=300",
            "wait_seconds=10",
        ]
    )


kinds = st.sampled_from(list(WaitKind))
bounds_strategy = st.builds(
    SdoBounds,
    response_timeout_seconds=st.floats(min_value=1, max_value=7200),
    verification_timeout_seconds=st.floats(min_value=1, max_value=1800),
    retry_backoff_seconds=st.floats(min_value=0, max_value=600),
    closeout_seconds=st.floats(min_value=0, max_value=600),
)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(kind=kinds, queue=st.integers(min_value=0, max_value=50), sdo=bounds_strategy)
def test_a_wait_is_never_shorter_than_the_owners_bound_times_the_queue(kind, queue, sdo):
    assert wait_for(kind, queue, bounds=sdo) >= bound(kind, sdo) * queue
    assert wait_for(kind, queue, bounds=sdo) >= bound(kind, sdo)


@settings(max_examples=200, deadline=None)
@given(kind=kinds, queue=st.integers(min_value=1, max_value=49))
def test_a_longer_queue_never_shortens_the_wait(kind, queue):
    assert wait_for(kind, queue + 1) >= wait_for(kind, queue)


def test_negative_queue_and_bounds_are_rejected():
    with pytest.raises(ValueError, match="queue_len"):
        wait_for(WaitKind.POLL, -1)
    with pytest.raises(ValueError, match="response_timeout_seconds"):
        SdoBounds(response_timeout_seconds=0)
    with pytest.raises(ValueError, match="retry_backoff_seconds"):
        SdoBounds(retry_backoff_seconds=-1)


def test_two_reflections_one_backing_off_182_seconds_are_waited_out():
    """P50: a queue of two reflections, one retrying through the 182 s backoff, is two full attempts."""
    attempt = DEFAULT_BOUNDS.response_timeout_seconds + 182 + DEFAULT_BOUNDS.closeout_seconds
    assert bound(WaitKind.REFLECTION_ATTEMPT) == attempt
    assert wait_for(WaitKind.REFLECTION_ATTEMPT, 2) >= 2 * attempt
    # The fixed 600 s recovery wait that lost row 52/012 is far below it.
    assert wait_for(WaitKind.REFLECTION_ATTEMPT, 2) > 6 * wait_for(WaitKind.OPERATION)
