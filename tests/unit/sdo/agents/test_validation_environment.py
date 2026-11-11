"""The agent's in-turn memory check sees every setting the worker's final check sees."""

import re
from pathlib import Path

from sdo.agents.responder.reflection_validation import VALIDATION_ENVIRONMENT
from sdo.agents.responder.session_turn import session_mcp_servers

OPERATOR = Path(__file__).resolve().parents[4] / "sdo" / "operator"
# The `sdo-operator memory check` command and the policy it reads.
CHECK_SOURCES = ("operatorcli/memory.go", "operatorcli/telemetry_policy.go")


def _constant(name: str) -> str:
    """Resolve a Go string constant (`name = "X"` or `name = pkg.Other`) in the operator tree."""
    short = name.rsplit(".", 1)[-1]
    pattern = re.compile(rf"\b{re.escape(short)}\s*(?:string\s*)?=\s*(\"[A-Z_]+\"|[\w.]+)")
    for path in sorted(OPERATOR.rglob("*.go")):
        if path.name.endswith("_test.go") or path.name.endswith(".pb.go"):
            continue
        match = pattern.search(path.read_text(encoding="utf-8"))
        if match:
            value = match.group(1)
            return value.strip('"') if value.startswith('"') else _constant(value)
    raise AssertionError(f"Go constant {name} not found")


def _check_environment() -> set[str]:
    names: set[str] = set()
    for source in CHECK_SOURCES:
        text = (OPERATOR / source).read_text(encoding="utf-8")
        for argument in re.findall(r"getenv\(([^)]*)\)", text):
            names.add(argument.strip('"') if argument.startswith('"') else _constant(argument))
    return names


def test_in_turn_check_receives_every_variable_the_memory_check_reads() -> None:
    read = _check_environment()
    assert "SDO_TELEMETRY_POLICY" in read
    assert read - set(VALIDATION_ENVIRONMENT) == set()


def test_validation_server_inherits_the_worker_telemetry_policy(tmp_path: Path, monkeypatch) -> None:
    policy = '{"mode":"open","provided":[{"ID":"prom","Capabilities":["alerts"]}]}'
    monkeypatch.setenv("SDO_TELEMETRY_POLICY", policy)
    monkeypatch.setenv("SDO_MEMORY_VALIDATION", "go")
    monkeypatch.setenv("SDO_CODE_PATH", "default")
    (server,) = session_mcp_servers(tmp_path, "codex")
    assert server.env["SDO_TELEMETRY_POLICY"] == policy
    assert server.env["SDO_MEMORY_VALIDATION"] == "go"
    assert server.env["SDO_CODE_PATH"] == "default"
