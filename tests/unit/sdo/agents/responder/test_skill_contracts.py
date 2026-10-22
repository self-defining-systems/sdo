"""Cheap drift checks for the guide an incident worker actually receives."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, get_args

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from sdo.agents.responder.credentials import AGENT_SKILLS, SKILLS_ROOT, install_skills
from sdo.agents.responder.reflection_authoring import OpenPhase
from sdo.agents.responder.reflection_validation import validation_server
from sdo.agents.responder.skill_contracts import CONTRACT_PATH, render_contract
from sdo.contracts import ReflectionResult, model_output_schema


@pytest.mark.asyncio
async def test_skill_contract_matches_live_decision_enums_and_tools(tmp_path: Path) -> None:
    assert CONTRACT_PATH.read_text() == await render_contract()
    tools = {tool.name: tool for tool in await validation_server(tmp_path).list_tools()}
    decision = tools["check_memory"].inputSchema["properties"]["learning_decision"]
    allowed = next(branch["enum"] for branch in decision["anyOf"] if "enum" in branch)
    assert allowed == model_output_schema(ReflectionResult.DESCRIPTOR)["properties"]["learning_decision"]["enum"]


@pytest.mark.asyncio
async def test_every_skill_reference_and_tool_link_resolves(tmp_path: Path) -> None:
    tools = {tool.name for tool in await validation_server(tmp_path).list_tools()}
    for file in SKILLS_ROOT.rglob("*.md"):
        text = file.read_text()
        for target in re.findall(r"\]\(([^)]+)\)", text):
            if "://" in target:
                continue
            relative, _, anchor = target.partition("#")
            resolved = file.parent / relative
            assert resolved.is_file(), (file, target)
            if resolved.name == "runtime.md" and anchor:
                assert anchor in tools, (file, anchor)
        for phase in re.findall(r"open_at\s*[=:]\s*[`\"]([^`\"\s,]+)", text):
            assert phase in get_args(OpenPhase), (file, phase)


def test_advertised_capture_phases_match_controller(tmp_path: Path) -> None:
    repository = SKILLS_ROOT.parents[1]
    loop = (repository / "sdo/operator/runtime/responder_loop.go").read_text()
    actual = set(re.findall(r'l\.capturePhase = "open@([^\"]+)"', loop))
    for file in (repository / "sdo/operator/runtime").glob("*.go"):
        if not file.name.endswith("_test.go"):
            actual.update(re.findall(r'CaptureIncidentSnapshot\([^\n]*?"open@([^\"]+)"', file.read_text()))
    assert actual <= set(get_args(OpenPhase))
    validator = (repository / "sdo/operator/memory/corpus_capture.go").read_text()
    accepted = set(re.findall(r'label\.Provenance\.OpenAt != "([^\"]+)"', validator))
    assert set(get_args(OpenPhase)) <= accepted
    controller = (repository / "sdo/operator/runtime/controller.go").read_text()
    assert '"open@detect"' in controller
    assert '"fixed"' in controller


@pytest.mark.parametrize("role", ["responder", "reflector"])
def test_worker_installs_linked_skill_contracts(tmp_path: Path, role: str) -> None:
    install_skills(tmp_path, AGENT_SKILLS[role])
    installed = tmp_path / "skills"
    assert (installed / "sdo-memory/references/runtime.md").read_text() == CONTRACT_PATH.read_text()
    for name in AGENT_SKILLS[role]:
        assert (installed / name / "SKILL.md").is_file()


@pytest.mark.asyncio
async def test_contract_is_stable_across_python_docstring_indentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import inspect

    from sdo.agents.responder import skill_contracts

    expected = await render_contract()
    server = validation_server(tmp_path)
    tools = await server.list_tools()
    for tool in tools:
        clean = inspect.cleandoc(tool.description or "")
        lines = clean.splitlines()
        if lines:
            # Python 3.12 retains this indentation in function.__doc__; newer
            # Python versions can strip it while compiling the function.
            tool.description = lines[0] + "\n" + "\n".join("        " + line for line in lines[1:]) + "\n        "

    async def raw_tools() -> object:
        return tools

    monkeypatch.setattr(server, "list_tools", raw_tools)
    monkeypatch.setattr(skill_contracts, "validation_server", lambda _: server)
    assert await render_contract() == expected
