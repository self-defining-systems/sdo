"""Discover registered implementations, template placeholders and tools, failing closed."""

from __future__ import annotations

import ast
import json
from pathlib import Path
from string import Template

import pytest

from scripts.check_skill_sizes import violations
from sdo.agents.context_output import budget_for
from sdo.agents.responder.reflection_validation import validation_server
from sdo.agents.responder.tools_cli import command_surface

ROOT = Path(__file__).resolve().parents[5]


def prompt_bypasses(source: str) -> list[int]:
    """Resolve imported symbols rather than scanning source text for snippets."""
    tree = ast.parse(source)
    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imports[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name] = alias.name

    def qualified(node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return imports.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            return qualified(node.value) + "." + node.attr
        return ""

    errors = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = qualified(node.func)
            if name in {"libs.agent_cli.structured.run_structured_turn", "string.Template", "agentshim.CliAgent"}:
                errors.append(node.lineno)
            if isinstance(node.func, ast.Attribute) and node.func.attr in {"substitute", "safe_substitute"}:
                errors.append(node.lineno)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == "prompt" for target in targets) and isinstance(
                node.value, (ast.Constant, ast.JoinedStr)
            ):
                errors.append(node.lineno)
    return errors


@pytest.mark.parametrize(
    "source",
    [
        "from libs.agent_cli.structured import run_structured_turn as send\nsend('codex', 'raw')",
        "import libs.agent_cli.structured as runner\nrunner.run_structured_turn('codex','raw')",
        "from string import Template as T\nT('raw').substitute(x=1)",
        "prompt = f'inline {123}'",
    ],
)
def test_prompt_bypass_negative_controls(source):
    assert prompt_bypasses(source)


def registered_modules() -> set[Path]:
    """A new registration must have inspectable implementation source, never silently skip it."""
    files = set()
    registrations = sorted((ROOT / "sdo/agents/registrations").glob("*.json"))
    assert registrations
    for registration in registrations:
        document = json.loads(registration.read_text())
        command = document["kubernetes_job"]["command"]
        assert "-m" in command, f"{registration}: add an inspectable Python implementation"
        module = command[command.index("-m") + 1]
        path = ROOT / (module.replace(".", "/") + ".py")
        assert path.is_file(), registration
        assert document["capabilities"], registration
        files.add(path)
    # The entire production tree is checked, including dependencies of all
    # current and future registered implementations, with no role allowlist.
    return files | set((ROOT / "sdo").rglob("*.py"))


def test_registered_agents_cannot_build_uncapped_prompts():
    errors = []
    for path in sorted(registered_modules()):
        if path == ROOT / "sdo/agents/prompt_templates.py":
            continue  # the single owner of template evaluation and provider dispatch
        errors.extend(f"{path.relative_to(ROOT)}:{line}" for line in prompt_bypasses(path.read_text()))
    assert not errors, "Use sdo.agents.prompt_templates for prompt construction/dispatch:\n" + "\n".join(errors)


def test_every_template_placeholder_has_a_cap_entry():
    manifest = json.loads((ROOT / "sdo/agents/prompt_sections.json").read_text())
    discovered = {}
    for path in sorted((ROOT / "sdo/agents").rglob("prompts/*.md")):
        key = f"{path.parents[1].name}/{path.stem}"
        template = Template(path.read_text())
        identifiers = {
            match.group("named") or match.group("braced")
            for match in template.pattern.finditer(template.template)
            if match.group("named") or match.group("braced")
        }
        assert key in manifest, f"{path}: missing placeholder budgets"
        assert identifiers == set(manifest[key]), path
        for budget_name in manifest[key].values():
            assert budget_for(budget_name) > 0, path
        discovered[key] = identifiers
    assert set(discovered) == set(manifest), "Remove stale placeholder budgets"


@pytest.mark.asyncio
async def test_every_registered_tool_has_output_and_description_caps(tmp_path):
    tools = await validation_server(tmp_path).list_tools()
    assert tools
    for tool in tools:
        assert len(tool.description or "") <= budget_for("tool_description." + tool.name)
        assert budget_for("tool_reply." + tool.name) > 0
    for command in command_surface()["commands"]:
        assert budget_for("tool_stdout." + command) > 0


def test_skill_glob_and_negative_control(tmp_path):
    assert not violations(ROOT / "sdo/skills")
    skill = tmp_path / "future"
    skill.mkdir()
    (skill / "SKILL.md").write_text("word " * 1201)
    (skill / "references").mkdir()
    (skill / "references/new.md").write_text("word " * 2001)
    (skill / "references/new.txt").write_text("x" * 12001)
    assert len(violations(tmp_path)) == 3


def test_every_registered_capability_has_a_production_render_fixture():
    from tests.unit.sdo.agents.prompts.cases import rendered_cases

    cases = rendered_cases()
    fixtures = {
        ("sdo-deployer", "source-deployment"): cases["deployer_source"],
        ("sdo-deployer", "source-architecture"): cases["lifecycle_DeployerDraft"],
        ("sdo-health-judge", "health-judgment-direct-v1"): cases["lifecycle_HealthJudgeDraft"],
        ("sdo-reflector", "outcome-reflection-direct-v1"): cases["reflection_resume_5"],
        ("sdo-responder", "incident-response"): cases["responder_oversized"],
    }
    registered = {
        (document["agent_id"], capability["task_type"])
        for path in (ROOT / "sdo/agents/registrations").glob("*.json")
        for document in [json.loads(path.read_text())]
        for capability in document["capabilities"]
    }
    assert registered == set(fixtures), "New implementations/capabilities need a production-rendered fixture"
    for prompt in fixtures.values():
        assert prompt.parts
        for part in prompt.parts:
            assert len(part.text) <= budget_for(part.name)


def test_all_template_placeholders_on_live_scale_values(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import render_prompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    original = "synthetic live evidence " * 8500
    manifest = json.loads((ROOT / "sdo/agents/prompt_sections.json").read_text())
    for key, placeholders in manifest.items():
        role, name = key.split("/")
        prompt = render_prompt(role, name, part=role + ".instructions", **dict.fromkeys(placeholders, original))
        totals = {}
        for part in prompt.parts:
            totals[part.name] = totals.get(part.name, 0) + len(part.text)
        for section, size in totals.items():
            assert size <= budget_for(section), key
