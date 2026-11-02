"""Shipped instructions must generalize beyond evaluation identities and faults."""

from __future__ import annotations

import ast
import re
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
# These identities also name general concepts used by the SDK and agent guidance.
GENERIC_TERMS = {
    "backend": "Generic server role in the SDK and incident guidance.",
    "catalog": "Generic database catalog and SDK registry concept.",
    "checkout": "Git checkout, unrelated to the shopping workload.",
    "configmap": "Kubernetes resource type used by the SDK.",
    "default": "Generic default values and the standard Kubernetes namespace.",
    "demo": "Synthetic detector example namespace.",
    "llm": "Generic language-model runtime prohibition.",
    "mongodb user": "Generic database principal in command help.",
    "rate": "Generic traffic and sampling rate.",
    "readiness": "Generic Kubernetes readiness observation.",
    "search": "Generic instruction to search evidence or memory.",
    "user": "Generic database principal or human user.",
}


def _variants(term: str) -> set[str]:
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", term).lower()
    words = re.sub(r"[-_ ]+", " ", words)
    return {term.lower(), words, words.replace(" ", "-"), words.replace(" ", "_")}


def _denied_terms(root: Path) -> set[str]:
    terms = {"SREGym", "benchmark verdict", "oracle"}
    apps = root / "apps"
    terms.update(path.name for path in apps.iterdir() if path.is_dir())
    # The app index also names nested applications inside uninitialized gitlinks.
    for name in re.findall(r"\[([^]]+)\]\(\./", (apps / "README.md").read_text()):
        terms.add(name)
        terms.add(name.rsplit("/", 1)[-1])
    seeds = root / "benchmarks/sregym/experiments/assurance/seeds"
    terms.update(
        re.sub(r"_(?:handwritten|[0-9a-f]{7,40})$", "", path.name)
        for path in seeds.iterdir()
        if path.is_dir() and path.name != "overlays"
    )
    # Concrete app manifests and seed manifests; do not parse vendored Helm templates.
    sources = sorted(
        {
            path
            for extension in ("yaml", "yml")
            for pattern in (
                f"**/kubernetes/**/*.{extension}",
                f"**/infrastructure/**/*.{extension}",
                f"**/seed/**/*.{extension}",
            )
            for path in apps.glob(pattern)
        }
        | {path for extension in ("yaml", "yml") for path in seeds.rglob(f"*.{extension}")}
    )
    assert sources, "No evaluation manifests found"
    for path in sources:
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict):
                continue
            pending = [doc]
            while pending:
                value = pending.pop()
                if isinstance(value, list):
                    pending.extend(value)
                elif isinstance(value, dict):
                    metadata = value.get("metadata", {})
                    if namespace := metadata.get("namespace", value.get("namespace")):
                        terms.add(namespace)
                    if value.get("kind") in {"Namespace", "Deployment", "Service"} and (
                        name := metadata.get("name", value.get("name"))
                    ):
                        terms.add(name)
                    pending.extend(value.values())
    # Handwritten app seeds declare their required workload identities in Go maps.
    for path in seeds.rglob("*.go"):
        source = path.read_text()
        for body in re.findall(
            r"required(?:Deployments|Services)\s*=\s*map\[string\]struct\{\}\{(.*?)\n\}", source, re.DOTALL
        ):
            terms.update(re.findall(r'"([^"\n]+)"\s*:', body))
        terms.update(re.findall(r'Namespace:\s*"([^"\n]+)"', source))
    # Read metadata rather than importing the benchmark adapter or external harness.
    catalog = root / "benchmarks/sregym/faults/probe_catalog.py"
    tree = ast.parse(catalog.read_text())
    fault_maps: list[ast.expr] = []
    # CLASS_KINDS includes a base dictionary and additive catalog updates.
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "CLASS_KINDS" for t in node.targets
        ):
            fault_maps.append(node.value)
        if (
            (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
            )
            and node.func.value.id == "CLASS_KINDS"
            and node.func.attr == "update"
        ):
            fault_maps.extend(node.args)
    assert fault_maps, f"Fault catalog structure changed: {catalog}"
    for mapping in fault_maps:
        values = ast.literal_eval(mapping)
        terms.update(values)
        terms.update(values.values())
    return {variant for term in terms for variant in _variants(term)}


@lru_cache(maxsize=8)
def _pattern(terms: tuple[str, ...]) -> re.Pattern[str]:
    return re.compile(r"(?<!\w)(?:" + "|".join(re.escape(term) for term in terms) + r")(?!\w)", re.IGNORECASE)


def _matches(text: str, terms: set[str]) -> list[str]:
    return sorted({match.group().lower() for match in _pattern(tuple(sorted(terms))).finditer(text)})


def _violations(root: Path, denied: set[str]) -> list[str]:
    files = sorted({*(root / "sdo/agents").glob("**/prompts/**/*"), *(root / "sdo/skills").rglob("*")})
    return [
        f"{path.relative_to(root)}: {', '.join(hits)}"
        for path in files
        if path.is_file() and (hits := _matches(path.read_text(), denied))
    ]


def test_shipped_prompts_and_skills_are_problem_agnostic() -> None:
    violations = _violations(ROOT, _denied_terms(ROOT) - GENERIC_TERMS.keys())
    assert not violations, "Evaluation-specific instructions:\n" + "\n".join(violations)


@pytest.mark.parametrize("text", ["CART", "cart-service", "SREGym", "benchmark verdict", "ORACLE"])
def test_identity_matching_negative_controls(text: str) -> None:
    assert _matches(text, {"cart", "sregym", "benchmark verdict", "oracle"})


def test_identity_matching_respects_word_boundaries() -> None:
    assert not _matches("discarded cartographer oracles", {"cart", "oracle"})


@pytest.mark.parametrize(
    "relative", ["sdo/agents/new-agent/prompts/nested/new.txt", "sdo/skills/new-skill/references/deep/example.go"]
)
def test_sweep_catches_new_files(tmp_path: Path, relative: str) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text("InjectedFaultClass")
    assert _violations(tmp_path, {"injectedfaultclass"}) == [f"{relative}: injectedfaultclass"]


def test_denylist_tracks_new_repository_metadata(tmp_path: Path) -> None:
    apps = tmp_path / "apps"
    manifests = apps / "synthetic_app/kubernetes"
    manifests.mkdir(parents=True)
    (apps / "README.md").write_text("[nestedApp](./suite/nestedApp)")
    (manifests / "new.yaml").write_text(
        "kind: Deployment\nmetadata: {name: synthetic-worker, namespace: synthetic-ns}\n"
        "---\nkind: Service\nmetadata: {name: synthetic-api}\n"
    )
    seeds = tmp_path / "benchmarks/sregym/experiments/assurance/seeds/seed_app_handwritten"
    seeds.mkdir(parents=True)
    (seeds / "provenance.yaml").write_text("active_topology: [{kind: Service, name: seed-api}]\n")
    (seeds / "detector.go").write_text('var requiredDeployments = map[string]struct{}{\n"seed-worker": {},\n}\n')
    catalog = tmp_path / "benchmarks/sregym/faults/probe_catalog.py"
    catalog.parent.mkdir(parents=True)
    catalog.write_text(
        'CLASS_KINDS = {"SyntheticFault": "synthetic_fault"}\nCLASS_KINDS.update({"AddedFault": "added_fault"})\n'
    )
    denied = _denied_terms(tmp_path)
    assert {
        "synthetic app",
        "nested app",
        "seed app",
        "synthetic-worker",
        "synthetic-ns",
        "synthetic-api",
        "seed-api",
        "seed-worker",
        "syntheticfault",
        "synthetic fault",
        "addedfault",
        "added_fault",
        "oracle",
        "sregym",
        "benchmark verdict",
    } <= denied
