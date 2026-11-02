"""Regenerate byte goldens from production prompt builders on synthetic fixtures."""

from tests.unit.sdo.agents.prompts.cases import rendered_cases
from tests.unit.sdo.agents.prompts.test_golden import GOLDENS, normalize_prompt


def main() -> None:
    prompts = rendered_cases()
    for name, prompt in prompts.items():
        (GOLDENS / f"{name}.md").write_bytes(normalize_prompt(prompt).encode())


if __name__ == "__main__":
    main()
