#!/usr/bin/env python3
"""Check installed model CLIs against image defaults and freeze measured versions."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


def version(value: str) -> tuple[int, ...]:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        raise ValueError(f"expected a concrete stable CLI version, got {value!r}")
    return tuple(map(int, value.split(".")))


@dataclass(frozen=True)
class ModelPolicy:
    default_models: dict[str, list[str]]
    minimum_cli_versions: dict[str, str]

    def __post_init__(self) -> None:
        if set(self.default_models) != {"claude", "codex"}:
            raise ValueError("default_models must declare claude and codex")
        for models in self.default_models.values():
            if (
                not isinstance(models, list)
                or not models
                or any(not isinstance(m, str) or not m.strip() for m in models)
            ):
                raise ValueError("each provider must declare a nonempty default model list")
        if "claude" not in self.minimum_cli_versions or self.minimum_cli_versions.keys() - self.default_models.keys():
            raise ValueError("minimum_cli_versions must include claude and name only declared providers")
        for minimum in self.minimum_cli_versions.values():
            version(minimum)


def measure(command: str, pattern: str) -> str:
    result = subprocess.run([command, "--version"], capture_output=True, text=True, check=True, timeout=30)
    match = re.fullmatch(pattern, result.stdout.strip())
    if match is None:
        raise ValueError(f"cannot measure {command} --version: expected a stable release version")
    return match[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--claude-code-version", required=True)
    parser.add_argument("--codex-version", required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    args = parser.parse_args()
    try:
        policy = ModelPolicy(**json.loads(args.requirements.read_text(encoding="utf-8")))
        versions = {
            "claude": measure("claude", r"([0-9]+\.[0-9]+\.[0-9]+) \(Claude Code\)"),
            "codex": measure("codex", r"codex-cli ([0-9]+\.[0-9]+\.[0-9]+)"),
        }
        for provider, minimum in policy.minimum_cli_versions.items():
            if version(versions[provider]) < version(minimum):
                label = "Claude Code" if provider == "claude" else "Codex"
                raise ValueError(
                    f"{label} {versions[provider]}: {', '.join(policy.default_models[provider])} "
                    f"requires >={minimum}; update the Dockerfile CLI pin"
                )
        for provider, pin in {"claude": args.claude_code_version, "codex": args.codex_version}.items():
            version(pin)
            if versions[provider] != pin:
                raise ValueError(f"installed {provider} {versions[provider]} differs from Dockerfile pin {pin}")
        metadata = (
            json.dumps(
                {
                    "schema": "sdo.model-cli-versions/v1",
                    "cli_versions": versions,
                    "default_models": policy.default_models,
                    "minimum_cli_versions": policy.minimum_cli_versions,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        args.metadata.write_text(metadata, encoding="utf-8")
    except (OSError, TypeError, ValueError, subprocess.SubprocessError) as error:
        print(f"model CLI compatibility check failed: {error}", file=sys.stderr)
        return 1
    print(metadata, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
