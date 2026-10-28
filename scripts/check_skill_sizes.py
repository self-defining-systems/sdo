"""Glob every shipped skill document; new files never need an opt-in list."""

from __future__ import annotations

import argparse
from pathlib import Path


def violations(root: Path) -> list[str]:
    errors = []
    files = sorted(root.rglob("*.md"))
    if not files:
        return [f"{root}: no shipped skill documents found"]
    for path in files:
        text = path.read_text()
        words, lines = len(text.split()), len(text.splitlines())
        limit = 1200 if path.name == "SKILL.md" else 2000
        if words > limit or (path.name == "SKILL.md" and lines > 100):
            errors.append(
                f"{path}: {words} words/{lines} lines; limit {limit} words"
                + ("/100 lines" if path.name == "SKILL.md" else "")
                + "; split into an index and topic files"
            )
    errors.extend(
        f"{directory}: missing SKILL.md"
        for directory in sorted(root.iterdir())
        if directory.is_dir() and not (directory / "SKILL.md").is_file()
    )
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path(__file__).resolve().parents[1] / "sdo/skills")
    args = parser.parse_args()
    errors = violations(args.root)
    if errors:
        raise SystemExit("\n".join(errors))


if __name__ == "__main__":
    main()
