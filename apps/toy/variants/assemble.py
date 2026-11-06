"""Assemble a standalone source root for one toy deployment variant.

A variant directory holds only deployment files. The toy code (``main.go`` and
``Dockerfile``) is shared, so a source root is that code plus the variant's
files, laid out as the variant directory lays them out. The ``raw`` variant is
the original ``kubernetes/`` manifests.

    python apps/toy/variants/assemble.py <raw|kustomize|helm|compose> <dest>
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

TOY = Path(__file__).resolve().parents[1]
VARIANTS_DIR = TOY / "variants"
SHARED_CODE = ("main.go", "Dockerfile")
VARIANTS = ("raw", "kustomize", "helm", "compose")


def assemble(variant: str, dest: Path) -> Path:
    """Write toy code plus ``variant``'s deployment files into the empty ``dest``."""
    if variant not in VARIANTS:
        raise ValueError(f"unknown toy variant {variant!r}; expected one of {VARIANTS}")
    if dest.exists() and any(dest.iterdir()):
        raise ValueError(f"destination {dest} is not empty")
    dest.mkdir(parents=True, exist_ok=True)
    for name in SHARED_CODE:
        shutil.copy2(TOY / name, dest / name)
    if variant == "raw":
        shutil.copytree(TOY / "kubernetes", dest / "kubernetes")
    else:
        shutil.copytree(VARIANTS_DIR / variant, dest, dirs_exist_ok=True)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("variant", choices=VARIANTS)
    parser.add_argument("dest", type=Path)
    args = parser.parse_args()
    print(assemble(args.variant, args.dest))


if __name__ == "__main__":
    main()
