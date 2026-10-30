"""Compiler-bearing images must inherit one byte-identical cache layer chain."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def test_controller_and_validator_inherit_one_compiler_base():
    source = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    stages = re.findall(r"^FROM (\S+) AS (\S+)$", source, re.MULTILINE)
    # Traverse by stage name; an extra source/binary stage must not break sharing.
    parents = {stage: parent for parent, stage in stages}
    for role in ("controller", "validator"):
        cursor = role
        while cursor in parents and cursor != "compiler-base":
            cursor = parents[cursor]
        assert cursor == "compiler-base", f"{role} does not inherit compiler-base"
    sections = re.split(r"^FROM .* AS (\S+)\n", source, flags=re.MULTILINE)
    owners = {}
    for stage, body in zip(sections[1::2], sections[2::2], strict=True):
        for destination in ("/usr/local/go", "/go/pkg/mod", "/go/build-cache"):
            if re.search(r"^COPY .* " + re.escape(destination) + r"/?$", body, re.MULTILINE):
                owners.setdefault(destination, set()).add(stage)
    assert owners == {path: {"compiler-base"} for path in ("/usr/local/go", "/go/pkg/mod", "/go/build-cache")}


def test_compiler_seed_and_worker_share_relocatable_go_cache_flags() -> None:
    source = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    # Both cache layers and every worker must use the same path policy. Otherwise
    # a seeded compiler silently rebuilds its entire transitive dependency graph.
    assert 'GOFLAGS="-p=4 -trimpath"' in source
    assert 'GOFLAGS="-mod=mod -p=2 -trimpath"' in source
