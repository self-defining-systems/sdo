"""Read the facts the derivation needs from a target."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from benchmarks.sregym.hosts.config import HostFacts
from benchmarks.sregym.hosts.steps import LRU_ENABLED
from benchmarks.sregym.hosts.target import TargetError

if TYPE_CHECKING:
    from benchmarks.sregym.hosts.target import Target


def gather_facts(target: Target) -> tuple[HostFacts, str]:
    """``(facts, home)`` of the machine behind ``target``."""
    cores = target.run(["nproc"])
    meminfo = target.read("/proc/meminfo")
    uid = target.run(["id", "-u"])
    home = target.run(["sh", "-c", 'printf %s "$HOME"'])
    match = re.search(rb"MemTotal:\s+(\d+) kB", meminfo or b"")
    if not (
        cores.ok and match and uid.ok and home.ok and cores.stdout.strip().isdigit() and uid.stdout.strip().isdigit()
    ):
        raise TargetError(f"{target.name}: cannot read cores, memory, uid and home (is the host reachable?)")
    facts = HostFacts(
        cores=int(cores.stdout),
        mem_total_kib=int(match[1]),
        uid=int(uid.stdout),
        has_lru_gen=target.run(["test", "-e", LRU_ENABLED]).ok,
    )
    return facts, home.stdout.strip()
