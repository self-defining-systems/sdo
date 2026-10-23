"""Run-owned Linux process identities; never search process arguments or environments."""

from __future__ import annotations

import fcntl
import os
import signal
import time
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ProcessRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pid: int = Field(gt=1)
    start_ticks: int
    boot_id: str
    uid: int
    name: str
    process_group: int

    @classmethod
    def capture(cls, pid: int, name: str) -> ProcessRef:
        proc = Path(f"/proc/{pid}")
        fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
        return cls(
            pid=pid,
            start_ticks=int(fields[19]),
            boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            uid=proc.stat().st_uid,
            name=name,
            process_group=int(fields[2]),
        )

    def alive(self) -> bool:
        try:
            current = self.capture(self.pid, self.name)
            state = Path(f"/proc/{self.pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        except (FileNotFoundError, ProcessLookupError):
            return False
        return (
            current.start_ticks == self.start_ticks
            and current.boot_id == self.boot_id
            and current.uid == self.uid == os.getuid()
            and current.process_group == self.process_group
            and state not in ("Z", "X")
        )


class ProcessRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    processes: list[ProcessRef] = Field(default_factory=list)


def registry_path(state: Path) -> Path:
    return state.with_name(state.name + ".processes.json")


def read_registry(state: Path) -> ProcessRegistry:
    path = registry_path(state)
    return ProcessRegistry.model_validate_json(path.read_text()) if path.exists() else ProcessRegistry()


def register(state: Path, ref: ProcessRef, *, new_run: bool = False) -> None:
    path = registry_path(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        registry = read_registry(state)
        if new_run:
            if any(item.alive() for item in registry.processes):
                raise RuntimeError(f"run already has registered live processes: {path}")
            registry = ProcessRegistry()
        registry.processes.append(ref)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(registry.model_dump_json(indent=2) + "\n")
        temporary.replace(path)


def terminate_run(state: Path, *, timeout: float = 5.0) -> list[str]:
    """Stop only recorded, same-UID identities, then list surviving registered helpers.

    A launcher calls this from its own abort trap, so its own PID is excluded.
    The guardian owns a private process group; killing it also kills descendants
    that have not yet reached the registry. A start-time mismatch never authorizes
    signalling a reused PID or process group.
    """
    refs = read_registry(state).processes
    own_pid = os.getpid()
    # Stop the launcher first, preventing new registrations while cleanup runs.
    ordered = sorted(refs, key=lambda ref: ref.name != "launcher")
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for ref in ordered:
            if ref.pid == own_pid or not ref.alive():
                continue
            try:
                if ref.name == "guardian" and ref.process_group == ref.pid:
                    os.killpg(ref.process_group, sig)
                else:
                    # External abort must not trigger a second concurrent abort trap.
                    os.kill(ref.pid, signal.SIGKILL if ref.name == "launcher" else sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + timeout / 2
        while time.monotonic() < deadline:
            if all(ref.pid == own_pid or not ref.alive() for ref in refs):
                return []
            time.sleep(0.05)
    return [f"helper/{ref.name} pid={ref.pid}" for ref in refs if ref.pid != own_pid and ref.alive()]


def bind_state(state: Path) -> None:
    """Attach the launcher's stable abort locator to the runner's result-directory state.

    The result tree keeps its normal layout, which deferred receipt publication
    relies on. The launcher's process registry remains stable across this bind.
    """
    selected = os.getenv("SDO_RUN_REGISTRY_STATE")
    if not selected:
        return
    locator = Path(selected)
    state = state.resolve()
    source_registry = registry_path(locator)
    target_registry = registry_path(state)
    if target_registry != source_registry:
        current_pids = {ref.pid for ref in read_registry(locator).processes}
        if any(ref.pid not in current_pids and ref.alive() for ref in read_registry(state).processes):
            raise RuntimeError(f"another live launcher owns {state}")
        target_registry.parent.mkdir(parents=True, exist_ok=True)
        temporary = target_registry.with_suffix(".link")
        temporary.unlink(missing_ok=True)
        temporary.symlink_to(source_registry)
        temporary.replace(target_registry)
    if locator.resolve() != state:
        if locator.exists() or locator.is_symlink():
            raise RuntimeError(f"abort locator already names another state: {locator}")
        locator.symlink_to(state)
