"""The setup steps: each compares a desired state derived from the config with the live host and yields changes.

A ``Change`` is one idempotent action (a file write with its diff, or a command) and is produced only when the host
differs from the desired state, so planning twice in a row is empty. Credential content never appears in a diff.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import posixpath
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.hosts.config import (
    REFUSED_FILESYSTEMS,
    HostConfig,
    HostFacts,
    SliceSpec,
    derive_slice,
    sysctl_values,
)
from benchmarks.sregym.hosts.target import TargetError

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.hosts.target import Target

TMPFILES = "/etc/tmpfiles.d/mglru.conf"
SYSCTL_FILE = "/etc/sysctl.d/90-sdo-eval.conf"
LRU_ENABLED = "/sys/kernel/mm/lru_gen/enabled"
LRU_TTL = "/sys/kernel/mm/lru_gen/min_ttl_ms"
GUARD_HEADER = "# SDO host memory guard (managed by benchmarks.sregym.hosts); other lines are left alone"
GUARD_KEYS = (
    "SDO_HOST_LANE_PREFIX",
    "SDO_HOST_LANE_BUDGET_GIB",
    "SDO_HOST_MEMORY_RESERVE_GIB",
    "SDO_HOST_MEMORY_PSI_MAX",
)
_TIMESPAN = re.compile(r"([0-9.]+)\s*(us|ms|s|min|h)")
_UNITS_US = {"us": 1, "ms": 1000, "s": 1_000_000, "min": 60_000_000, "h": 3_600_000_000}


@dataclass(frozen=True)
class Note:
    level: str  # "warn" | "blocked"
    step: str
    text: str


@dataclass(frozen=True)
class Change:
    step: str
    title: str
    apply: Callable[[], None]
    root: bool = False
    diff: str = ""
    #: A failure is reported but does not fail the apply (for example enabling linger without polkit).
    best_effort: bool = False


@dataclass
class Plan:
    changes: list[Change] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)

    @property
    def blocked(self) -> list[Note]:
        return [note for note in self.notes if note.level == "blocked"]

    @property
    def needs_root(self) -> bool:
        return any(change.root for change in self.changes)


def _local_read(path: str) -> bytes | None:
    file = Path(path).expanduser()
    return file.read_bytes() if file.is_file() else None


@dataclass(frozen=True)
class Context:
    host: HostConfig
    facts: HostFacts
    target: Target
    home: str
    controller_read: Callable[[str], bytes | None] = field(default=lambda path: _local_read(path))
    #: The watcher script the controller itself runs; copied to a host that has none.
    controller_script: str | None = None

    @property
    def spec(self) -> SliceSpec:
        return derive_slice(self.host.slice, self.facts)

    def path(self, value: str) -> str:
        if value == "~" or value.startswith("~/"):
            return self.home.rstrip("/") + value[1:]
        return value

    def user_systemctl(self, *args: str) -> list[str]:
        return ["env", f"XDG_RUNTIME_DIR=/run/user/{self.facts.uid}", "systemctl", "--user", *args]


def render_slice(name: str, spec: SliceSpec) -> str:
    gib = 1024**3
    return (
        "[Unit]\n"
        "Description=SDO eval lanes: hard caps so a storm inside our lanes cannot make the host unresponsive\n"
        "[Slice]\n"
        "MemoryAccounting=yes\n"
        f"MemoryHigh={spec.memory_high_bytes // gib}G\n"
        f"MemoryMax={spec.memory_max_bytes // gib}G\n"
        "CPUAccounting=yes\n"
        f"CPUQuota={spec.cpu_quota_pct}%\n"
        f"CPUWeight={spec.cpu_weight}\n"
        "TasksAccounting=yes\n"
        f"TasksMax={spec.tasks_max}\n"
        "IOAccounting=yes\n"
        f"IOWeight={spec.io_weight}\n"
    )


def render_tmpfiles(min_ttl_ms: int) -> str:
    return f"w {LRU_ENABLED} - - - - y\nw {LRU_TTL} - - - - {min_ttl_ms}\n"


def render_sysctl(values: dict[str, int]) -> str:
    return "".join(f"{key}={value}\n" for key, value in values.items())


def sysctl_live(ctx: Context, key: str) -> int | None:
    raw = read_text(ctx, "/proc/sys/" + key.replace(".", "/"))
    try:
        return int((raw or "").strip())
    except ValueError:
        return None


def sysctl_shortfalls(ctx: Context) -> list[str]:
    """Wanted sysctls the host runs below (a higher live value is fine for every one of them)."""
    out = []
    for key, want in sysctl_values(ctx.host.sysctl, ctx.facts).items():
        live = sysctl_live(ctx, key)
        if live is None or live < want:
            out.append(f"{key}={live} (want {want})")
    return out


def guard_values(host: HostConfig, spec: SliceSpec) -> dict[str, str]:
    guard = host.guard
    return {
        "SDO_HOST_LANE_PREFIX": guard.lane_prefix,
        "SDO_HOST_LANE_BUDGET_GIB": str(spec.memory_max_gib),
        "SDO_HOST_MEMORY_RESERVE_GIB": f"{guard.reserve_gib:g}",
        "SDO_HOST_MEMORY_PSI_MAX": f"{guard.psi_max:g}",
    }


def parse_env(text: str | None) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (text or "").splitlines():
        match = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if match:
            values[match[1]] = match[2].strip().strip("'\"")
    return values


def merge_env(existing: str | None, wanted: dict[str, str]) -> str:
    """Set ``wanted`` keys in an env file, keeping every other line, ``export`` style and order."""
    lines = (existing or "").splitlines()
    pending = dict(wanted)
    out: list[str] = []
    for line in lines:
        match = re.match(r"^(\s*(?:export\s+)?)([A-Za-z_][A-Za-z0-9_]*)=", line)
        if match and match[2] in pending:
            out.append(f"{match[1]}{match[2]}={pending.pop(match[2])}")
        else:
            out.append(line)
    if pending:
        if GUARD_HEADER not in out:
            out.append(GUARD_HEADER)
        out.extend(f"{key}={value}" for key, value in pending.items())
    return "\n".join(out) + "\n"


def strip_env(existing: str, keys: tuple[str, ...]) -> str:
    kept = [line for line in existing.splitlines() if not re.match(rf"^\s*(?:export\s+)?({'|'.join(keys)})=", line)]
    kept = [line for line in kept if line != GUARD_HEADER]
    return "\n".join(kept) + "\n" if kept else ""


DEFAULT_DISKWATCH_SCRIPT = "~/.local/share/sdo/diskwatch/sdo-diskwatch.sh"
_EXEC_START = re.compile(r"^ExecStart=/bin/bash (\S+)\s*$", re.MULTILINE)
_EXEC_POST = re.compile(r"^ExecStartPost=-?/bin/bash (\S+)\s*$", re.MULTILINE)


def installed_diskwatch(service_text: str | None) -> tuple[str | None, str | None]:
    """``(script, collector)`` an installed ``sdo-diskwatch.service`` runs."""
    text = service_text or ""
    start, post = _EXEC_START.search(text), _EXEC_POST.search(text)
    return (start[1] if start else None, post[1] if post else None)


def diskwatch_paths(ctx: Context) -> tuple[str, str | None]:
    """The script and collector the watcher should run: configured, else already installed, else the default."""
    settings = ctx.host.diskwatch
    unit = f"{ctx.home.rstrip('/')}/.config/systemd/user/sdo-diskwatch.service"
    script, collector = installed_diskwatch(read_text(ctx, unit))
    chosen = settings.script or script or DEFAULT_DISKWATCH_SCRIPT
    post = settings.collector or collector
    return ctx.path(chosen), ctx.path(post) if post else None


def render_diskwatch_units(script: str, collector: str | None, minutes: int) -> tuple[str, str]:
    service = (
        "[Unit]\nDescription=SDO disk watcher: free space, inodes, docker root; safe cleanup on critical\n"
        "[Service]\nType=oneshot\nEnvironment=PATH=/usr/local/bin:/usr/bin:/bin\nNice=10\nTimeoutStartSec=300\n"
        f"ExecStart=/bin/bash {script}\n"
    )
    if collector:
        service += f"ExecStartPost=-/bin/bash {collector}\n"
    timer = (
        f"[Unit]\nDescription=Run sdo-diskwatch every {minutes} minutes\n"
        f"[Timer]\nOnActiveSec=5s\nOnUnitActiveSec={minutes}min\nAccuracySec=5s\n"
        "[Install]\nWantedBy=timers.target\n"
    )
    return service, timer


def parse_timespan_percent(value: str) -> int | None:
    """``CPUQuotaPerSecUSec`` ("40s", "2min 20s") as a percentage of one core, ``None`` when unset."""
    if not value or value == "infinity":
        return None
    total = sum(float(number) * _UNITS_US[unit] for number, unit in _TIMESPAN.findall(value))
    return round(total / 10_000)


def unified(path: str, before: str | None, after: str) -> str:
    diff = difflib.unified_diff(
        (before or "").splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=path if before is not None else "/dev/null",
        tofile=path,
    )
    return "".join(line if line.endswith("\n") else line + "\n" for line in diff)


def read_text(ctx: Context, path: str) -> str | None:
    data = ctx.target.read(path)
    return None if data is None else data.decode(errors="replace")


def systemd_show(ctx: Context, unit: str, *props: str) -> dict[str, str]:
    result = ctx.target.run(["systemctl", "show", unit, *[arg for p in props for arg in ("-p", p)]])
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, _, value = line.partition("=")
        values[key] = value
    return values


def slice_mismatches(spec: SliceSpec, live: dict[str, str]) -> list[str]:
    """Differences between the derived limits and what systemd enforces; empty when the slice is as derived."""
    if live.get("ActiveState") != "active":
        return [
            f"not active (ActiveState={live.get('ActiveState') or 'unknown'}); the unit is missing or never started"
        ]
    problems: list[str] = []
    expected = {
        "MemoryHigh": str(spec.memory_high_bytes),
        "MemoryMax": str(spec.memory_max_bytes),
        "CPUWeight": str(spec.cpu_weight),
        "TasksMax": str(spec.tasks_max),
        "IOWeight": str(spec.io_weight),
    }
    problems += [f"{key}={live.get(key, '?')} (want {want})" for key, want in expected.items() if live.get(key) != want]
    quota = parse_timespan_percent(live.get("CPUQuotaPerSecUSec", ""))
    if quota != spec.cpu_quota_pct:
        problems.append(f"CPUQuota={quota}% (want {spec.cpu_quota_pct}%)")
    return problems


SLICE_PROPS = ("ActiveState", "MemoryHigh", "MemoryMax", "CPUQuotaPerSecUSec", "CPUWeight", "TasksMax", "IOWeight")


def filesystem_type(ctx: Context, path: str) -> str | None:
    """The filesystem type of the mount holding ``path`` (or its nearest existing parent)."""
    probe = path
    while probe not in ("", "/") and not ctx.target.run(["test", "-e", probe]).ok:
        probe = posixpath.dirname(probe)
    result = ctx.target.run(["findmnt", "-n", "-o", "FSTYPE", "--target", probe or "/"])
    lines = result.stdout.split() if result.ok else []
    return lines[-1] if lines else None  # an autofs mount lists autofs first and the real filesystem last


def file_change(
    ctx: Context, step: str, path: str, desired: str, *, mode: int = 0o644, root: bool = False
) -> Change | None:
    current = read_text(ctx, path)
    if current == desired:
        return None
    data = desired.encode()
    write = (
        (lambda: ctx.target.sudo_install(path, data, mode)) if root else (lambda: ctx.target.write(path, data, mode))
    )
    return Change(step, f"write {path}", write, root=root, diff=unified(path, current, desired))


def command_change(
    ctx: Context, step: str, title: str, argv: list[str], *, root: bool = False, best_effort: bool = False
) -> Change:
    def run() -> None:
        result = ctx.target.sudo(argv) if root else ctx.target.run(argv)
        if not result.ok:
            raise TargetError(f"{ctx.target.name}: {' '.join(argv[:4])} failed: {result.stderr.strip()[:300]}")

    return Change(
        step, title, run, root=root, diff=f"$ {'sudo ' if root else ''}{' '.join(argv)}\n", best_effort=best_effort
    )


def plan_slice(ctx: Context) -> Plan:
    plan = Plan()
    name = ctx.host.slice.name
    spec = ctx.spec
    change = file_change(ctx, "slice", f"/etc/systemd/system/{name}", render_slice(name, spec), root=True)
    if change:
        plan.changes.append(change)
    if change or slice_mismatches(spec, systemd_show(ctx, name, *SLICE_PROPS, "ControlGroup")):
        plan.changes.append(command_change(ctx, "slice", "reload systemd", ["systemctl", "daemon-reload"], root=True))
        plan.changes.append(command_change(ctx, "slice", f"start {name}", ["systemctl", "start", name], root=True))
    return plan


def plan_mglru(ctx: Context) -> Plan:
    plan = Plan()
    if not ctx.host.mglru.enabled:
        return plan
    if not ctx.facts.has_lru_gen:
        plan.notes.append(Note("warn", "mglru", "kernel has no lru_gen (MGLRU); skipped"))
        return plan
    ttl = ctx.host.mglru.min_ttl_ms
    change = file_change(ctx, "mglru", TMPFILES, render_tmpfiles(ttl), root=True)
    if change:
        plan.changes.append(change)
    live = mglru_state(ctx)
    if change or live != (True, ttl):
        plan.changes.append(
            command_change(ctx, "mglru", "apply MGLRU now", ["systemd-tmpfiles", "--create", TMPFILES], root=True)
        )
    return plan


def mglru_state(ctx: Context) -> tuple[bool, int | None]:
    enabled = read_text(ctx, LRU_ENABLED)
    ttl = read_text(ctx, LRU_TTL)
    try:
        on = bool(int((enabled or "0").strip(), 16) & 1)
        return on, int((ttl or "").strip())
    except ValueError:
        return False, None


def plan_sysctl(ctx: Context) -> Plan:
    plan = Plan()
    change = file_change(
        ctx, "sysctl", SYSCTL_FILE, render_sysctl(sysctl_values(ctx.host.sysctl, ctx.facts)), root=True
    )
    if change:
        plan.changes.append(change)
    if change or sysctl_shortfalls(ctx):
        plan.changes.append(
            command_change(ctx, "sysctl", "apply sysctls now", ["sysctl", "-p", SYSCTL_FILE], root=True)
        )
    return plan


def plan_lane_root(ctx: Context) -> Plan:
    plan = Plan()
    for label, raw in (("lane root", ctx.host.lane_root), ("evidence root", ctx.host.evidence_dir)):
        path = ctx.path(raw)
        fstype = filesystem_type(ctx, path)
        if fstype is None:
            plan.notes.append(Note("blocked", "lane-root", f"cannot determine the filesystem of the {label} {path}"))
        elif fstype in REFUSED_FILESYSTEMS:
            plan.notes.append(
                Note(
                    "blocked", "lane-root", f"{label} {path} is on {fstype}; lanes, images and evidence need local disk"
                )
            )
        elif not ctx.target.run(["test", "-d", path]).ok:
            plan.changes.append(command_change(ctx, "lane-root", f"create {label} {path}", ["mkdir", "-p", path]))
    return plan


def plan_guard(ctx: Context) -> Plan:
    plan = Plan()
    path = ctx.path(ctx.host.guard.env_file)
    desired = merge_env(read_text(ctx, path), guard_values(ctx.host, ctx.spec))
    change = file_change(ctx, "guard", path, desired)
    if change:
        plan.changes.append(change)
    return plan


def plan_diskwatch(ctx: Context) -> Plan:
    plan = Plan()
    settings = ctx.host.diskwatch
    if not settings.enabled:
        return plan
    script, collector = diskwatch_paths(ctx)
    if ctx.target.read(script) is None:
        source = ctx.controller_read(ctx.controller_script) if ctx.controller_script else None
        if source is None:
            plan.notes.append(
                Note("blocked", "diskwatch", f"watcher script {script} is missing and the controller has none to copy")
            )
            return plan
        plan.changes.append(
            Change(
                "diskwatch",
                f"copy watcher script to {script}",
                lambda: ctx.target.write(script, source, 0o755),
                diff=f"(new executable, {len(source.splitlines())} lines, copied from the controller)\n",
            )
        )
    service, timer = render_diskwatch_units(script, collector, settings.interval_minutes)
    unit_dir = f"{ctx.home.rstrip('/')}/.config/systemd/user"
    written = [
        change
        for change in (
            file_change(ctx, "diskwatch", f"{unit_dir}/sdo-diskwatch.service", service),
            file_change(ctx, "diskwatch", f"{unit_dir}/sdo-diskwatch.timer", timer),
        )
        if change
    ]
    plan.changes.extend(written)
    active = ctx.target.run(ctx.user_systemctl("is-active", "sdo-diskwatch.timer")).stdout.strip() == "active"
    enabled = ctx.target.run(ctx.user_systemctl("is-enabled", "sdo-diskwatch.timer")).stdout.strip() == "enabled"
    if written or not (active and enabled):
        plan.changes.append(command_change(ctx, "diskwatch", "reload user units", ctx.user_systemctl("daemon-reload")))
        plan.changes.append(
            command_change(
                ctx,
                "diskwatch",
                "enable and start the timer",
                ctx.user_systemctl("enable", "--now", "sdo-diskwatch.timer"),
            )
        )
    if not linger_enabled(ctx):
        plan.changes.append(
            command_change(
                ctx,
                "diskwatch",
                "enable linger so the timer survives logout",
                ["loginctl", "enable-linger"],
                best_effort=True,
            )
        )
    return plan


def linger_enabled(ctx: Context) -> bool:
    result = ctx.target.run(["loginctl", "show-user", str(ctx.facts.uid), "-p", "Linger", "--value"])
    return result.stdout.strip() == "yes"


def plan_credential_modes(ctx: Context) -> Plan:
    """Tighten an existing profile in place (dir 700, essentials 600); nothing is copied or read."""
    plan = Plan()
    cred = ctx.host.credentials
    if cred is None:
        return plan
    profile = ctx.path(cred.profile_dir)
    targets = (
        (profile, "700", ctx.target.run(["test", "-d", profile]).ok),
        *(
            (path, "600", ctx.target.run(["test", "-f", path]).ok)
            for path in (
                posixpath.join(profile, "settings.json"),
                posixpath.join(profile, ".claude.json"),
                ctx.path(cred.key_path),
            )
        ),
    )
    for path, mode, present in targets:
        if present and ctx.target.run(["stat", "-c", "%a", path]).stdout.strip() != mode:
            plan.changes.append(command_change(ctx, "credentials", f"chmod {mode} {path}", ["chmod", mode, path]))
    return plan


def _digest(data: bytes | None) -> str | None:
    return None if data is None else hashlib.sha256(data).hexdigest()


def plan_credentials(ctx: Context, source: Target, source_settings: tuple[str, str]) -> Plan:
    """Copy the ClaudeGCP essentials from ``source``; only names are shown, never content.

    ``source_settings`` is the source's ``(profile_dir, key_path)``. Sessions, projects and transcripts never move.
    """
    plan = Plan()
    dest = ctx.host.credentials
    if dest is None:
        plan.notes.append(Note("blocked", "credentials", "host has no [credentials] table to copy into"))
        return plan
    src_dir, src_key = source_settings
    settings_raw = source.read(posixpath.join(src_dir, "settings.json"))
    claude_json = source.read(posixpath.join(src_dir, ".claude.json"))
    if settings_raw is None or claude_json is None:
        plan.notes.append(
            Note("blocked", "credentials", f"source {source.name} lacks settings.json or .claude.json in {src_dir}")
        )
        return plan
    try:
        settings = json.loads(settings_raw)
        env = settings.setdefault("env", {})
        key_path = env.get("GOOGLE_APPLICATION_CREDENTIALS") or src_key
    except (ValueError, AttributeError):
        plan.notes.append(Note("blocked", "credentials", f"source {source.name} settings.json is not a JSON object"))
        return plan
    key = source.read(key_path)
    if key is None:
        plan.notes.append(
            Note(
                "blocked",
                "credentials",
                f"source {source.name} has no service-account key at the path its settings name",
            )
        )
        return plan
    profile = ctx.path(dest.profile_dir)
    dest_key = ctx.path(dest.key_path)
    env["GOOGLE_APPLICATION_CREDENTIALS"] = dest_key
    rewritten = (json.dumps(settings, indent=2) + "\n").encode()

    def ensure_dir(path: str) -> None:
        result = ctx.target.run(["sh", "-c", 'mkdir -p "$1" && chmod 700 "$1"', "sh", path])
        if not result.ok:
            raise TargetError(f"{ctx.target.name}: cannot create {path}: {result.stderr.strip()[:200]}")

    key_dir = posixpath.dirname(dest_key)
    wanted_dirs = [(profile, ctx.target.run(["stat", "-c", "%a", profile]).stdout.strip() != "700")]
    if key_dir != profile:
        wanted_dirs.append((key_dir, not ctx.target.run(["test", "-d", key_dir]).ok))
    for directory, needed in wanted_dirs:
        if needed:
            plan.changes.append(
                Change(
                    "credentials",
                    f"create or chmod 700 {directory}",
                    lambda directory=directory: ensure_dir(directory),
                    diff=f"mkdir -p {directory}; chmod 700 {directory}\n",
                )
            )
    files = (
        (posixpath.join(profile, "settings.json"), rewritten, "settings.json (key path rewritten)"),
        (posixpath.join(profile, ".claude.json"), claude_json, ".claude.json"),
        (dest_key, key, "service-account key"),
    )
    for path, data, label in files:
        if _digest(ctx.target.read(path)) != _digest(data):
            plan.changes.append(
                Change(
                    "credentials",
                    f"copy {label} to {path} (mode 600)",
                    lambda path=path, data=data: ctx.target.write(path, data, 0o600),
                    diff=f"copy {label} from {source.name} -> {path} (content not shown)\n",
                )
            )
    return plan


def plan_remove(ctx: Context) -> Plan:
    plan = Plan()
    name = ctx.host.slice.name
    root_files = [f"/etc/systemd/system/{name}", TMPFILES, SYSCTL_FILE]
    present = [path for path in root_files if ctx.target.read(path) is not None]
    for path in present:
        plan.changes.append(command_change(ctx, "remove", f"delete {path}", ["rm", "-f", "--", path], root=True))
    if present:
        plan.changes.append(command_change(ctx, "remove", "reload systemd", ["systemctl", "daemon-reload"], root=True))
    if ctx.target.run(["systemctl", "is-active", name]).stdout.strip() == "active":
        plan.changes.append(command_change(ctx, "remove", f"stop {name}", ["systemctl", "stop", name], root=True))
    unit_dir = f"{ctx.home.rstrip('/')}/.config/systemd/user"
    units = [f"{unit_dir}/sdo-diskwatch.{kind}" for kind in ("timer", "service")]
    if any(ctx.target.read(unit) is not None for unit in units):
        plan.changes.append(
            command_change(
                ctx,
                "remove",
                "disable the disk watcher timer",
                ctx.user_systemctl("disable", "--now", "sdo-diskwatch.timer"),
                best_effort=True,
            )
        )
        plan.changes.extend(
            command_change(ctx, "remove", f"delete {unit}", ["rm", "-f", "--", unit])
            for unit in units
            if ctx.target.read(unit) is not None
        )
    guard_path = ctx.path(ctx.host.guard.env_file)
    guard_text = read_text(ctx, guard_path)
    if guard_text is not None and any(key in parse_env(guard_text) for key in GUARD_KEYS):
        stripped = strip_env(guard_text, GUARD_KEYS)
        if stripped:
            change = file_change(ctx, "remove", guard_path, stripped)
            if change:
                plan.changes.append(change)
        else:
            plan.changes.append(command_change(ctx, "remove", f"delete {guard_path}", ["rm", "-f", "--", guard_path]))
    plan.notes.append(
        Note(
            "warn",
            "remove",
            "MGLRU stays on until reboot, and the lane root, watcher script and credentials are left in place",
        )
    )
    return plan


def build_plan(ctx: Context) -> Plan:
    """Every step in dependency order: refuse first, then root parts, then user parts."""
    merged = Plan()
    for step in (
        plan_lane_root,
        plan_slice,
        plan_mglru,
        plan_sysctl,
        plan_guard,
        plan_diskwatch,
        plan_credential_modes,
    ):
        part = step(ctx)
        merged.changes.extend(part.changes)
        merged.notes.extend(part.notes)
    return merged
