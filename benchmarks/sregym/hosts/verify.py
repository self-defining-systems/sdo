"""Read-only checks that a host is a safe eval host, reported as a pass/fail table."""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass

from benchmarks.sregym.hosts.config import REFUSED_FILESYSTEMS
from benchmarks.sregym.hosts.steps import (
    SLICE_PROPS,
    Context,
    diskwatch_paths,
    filesystem_type,
    guard_values,
    linger_enabled,
    mglru_state,
    parse_env,
    read_text,
    slice_mismatches,
    sysctl_shortfalls,
    systemd_show,
)

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"
SMOKE_PROMPT = "Reply with the single word OK."


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str = ""


def check_slice(ctx: Context) -> Check:
    name = ctx.host.slice.name
    problems = slice_mismatches(ctx.spec, systemd_show(ctx, name, *SLICE_PROPS))
    spec = ctx.spec
    summary = (
        f"MemoryHigh={spec.memory_high_bytes >> 30}G MemoryMax={spec.memory_max_gib}G CPUQuota={spec.cpu_quota_pct}%"
    )
    return Check(name, FAIL, "; ".join(problems)) if problems else Check(name, PASS, summary)


def check_mglru(ctx: Context) -> Check:
    if not ctx.host.mglru.enabled:
        return Check("mglru", SKIP, "disabled in config")
    if not ctx.facts.has_lru_gen:
        return Check("mglru", WARN, "kernel has no lru_gen")
    on, ttl = mglru_state(ctx)
    want = ctx.host.mglru.min_ttl_ms
    if on and ttl == want:
        return Check("mglru", PASS, f"enabled, min_ttl_ms={ttl}")
    return Check("mglru", FAIL, f"enabled={on}, min_ttl_ms={ttl} (want on, {want})")


def check_sysctl(ctx: Context) -> Check:
    short = sysctl_shortfalls(ctx)
    return (
        Check("sysctl", FAIL, "; ".join(short))
        if short
        else Check("sysctl", PASS, "kind neighbour table, inotify limits")
    )


def _local_dir(ctx: Context, name: str, raw: str) -> Check:
    path = ctx.path(raw)
    fstype = filesystem_type(ctx, path)
    if fstype is None:
        return Check(name, FAIL, f"{path}: filesystem unknown")
    if fstype in REFUSED_FILESYSTEMS:
        return Check(name, FAIL, f"{path} is on {fstype}")
    if not ctx.target.run(["test", "-w", path]).ok:
        return Check(name, FAIL, f"{path} ({fstype}) is missing or not writable")
    return Check(name, PASS, f"{path} ({fstype})")


def check_lane_root(ctx: Context) -> list[Check]:
    checks = [_local_dir(ctx, "lane-root", ctx.host.lane_root), _local_dir(ctx, "evidence-root", ctx.host.evidence_dir)]
    docker = _docker_info(ctx)
    if docker is not None:
        root = docker[2]
        fstype = filesystem_type(ctx, root)
        bad = fstype is None or fstype in REFUSED_FILESYSTEMS
        checks.append(Check("docker-root", FAIL if bad else PASS, f"{root} ({fstype or 'unknown'})"))
    return checks


def _docker_info(ctx: Context) -> tuple[str, str, str] | None:
    result = ctx.target.run(["docker", "info", "--format", "{{.CgroupDriver}} {{.CgroupVersion}} {{.DockerRootDir}}"])
    parts = result.stdout.split()
    return (parts[0], parts[1], parts[2]) if result.ok and len(parts) == 3 else None


def check_docker(ctx: Context) -> Check:
    info = _docker_info(ctx)
    if info is None:
        return Check("docker-cgroup", FAIL, "docker info failed (not running or no permission)")
    driver, version, _ = info
    if driver == "systemd" and version == "2":
        return Check("docker-cgroup", PASS, "cgroup driver systemd, cgroup v2")
    return Check("docker-cgroup", FAIL, f"driver={driver} cgroup v{version} (want systemd, v2)")


def check_guard(ctx: Context) -> Check:
    path = ctx.path(ctx.host.guard.env_file)
    text = read_text(ctx, path)
    if text is None:
        return Check("guard-config", FAIL, f"{path} is missing")
    have = parse_env(text)
    want = guard_values(ctx.host, ctx.spec)
    problems = [
        f"{key}={have.get(key, 'unset')} (want {want[key]})"
        for key in ("SDO_HOST_LANE_PREFIX", "SDO_HOST_LANE_BUDGET_GIB")
        if have.get(key) != want[key]
    ]
    problems += [
        f"{key}={have[key]} (want {want[key]})"
        for key in ("SDO_HOST_MEMORY_RESERVE_GIB", "SDO_HOST_MEMORY_PSI_MAX")
        if key in have and float(have[key] or 0) != float(want[key])
    ]
    return (
        Check("guard-config", FAIL, f"{path}: " + "; ".join(problems))
        if problems
        else Check("guard-config", PASS, path)
    )


def check_diskwatch(ctx: Context) -> Check:
    if not ctx.host.diskwatch.enabled:
        return Check("diskwatch", SKIP, "disabled in config")
    script, _ = diskwatch_paths(ctx)
    if ctx.target.read(script) is None:
        return Check("diskwatch", FAIL, f"script {script} is missing")
    timer = ctx.target.run(ctx.user_systemctl("is-active", "sdo-diskwatch.timer")).stdout.strip()
    if timer != "active":
        return Check("diskwatch", FAIL, f"sdo-diskwatch.timer is {timer or 'not installed'}")
    if not linger_enabled(ctx):
        return Check("diskwatch", WARN, "timer active but linger is off, so it stops at logout")
    return Check("diskwatch", PASS, "timer active, linger on")


def _mode(ctx: Context, path: str) -> str:
    return ctx.target.run(["stat", "-c", "%a", path]).stdout.strip()


def check_credentials(ctx: Context, *, smoke: bool = False) -> list[Check]:
    cred = ctx.host.credentials
    if cred is None:
        return [Check("credentials", SKIP, "no [credentials] table")]
    profile, key = ctx.path(cred.profile_dir), ctx.path(cred.key_path)
    settings_path, claude_json = posixpath.join(profile, "settings.json"), posixpath.join(profile, ".claude.json")
    missing = [p for p in (settings_path, claude_json, key) if not ctx.target.run(["test", "-s", p]).ok]
    if missing:
        return [Check("credentials", FAIL, "missing or empty: " + ", ".join(missing))]
    try:
        env = json.loads((ctx.target.read(settings_path) or b"{}").decode()).get("env", {})
    except (ValueError, AttributeError):
        return [Check("credentials", FAIL, f"{settings_path} is not valid JSON")]
    if env.get("GOOGLE_APPLICATION_CREDENTIALS") != key:
        return [Check("credentials", FAIL, f"settings.json does not point GOOGLE_APPLICATION_CREDENTIALS at {key}")]
    loose = [
        f"{p} {_mode(ctx, p)}"
        for p, want in ((profile, "700"), (settings_path, "600"), (claude_json, "600"), (key, "600"))
        if _mode(ctx, p) != want
    ]
    checks = [
        Check("credentials", WARN, "present; modes should be 700/600: " + ", ".join(loose))
        if loose
        else Check("credentials", PASS, f"present in {profile}")
    ]
    if smoke:
        checks.append(smoke_call(ctx, profile))
    return checks


def smoke_call(ctx: Context, profile: str) -> Check:
    """One minimal model call with the host's ClaudeGCP profile; the reply is never shown."""
    script = (
        'PATH="$HOME/.local/bin:$HOME/.npm-global/bin:/usr/local/bin:$PATH"; '
        'CLAUDE_CONFIG_DIR="$1" claude -p "$2" --max-turns 1 --output-format text'
    )
    result = ctx.target.run(["sh", "-c", script, "sh", profile, SMOKE_PROMPT], timeout=180)
    if result.ok and result.stdout.strip():
        return Check("credentials-smoke", PASS, "model replied")
    return Check("credentials-smoke", FAIL, f"call failed (exit {result.returncode}): {result.stderr.strip()[:160]}")


def check_lane_cgroup(ctx: Context) -> Check:
    """Informational: lanes started under the host prefix should sit in the eval slice."""
    prefix = ctx.host.guard.lane_prefix
    names = ctx.target.run(["docker", "ps", "--format", "{{.Names}}"]).stdout.split()
    ours = [n for n in names if n.startswith(prefix)]
    if not ours:
        return Check("lane-cgroup", SKIP, f"no running containers named {prefix}*")
    parents = ctx.target.run(["docker", "inspect", "--format", "{{.HostConfig.CgroupParent}}", *ours]).stdout.split()
    inside = sum(1 for parent in parents if parent == ctx.host.slice.name)
    status = PASS if inside == len(ours) else WARN
    return Check(
        "lane-cgroup", status, f"{inside} of {len(ours)} running {prefix}* containers are under {ctx.host.slice.name}"
    )


def run_checks(ctx: Context, *, smoke: bool = False) -> list[Check]:
    checks: list[Check] = [check_slice(ctx), check_mglru(ctx)]
    checks.append(check_sysctl(ctx))
    checks += check_lane_root(ctx)
    checks += [check_guard(ctx), check_diskwatch(ctx)]
    checks += check_credentials(ctx, smoke=smoke)
    checks += [check_docker(ctx), check_lane_cgroup(ctx)]
    return checks


def render_table(host: str, checks: list[Check]) -> str:
    width = max(len(c.name) for c in checks)
    lines = [f"== {host}", f"{'check'.ljust(width)}  status  detail"]
    lines += [f"{c.name.ljust(width)}  {c.status:<6}  {c.detail}" for c in checks]
    failed = sum(1 for c in checks if c.status == FAIL)
    lines.append(
        f"-> {'FAIL' if failed else 'PASS'} ({failed} failing, {sum(1 for c in checks if c.status == WARN)} warnings)"
    )
    return "\n".join(lines)
