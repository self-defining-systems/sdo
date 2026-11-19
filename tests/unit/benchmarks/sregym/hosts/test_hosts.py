from __future__ import annotations

import json

import pytest

from benchmarks.sregym.hosts import cli
from benchmarks.sregym.hosts.config import (
    DEFAULT_CONFIG,
    GIB,
    HostConfigError,
    HostFacts,
    SliceSettings,
    derive_slice,
    load_hosts,
    parse_hosts,
)
from benchmarks.sregym.hosts.steps import (
    SLICE_PROPS,
    Context,
    build_plan,
    diskwatch_paths,
    installed_diskwatch,
    merge_env,
    parse_timespan_percent,
    plan_credential_modes,
    plan_credentials,
    plan_remove,
    render_slice,
    strip_env,
)
from benchmarks.sregym.hosts.verify import FAIL, PASS, WARN, render_table, run_checks
from tests.unit.benchmarks.sregym.hosts.fake_target import FakeTarget

CHELAN4 = HostFacts(cores=64, mem_total_kib=263801904)
SPOKANE1 = HostFacts(cores=224, mem_total_kib=791087316)
MINIMAL = """
[hosts.h1]
lane_root = "/data/lanes"
[hosts.h1.guard]
lane_prefix = "h1-"
[hosts.h1.diskwatch]
enabled = false
"""


def test_slice_limits_derive_from_the_hosts_cores_and_ram() -> None:
    defaults = SliceSettings()
    chelan4 = derive_slice(defaults, CHELAN4)
    assert (chelan4.memory_high_bytes, chelan4.memory_max_bytes, chelan4.cpu_quota_pct) == (117 * GIB, 125 * GIB, 2800)
    spokane1 = derive_slice(defaults, SPOKANE1)
    assert (spokane1.memory_high_bytes, spokane1.memory_max_bytes, spokane1.cpu_quota_pct) == (
        354 * GIB,
        377 * GIB,
        9800,
    )


def test_slice_overrides_are_absolute() -> None:
    spec = derive_slice(SliceSettings(memory_max_gib=128, cpu_cores=40, cpu_weight=50), CHELAN4)
    assert (spec.memory_high_bytes, spec.memory_max_bytes, spec.cpu_quota_pct) == (120 * GIB, 128 * GIB, 4000)
    with pytest.raises(ValueError, match="memory_high_gib"):
        SliceSettings(memory_high_gib=9, memory_max_gib=8)


def test_rendered_slice_is_the_hand_applied_file() -> None:
    text = render_slice("sdo-eval.slice", derive_slice(SliceSettings(), CHELAN4))
    assert text.splitlines() == [
        "[Unit]",
        "Description=SDO eval lanes: hard caps so a storm inside our lanes cannot make the host unresponsive",
        "[Slice]",
        "MemoryAccounting=yes",
        "MemoryHigh=117G",
        "MemoryMax=125G",
        "CPUAccounting=yes",
        "CPUQuota=2800%",
        "CPUWeight=100",
        "TasksAccounting=yes",
        "TasksMax=16384",
        "IOAccounting=yes",
        "IOWeight=50",
    ]


def test_checked_in_hosts_parse_and_spokane1_keeps_everything_off_nfs() -> None:
    hosts = load_hosts(DEFAULT_CONFIG)
    assert {"chelan5", "chelan4", "spokane1"} <= set(hosts)
    assert hosts["spokane1"].lane_root.startswith("/mnt/nvme1/")
    assert "password" not in DEFAULT_CONFIG.read_text().lower()


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[hosts.h]\nlane_root='/x'\n[hosts.h.guard]\nlane_prefix='a'\nbogus=1\n", "bogus"),
        ("[hosts.h]\nlane_root='/x'\n", "guard"),
        ("[defaults.slice]\nmemory_max_pct=0\n" + MINIMAL, "memory_max_pct"),
        ("", "defines no"),
    ],
)
def test_bad_configuration_is_rejected(text: str, message: str) -> None:
    with pytest.raises(HostConfigError, match=message):
        parse_hosts(text)


def test_timespan_percent_reads_systemd_quota_strings() -> None:
    assert parse_timespan_percent("39s") == 3900
    assert parse_timespan_percent("2min 18s") == 13800
    assert parse_timespan_percent("infinity") is None


def test_env_merge_keeps_other_lines_and_export_style() -> None:
    text = "A=1\nexport SDO_HOST_LANE_PREFIX=old-\nB=2\n"
    merged = merge_env(text, {"SDO_HOST_LANE_PREFIX": "c5-", "SDO_HOST_LANE_BUDGET_GIB": "128"})
    assert merged.splitlines()[:3] == ["A=1", "export SDO_HOST_LANE_PREFIX=c5-", "B=2"]
    assert merged.splitlines()[-1] == "SDO_HOST_LANE_BUDGET_GIB=128"
    assert merge_env(merged, {"SDO_HOST_LANE_PREFIX": "c5-", "SDO_HOST_LANE_BUDGET_GIB": "128"}) == merged
    assert strip_env(merged, ("SDO_HOST_LANE_PREFIX", "SDO_HOST_LANE_BUDGET_GIB")) == "A=1\nB=2\n"


def _host(extra: str = "", *, mounts: dict[str, str] | None = None):
    hosts = parse_hosts(MINIMAL + extra)
    target = FakeTarget(fstype=mounts or {"/": "ext4"})
    target.commands[("systemctl", "is-active", "sdo-eval.slice")] = "inactive"
    ctx = Context(host=hosts["h1"], facts=CHELAN4, target=target, home="/home/u")
    return ctx, target


def test_plan_lists_root_and_user_changes_with_diffs() -> None:
    ctx, _ = _host()
    plan = build_plan(ctx)
    titles = [change.title for change in plan.changes]
    assert "write /etc/systemd/system/sdo-eval.slice" in titles
    assert "write /etc/tmpfiles.d/mglru.conf" in titles
    assert "write /etc/sysctl.d/90-sdo-eval.conf" in titles
    assert any(change.root for change in plan.changes)
    guard = next(change for change in plan.changes if change.step == "guard")
    assert "+SDO_HOST_LANE_BUDGET_GIB=125" in guard.diff
    assert "+SDO_HOST_LANE_PREFIX=h1-" in guard.diff


def test_plan_then_apply_then_plan_is_empty_for_files() -> None:
    ctx, target = _host()
    target.files["/sys/kernel/mm/lru_gen/enabled"] = b"0x0007\n"
    target.files["/sys/kernel/mm/lru_gen/min_ttl_ms"] = b"1000\n"
    plan = build_plan(ctx)
    for change in plan.changes:
        change.apply()
    for key, want in (("net/ipv4/neigh/default/gc_thresh1", 16384), ("fs/inotify/max_user_watches", 1048576)):
        target.files["/proc/sys/" + key] = f"{want}\n".encode()
    for key in ("gc_thresh2", "gc_thresh3"):
        target.files[f"/proc/sys/net/ipv4/neigh/default/{key}"] = b"99999\n"
    target.files["/proc/sys/fs/inotify/max_user_instances"] = b"8192\n"
    second = build_plan(ctx)
    assert [
        c.title
        for c in second.changes
        if c.step in {"slice", "mglru", "sysctl", "guard"} and c.title.startswith("write")
    ] == []


def test_mglru_is_skipped_with_a_warning_when_the_kernel_lacks_it() -> None:
    ctx, _ = _host()
    ctx = Context(
        host=ctx.host,
        facts=HostFacts(cores=64, mem_total_kib=263801904, has_lru_gen=False),
        target=ctx.target,
        home="/home/u",
    )
    plan = build_plan(ctx)
    assert any(note.step == "mglru" and note.level == "warn" for note in plan.notes)
    assert not any(change.step == "mglru" for change in plan.changes)
    assert next(c for c in run_checks(ctx) if c.name == "mglru").status == WARN


def test_nfs_lane_root_is_refused() -> None:
    ctx, _ = _host(mounts={"/": "nfs4"})
    plan = build_plan(ctx)
    assert [note.step for note in plan.blocked] == ["lane-root", "lane-root"]  # lane root and evidence root
    assert "nfs4" in plan.blocked[0].text
    ctx, target = _host(mounts={"/": "ext4", "/data": "nfs"})
    target.dirs.add("/data")
    assert len(build_plan(ctx).blocked) == 2
    ctx, target = _host(mounts={"/": "nfs4", "/data": "ext4"})
    target.dirs.add("/data")
    assert build_plan(ctx).blocked == []  # a local mount under an NFS root is fine


def test_lane_root_is_created_on_local_disk() -> None:
    ctx, _ = _host(mounts={"/": "ext4"})
    assert any(c.title == "create lane root /data/lanes" for c in build_plan(ctx).changes)


def test_root_changes_need_the_sudo_password_but_a_plan_never_runs_them() -> None:
    ctx, target = _host()
    plan = build_plan(ctx)
    assert plan.needs_root
    assert target.sudo_calls == []


def test_apply_installs_root_files_through_sudo() -> None:
    ctx, target = _host()
    for change in build_plan(ctx).changes:
        change.apply()
    assert ["install", "/etc/systemd/system/sdo-eval.slice"] in target.sudo_calls
    assert ["systemctl", "start", "sdo-eval.slice"] in target.sudo_calls


def test_remove_plans_deletions_and_keeps_foreign_env_lines() -> None:
    ctx, target = _host()
    for change in build_plan(ctx).changes:
        change.apply()
    plan = plan_remove(ctx)
    titles = [c.title for c in plan.changes]
    assert "delete /etc/systemd/system/sdo-eval.slice" in titles
    assert "delete /home/u/.config/sdo/host.env" in titles
    assert any(note.step == "remove" for note in plan.notes)


def _credential_hosts():
    extra = '[hosts.h1.credentials]\nprofile_dir = "/home/u/.claude-gcp"\nkey_path = "/home/u/key.json"\n'
    ctx, target = _host(extra)
    source = FakeTarget(name="src")
    source.files["/src/profile/settings.json"] = json.dumps(
        {"env": {"GOOGLE_APPLICATION_CREDENTIALS": "/src/key.json", "X": "1"}}
    ).encode()
    source.files["/src/profile/.claude.json"] = b'{"a": "TOPSECRET"}'
    source.files["/src/key.json"] = b"PRIVATE-KEY-BYTES"
    return ctx, target, source


def test_credentials_copy_only_essentials_rewrites_the_key_path_and_never_shows_content() -> None:
    ctx, target, source = _credential_hosts()
    plan = plan_credentials(ctx, source, ("/src/profile", "/src/key.json"))
    shown = "\n".join(change.title + change.diff for change in plan.changes)
    assert "TOPSECRET" not in shown
    assert "PRIVATE-KEY-BYTES" not in shown
    assert "/src/key.json" not in shown
    for change in plan.changes:
        change.apply()
    assert (
        json.loads(target.files["/home/u/.claude-gcp/settings.json"])["env"]["GOOGLE_APPLICATION_CREDENTIALS"]
        == "/home/u/key.json"
    )
    assert target.files["/home/u/key.json"] == b"PRIVATE-KEY-BYTES"
    assert set(target.files) == {
        "/home/u/.claude-gcp/settings.json",
        "/home/u/.claude-gcp/.claude.json",
        "/home/u/key.json",
    }
    assert plan_credentials(ctx, source, ("/src/profile", "/src/key.json")).changes == []


def test_credentials_missing_at_the_source_block_the_plan() -> None:
    ctx, _, source = _credential_hosts()
    del source.files["/src/key.json"]
    assert plan_credentials(ctx, source, ("/src/profile", "/src/key.json")).blocked


def test_verify_reports_a_failing_table_for_an_unset_host() -> None:
    ctx, _ = _host()
    checks = run_checks(ctx)
    by_name = {c.name: c.status for c in checks}
    assert by_name["sdo-eval.slice"] == FAIL
    assert by_name["guard-config"] == FAIL
    table = render_table("h1", checks)
    assert "-> FAIL" in table
    assert "TOPSECRET" not in table


def test_verify_passes_a_fully_configured_host() -> None:
    ctx, target = _host()
    for change in build_plan(ctx).changes:
        if change.step in {"guard"}:
            change.apply()
    props = {
        "ActiveState": "active",
        "MemoryHigh": str(117 * GIB),
        "MemoryMax": str(125 * GIB),
        "CPUWeight": "100",
        "TasksMax": "16384",
        "IOWeight": "50",
        "CPUQuotaPerSecUSec": "28s",
    }
    target.commands[("systemctl", "show", "sdo-eval.slice", *[a for p in SLICE_PROPS for a in ("-p", p)])] = "\n".join(
        f"{k}={v}" for k, v in props.items()
    )
    status = {c.name: c.status for c in run_checks(ctx)}
    assert status["sdo-eval.slice"] == PASS
    assert status["guard-config"] == PASS


def test_sudo_password_file_is_read_without_exposing_it(tmp_path, monkeypatch, capsys) -> None:
    from benchmarks.sregym.hosts.target import TargetError, read_password

    file = tmp_path / "pw"
    file.write_text("hunter2\nignored\n")
    assert read_password(str(file)) == "hunter2"
    monkeypatch.setenv("SUDO_PASSWORD_FILE", str(file))
    assert read_password(None) == "hunter2"
    monkeypatch.delenv("SUDO_PASSWORD_FILE")
    assert read_password(None) is None
    (tmp_path / "empty").write_text("")
    with pytest.raises(TargetError, match="empty"):
        read_password(str(tmp_path / "empty"))
    assert "hunter2" not in capsys.readouterr().out


def test_cli_rejects_an_unknown_host(tmp_path, capsys) -> None:
    config = tmp_path / "h.toml"
    config.write_text(MINIMAL)
    assert cli.main(["--config", str(config), "verify", "--host", "nope", "--local"]) == 2
    assert "known: h1" in capsys.readouterr().err


SERVICE = (
    "[Service]\nExecStart=/bin/bash /opt/dw/sdo-diskwatch.sh\nExecStartPost=-/bin/bash /opt/dw/sdo-diskcollect.sh\n"
)


def _diskwatch_ctx(extra: str = "", *, controller: str | None = None):
    hosts = parse_hosts(MINIMAL.replace("enabled = false", "enabled = true") + extra)
    target = FakeTarget()
    ctx = Context(
        host=hosts["h1"],
        facts=CHELAN4,
        target=target,
        home="/home/u",
        controller_read=lambda path: b"#!/bin/bash\necho watch\n" if path == controller else None,
        controller_script=controller,
    )
    return ctx, target


def test_installed_watcher_paths_are_adopted() -> None:
    assert installed_diskwatch(SERVICE) == ("/opt/dw/sdo-diskwatch.sh", "/opt/dw/sdo-diskcollect.sh")
    assert installed_diskwatch(None) == (None, None)
    ctx, target = _diskwatch_ctx()
    assert diskwatch_paths(ctx)[0] == "/home/u/.local/share/sdo/diskwatch/sdo-diskwatch.sh"
    target.files["/home/u/.config/systemd/user/sdo-diskwatch.service"] = SERVICE.encode()
    assert diskwatch_paths(ctx) == ("/opt/dw/sdo-diskwatch.sh", "/opt/dw/sdo-diskcollect.sh")


def test_new_host_gets_the_controllers_watcher_script_and_a_user_timer() -> None:
    ctx, target = _diskwatch_ctx(controller="/ctl/sdo-diskwatch.sh")
    plan = build_plan(ctx)
    titles = [c.title for c in plan.changes if c.step == "diskwatch"]
    assert "copy watcher script to /home/u/.local/share/sdo/diskwatch/sdo-diskwatch.sh" in titles
    assert any("sdo-diskwatch.timer" in t for t in titles)
    assert any("enable and start the timer" in t for t in titles)
    for change in plan.changes:
        change.apply()
    assert target.files["/home/u/.local/share/sdo/diskwatch/sdo-diskwatch.sh"].startswith(b"#!/bin/bash")
    unit = target.files["/home/u/.config/systemd/user/sdo-diskwatch.service"].decode()
    assert "ExecStart=/bin/bash /home/u/.local/share/sdo/diskwatch/sdo-diskwatch.sh" in unit


def test_missing_watcher_with_no_controller_copy_blocks_the_plan() -> None:
    ctx, _ = _diskwatch_ctx()
    assert [n.step for n in build_plan(ctx).blocked if n.step == "diskwatch"] == ["diskwatch"]


def test_existing_credential_profile_is_tightened_in_place_without_copying() -> None:
    ctx, target, _ = _credential_hosts()
    target.dirs.add("/home/u/.claude-gcp")
    target.modes["/home/u/.claude-gcp"] = "755"
    for path in ("/home/u/.claude-gcp/settings.json", "/home/u/.claude-gcp/.claude.json", "/home/u/key.json"):
        target.files[path] = b"x"
        target.modes[path] = "644" if path.endswith("key.json") else "600"
    plan = plan_credential_modes(ctx)
    assert [c.title for c in plan.changes] == ["chmod 700 /home/u/.claude-gcp", "chmod 600 /home/u/key.json"]
    for change in plan.changes:
        change.apply()
    assert plan_credential_modes(ctx).changes == []
    assert target.files["/home/u/key.json"] == b"x"
    assert all(c.step != "credentials" for c in build_plan(ctx).changes)  # tightened: nothing left to do
