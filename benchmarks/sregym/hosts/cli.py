"""``plan`` shows what would change, ``apply`` does it, ``verify`` reports pass/fail, ``remove`` undoes the setup.

Run it on the host itself, or from another machine with ``--host NAME`` (ssh to the name in ``hosts.toml``). Root
steps use ``sudo -S`` with the password read from ``--sudo-password-file`` or ``SUDO_PASSWORD_FILE``; it is never
printed. Credentials are copied only with ``--credentials-from HOST``.
"""

from __future__ import annotations

import argparse
import socket
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.hosts.config import DEFAULT_CONFIG, HostConfig, HostConfigError, load_hosts
from benchmarks.sregym.hosts.facts import gather_facts
from benchmarks.sregym.hosts.steps import Context, Plan, build_plan, installed_diskwatch, plan_credentials, plan_remove
from benchmarks.sregym.hosts.target import LocalTarget, SshTarget, Target, TargetError, read_password
from benchmarks.sregym.hosts.verify import FAIL, render_table, run_checks

if TYPE_CHECKING:
    from collections.abc import Sequence


def local_name() -> str:
    return socket.gethostname().split(".")[0]


def make_target(host: HostConfig, password: str | None, *, force_local: bool = False) -> Target:
    if force_local or host.name == local_name():
        return LocalTarget(host.name, password)
    return SshTarget(host.name, host.ssh or host.name, password)


def make_context(host: HostConfig, target: Target) -> Context:
    facts, home = gather_facts(target)
    unit = Path("~/.config/systemd/user/sdo-diskwatch.service").expanduser()
    script = installed_diskwatch(unit.read_text() if unit.is_file() else None)[0]
    return Context(host=host, facts=facts, target=target, home=home, controller_script=script)


def credentials_plan(ctx: Context, hosts: dict[str, HostConfig], source_name: str, *, local: bool) -> Plan:
    source = hosts.get(source_name)
    if source is None or source.credentials is None:
        raise HostConfigError(f"--credentials-from {source_name}: no such host with a [credentials] table")
    source_target = make_target(source, None, force_local=local and source_name == ctx.host.name)
    source_ctx = make_context(source, source_target)
    return plan_credentials(
        ctx,
        source_target,
        (source_ctx.path(source.credentials.profile_dir), source_ctx.path(source.credentials.key_path)),
    )


def render_plan(host: str, plan: Plan) -> str:
    lines = [f"== {host}: {len(plan.changes)} change(s)"]
    lines.extend(f"[{note.level.upper()}] {note.step}: {note.text}" for note in plan.notes)
    for change in plan.changes:
        lines.append(f"-- [{change.step}] {change.title}{' (root)' if change.root else ''}")
        lines.append(change.diff.rstrip("\n"))
    if not plan.changes:
        lines.append("nothing to do")
    return "\n".join(lines)


def execute(plan: Plan) -> int:
    failures = 0
    for change in plan.changes:
        try:
            change.apply()
            print(f"ok   [{change.step}] {change.title}")
        except TargetError as exc:
            print(f"{'warn' if change.best_effort else 'FAIL'} [{change.step}] {change.title}: {exc}")
            failures += 0 if change.best_effort else 1
            if not change.best_effort:
                break
    return failures


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="python -m benchmarks.sregym.hosts", description=__doc__)
    root.add_argument(
        "--config", type=str, default=str(DEFAULT_CONFIG), help="hosts.toml (default: the checked-in one)"
    )
    sub = root.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("plan", "dry run: show every change and diff"),
        ("apply", "make the host match the config"),
        ("verify", "pass/fail table of the host's state"),
        ("remove", "undo the setup (leaves lane root, watcher script, credentials)"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--host", default=None, help="entry in hosts.toml (default: this machine)")
        cmd.add_argument("--local", action="store_true", help="run on this machine whatever --host says")
        cmd.add_argument("--sudo-password-file", default=None, help="file whose first line is the sudo password")
        if name in {"plan", "apply"}:
            cmd.add_argument(
                "--credentials-from", default=None, metavar="HOST", help="opt in: copy the ClaudeGCP profile"
            )
        if name in {"apply", "remove"}:
            cmd.add_argument("--dry-run", action="store_true", help="show the plan only")
        if name == "verify":
            cmd.add_argument(
                "--smoke", action="store_true", help="one real model call with the credentials (costs tokens)"
            )
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        hosts = load_hosts(Path(args.config))
        name = args.host or local_name()
        if name not in hosts:
            raise HostConfigError(f"no host {name!r} in {args.config}; known: {', '.join(sorted(hosts))}")
        host = hosts[name]
        target = make_target(host, read_password(args.sudo_password_file), force_local=args.local)
        ctx = make_context(host, target)
        if args.command == "verify":
            checks = run_checks(ctx, smoke=args.smoke)
            print(render_table(name, checks))
            return 1 if any(check.status == FAIL for check in checks) else 0
        if args.command == "remove":
            plan = plan_remove(ctx)
        else:
            plan = build_plan(ctx)
            if args.credentials_from:
                extra = credentials_plan(ctx, hosts, args.credentials_from, local=args.local)
                plan.changes.extend(extra.changes)
                plan.notes.extend(extra.notes)
        print(render_plan(name, plan))
        dry = args.command == "plan" or getattr(args, "dry_run", False)
        if plan.blocked:
            print("refusing: fix the blocked items above first", file=sys.stderr)
            return 2
        if dry:
            return 0
        return 1 if execute(plan) else 0
    except (HostConfigError, TargetError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
