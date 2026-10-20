"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path


def _binding(value: str) -> tuple[str, dict[str, str]]:
    name, separator, target = value.partition("=")
    parts = target.split("/")
    if not separator or not name or len(parts) != 3 or not all(parts):
        raise argparse.ArgumentTypeError("binding must be name=Kind/namespace/name")
    kind, namespace, resource = parts
    api_version = "apps/v1" if kind in ("Deployment", "ReplicaSet") else "v1"
    return name, {"api_version": api_version, "kind": kind, "namespace": namespace, "name": resource}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sdo")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    for name in ("diagnose", "plan"):
        command = commands.add_parser(name)
        command.add_argument("program_id", nargs="?" if name == "diagnose" else None)
        command.add_argument("--bind", action="append", type=_binding, default=[])
    commands.add_parser("apply").add_argument("plan_ids", nargs="+")
    commands.add_parser("verify")
    commands.add_parser("rollback").add_argument("application_id")
    return parser


def _request(args: argparse.Namespace) -> dict[str, object]:
    request: dict[str, object] = {"command": args.command}
    if args.command in ("diagnose", "plan"):
        if args.program_id:
            request["program_id"] = args.program_id
        if args.bind:
            bindings = dict(args.bind)
            if len(bindings) != len(args.bind):
                raise ValueError("duplicate binding name")
            request["bindings"] = bindings
    elif args.command == "apply":
        request["plan_ids"] = args.plan_ids
    elif args.command == "rollback":
        request["application_id"] = args.application_id
    return request


def _exchange(request: dict[str, object], directory: Path) -> dict[str, object]:
    calls, responses = directory / "requests", directory / "responses"
    if not calls.is_dir() or not responses.is_dir():
        raise OSError("SDO tools are unavailable in this responder session")
    identifier = uuid.uuid4().hex + ".json"
    temporary = calls / ("." + identifier)
    target = calls / identifier
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(request, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    response = responses / identifier
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            data = response.read_text(encoding="utf-8")
        except FileNotFoundError:
            time.sleep(0.05)
            continue
        response.unlink(missing_ok=True)
        parsed = json.loads(data)
        if not isinstance(parsed, dict):
            raise ValueError("invalid controller tool response")
        return parsed
    raise TimeoutError("SDO tool response timed out")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        response = _exchange(_request(args), Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"sdo: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(response, sort_keys=True))
    print(response.get("summary", ""))
    return 1 if response.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
