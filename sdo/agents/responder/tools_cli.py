"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from sdo.agents.responder import recorded_actions

PLAN_ACTIONS = (
    "SetField updates a schema-checked field by index or unique name, optionally from accepted source; "
    "ConfigMap JSON/YAML entries preserve unrelated keys."
)

CLI_EXAMPLES = (
    "sdo source Deployment/app/api",
    "sdo diagnose targetport --bind service=Service/app/api",
    "sdo plan targetport --bind service=Service/app/api",
)
ACT_EXAMPLE = "sdo act --resource Deployment/app/api -- kubectl -n app set image deployment/api api=img:1.2"


def _resource(value: str) -> dict[str, str]:
    try:
        return recorded_actions.resource_ref(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _binding(value: str) -> tuple[str, str]:
    name, separator, target = value.partition("=")
    if not separator or not name or not target:
        raise argparse.ArgumentTypeError("binding must be name=Kind/namespace/name")
    # Go owns object-reference parsing and API-version resolution.
    return name, target


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sdo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples (replace names with incident resources):\n  " + "\n  ".join(CLI_EXAMPLES),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="read current findings and session state")
    for name in ("diagnose", "plan"):
        command = commands.add_parser(
            name,
            help="evaluate a bound program" if name == "diagnose" else "plan a bound program repair",
            description=PLAN_ACTIONS if name == "plan" else None,
            epilog="Example: " + CLI_EXAMPLES[1 if name == "diagnose" else 2],
        )
        command.add_argument("program_id", nargs="?" if name == "diagnose" else None)
        command.add_argument(
            "--bind",
            action="append",
            type=_binding,
            default=[],
            metavar="NAME=Kind/namespace/name",
            help="named program parameter; repeat for each binding",
        )
    commands.add_parser("apply", help="apply a previously planned repair").add_argument("plan_ids", nargs="+")
    commands.add_parser(
        "source", help="inspect accepted source for one resource", epilog="Example: " + CLI_EXAMPLES[0]
    ).add_argument("ref", metavar="Kind/namespace/name")
    act = commands.add_parser(
        "act",
        help="run a live mutation and record its receipt",
        description=(
            "Run one live mutation and record its repair receipt (target, resources, timing, outcome and "
            "command line). The command's exit code is returned. Do not hand-write receipts."
        ),
        epilog="Example: " + ACT_EXAMPLE,
    )
    act.add_argument(
        "--resource",
        action="append",
        type=_resource,
        required=True,
        metavar="Kind/namespace/name",
        help="object the command mutates (Kind/name when cluster-scoped); repeat for each one",
    )
    act.add_argument("--summary", help="one line describing the repair")
    act.add_argument("--irreversible", action="store_true", help="the mutation cannot be undone")
    act.add_argument("act_command", nargs=argparse.REMAINDER, metavar="-- COMMAND", help="the mutating command")
    commands.add_parser("verify", help="verify health and retained drift findings")
    commands.add_parser("rollback", help="roll back a plan application").add_argument("application_id")
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
    elif args.command == "source":
        request["ref"] = args.ref
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


def _act(args: argparse.Namespace) -> int:
    command = args.act_command[1:] if args.act_command[:1] == ["--"] else args.act_command
    try:
        if not command:
            raise ValueError("a command is required after --")
        # Capture is evidence collection, not mutation authority. Keep repairs and
        # their receipts available even when the controller cannot capture.
        try:
            response = _exchange({"command": "pre-apply"}, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
            if response.get("error"):
                raise ValueError(str(response["error"]))
        except (OSError, ValueError, TimeoutError) as exc:
            print(f"sdo: pre-apply evidence gap: {exc}; proceeding with repair", file=sys.stderr)
        return recorded_actions.run_and_record(
            command, args.resource, summary=args.summary, reversible=not args.irreversible
        )
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"sdo: {exc}", file=sys.stderr)
        return 2


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "act":
        return _act(args)
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
