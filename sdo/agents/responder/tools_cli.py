"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

CLI_EXAMPLES = (
    "sdo source Deployment/app/api",
    "sdo diagnose targetport --bind service=Service/app/api",
    "sdo plan targetport --bind service=Service/app/api",
)


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
    commands.add_parser(
        "status",
        help="Show current incident findings",
        description="Show current incident findings. The task's prior_stages lists what automatic stages "
        "already applied.",
    )
    for name in ("diagnose", "plan"):
        command = commands.add_parser(name, epilog="Example: " + CLI_EXAMPLES[1 if name == "diagnose" else 2])
        command.add_argument("program_id", nargs="?" if name == "diagnose" else None)
        command.add_argument(
            "--bind",
            action="append",
            type=_binding,
            default=[],
            metavar="NAME=Kind/namespace/name",
            help="named program parameter; repeat for each binding",
        )
    commands.add_parser(
        "apply",
        help="Apply plans returned by sdo plan",
        description="Apply plans returned by sdo plan. An attempt recorded as applied in prior_stages already wrote: "
        "re-check live state before applying it again.",
    ).add_argument("plan_ids", nargs="+")
    commands.add_parser("source", epilog="Example: " + CLI_EXAMPLES[0]).add_argument(
        "ref", metavar="Kind/namespace/name"
    )
    verify = commands.add_parser(
        "verify",
        help="Verify current health and run a bounded traffic burst through the controller and active memory host",
    )
    verify.add_argument("--workload")
    verify.add_argument("--scenario", action="append", default=[])
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
    elif args.command == "verify":
        if args.workload:
            request["workload"] = args.workload
        if args.scenario:
            request["scenarios"] = args.scenario
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


def verify_incident() -> dict[str, object]:
    """Run bounded verification through the controller's incident session."""
    return _exchange({"command": "verify"}, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        response = _exchange(_request(args), Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"sdo: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(response, sort_keys=True))
    print(response.get("summary", ""))
    if args.command == "verify":
        return 3 if response.get("error") else (0 if response.get("healthy") else 1)
    return 1 if response.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
