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
    "ConfigMap JSON/YAML entries preserve unrelated keys.\n"
    "CreateFromSource restores a named missing object from accepted source.\n"
    "DeleteObject irreversibly removes an allowed named source-absent object and never auto-recreates it."
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
    detect = commands.add_parser("detect", help="run all or selected detectors on fresh state (read-only)")
    detect.add_argument(
        "--detector",
        action="extend",
        nargs="+",
        default=[],
        metavar="ID",
        help="detector IDs to run; repeat for more IDs (default: all)",
    )
    for name in ("diagnose", "plan", "menu"):
        command = commands.add_parser(
            name,
            help={
                "diagnose": "evaluate a bound program",
                "plan": "plan a bound program repair",
                "menu": "list controller mitigation options",
            }[name],
            description=PLAN_ACTIONS if name == "plan" else None,
            epilog="Example: " + CLI_EXAMPLES[1 if name == "diagnose" else 2],
        )
        command.add_argument("program_id", nargs=None if name == "plan" else "?")
        command.add_argument(
            "--bind",
            action="append",
            type=_binding,
            default=[],
            metavar="NAME=Kind/namespace/name",
            help="named program parameter; repeat for each binding",
        )
    commands.add_parser("apply", help="apply a previously planned repair").add_argument("plan_ids", nargs="+")
    mitigate = commands.add_parser(
        "mitigate",
        help="run a program or one-action plan through the ordinary executor",
        description=(
            "Run an option with --NAME VALUE parameters. Read the typed report before trying another option. "
            "Uses the ordinary executor and controller RBAC; up to five rounds per incident."
        ),
    )
    mitigate.add_argument("option", nargs="?", help="option ID or unambiguous program ID from the current menu")
    mitigate.add_argument("--rounds", type=int, help="maximum rounds within the remaining incident allowance")
    mitigate.add_argument("--selection", type=Path, metavar="FILE")
    mitigate.add_argument("--param", action="append", type=_binding, default=[], metavar="NAME=VALUE")
    report = commands.add_parser("report", help="retrieve a settled mitigation report")
    report.add_argument("attempt_id")
    report.add_argument("--incident-id", required=True)
    report.add_argument("--session-id", required=True)
    report.add_argument("--epoch", type=int, required=True)
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
    if args.command == "detect":
        if args.detector:
            request["detector_ids"] = args.detector
    elif args.command in ("diagnose", "plan", "menu"):
        if args.program_id:
            request["program_id"] = args.program_id
        if args.bind:
            bindings = dict(args.bind)
            if len(bindings) != len(args.bind):
                raise ValueError("duplicate binding name")
            request["bindings"] = bindings
    elif args.command == "mitigate":
        if bool(args.selection) == bool(args.option):
            raise ValueError("choose an option or a selection file")
        parameters = dict(args.param)
        if len(parameters) != len(args.param):
            raise ValueError("duplicate parameter name")
        for name, value in getattr(args, "named_parameters", []):
            if name in parameters:
                raise ValueError("duplicate parameter name")
            parameters[name] = value
        request["selection"] = _selection(args.selection) if args.selection else _option_selection(args, parameters)
        if parameters:
            request["parameters"] = parameters
    elif args.command == "report":
        if not args.attempt_id or not args.incident_id or not args.session_id or not 0 < args.epoch < 2**64:
            raise ValueError("report requires nonempty IDs and a positive uint64 epoch")
        request["report_query"] = {
            "version": 1,
            "incident_id": args.incident_id,
            "session_id": args.session_id,
            "dispatch_epoch": str(args.epoch),
            "attempt_id": args.attempt_id,
        }
    elif args.command == "apply":
        request["plan_ids"] = args.plan_ids
    elif args.command == "rollback":
        request["application_id"] = args.application_id
    elif args.command == "source":
        request["ref"] = args.ref
    return request


def _option_selection(args: argparse.Namespace, parameters: dict[str, str]) -> dict[str, object]:
    response = _exchange({"command": "menu"}, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    menu = response.get("menu")
    if response.get("error") or not isinstance(menu, dict):
        raise ValueError("current mitigation menu unavailable")
    exact = [option for option in menu.get("options", []) if args.option == option.get("option_id")]
    options = exact or [option for option in menu.get("options", []) if args.option == option.get("program_id")]
    if not exact and len(options) > 1 and parameters:
        # Several finding-derived rows may name one program. Let Go parse and
        # validate explicit object/scalar parameters into one concrete option.
        response = _exchange(
            {"command": "menu", "program_id": args.option, "parameters": parameters},
            Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")),
        )
        menu = response.get("menu")
        if response.get("error") or not isinstance(menu, dict):
            raise ValueError("bound mitigation menu unavailable")
        options = [option for option in menu.get("options", []) if args.option == option.get("program_id")]
    if len(options) != 1 or options[0].get("disabled_reason"):
        raise ValueError("mitigation option unavailable or ambiguous")
    option = options[0]
    limit = int(menu.get("remaining_batches", menu.get("round_limit", 0)))
    rounds = args.rounds if args.rounds is not None else 1
    if not 1 <= rounds <= limit:
        raise ValueError("rounds exceed remaining incident allowance")
    selection = {
        "version": 1,
        "option_id": option["option_id"],
        "request_id": "request-" + uuid.uuid4().hex,
        "round_limit": rounds,
    }
    return selection


def _selection(path: Path) -> dict[str, object]:
    # Keep help and all existing tools independent of protobuf startup costs.
    from google.protobuf.json_format import ParseError

    from sdo.contracts import MitigationSelection, parse_json, to_canonical_json, validate

    try:
        selection = parse_json(path.read_text(encoding="utf-8"), MitigationSelection())
        validate(selection)
    except (OSError, UnicodeError, ValueError, ParseError) as exc:
        # Parser/validation errors may contain agent-supplied opaque values.
        raise ValueError(
            "invalid mitigation selection; supply a complete version 1 selection within the incident round allowance"
        ) from exc
    return json.loads(to_canonical_json(selection))


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
            response = _exchange(
                {"command": "pre-apply", "refs": args.resource},
                Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")),
            )
            if response.get("error"):
                raise ValueError(str(response["error"]))
        except (OSError, ValueError, TimeoutError) as exc:
            print(f"sdo: pre-apply evidence gap: {exc}; proceeding with repair", file=sys.stderr)
        result = recorded_actions.run_and_record(
            command, args.resource, summary=args.summary, reversible=not args.irreversible
        )
        return result
    except (OSError, ValueError, TimeoutError) as exc:
        print(f"sdo: {exc}", file=sys.stderr)
        return 2


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args, extra = parser.parse_known_args(argv)
    if extra:
        if args.command != "mitigate" or len(extra) % 2:
            parser.error("parameters must use --NAME VALUE")
        pairs = list(zip(extra[::2], extra[1::2], strict=True))
        if any(not name.startswith("--") or len(name) == 2 or value.startswith("--") for name, value in pairs):
            parser.error("parameters must use --NAME VALUE")
        args.named_parameters = [(name[2:], value) for name, value in pairs]
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
