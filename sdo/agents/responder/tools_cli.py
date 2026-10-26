"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from sdo.agents.responder import recorded_actions
from sdo.agents.responder.mutation_output import compact_mutation_envelope
from sdo.agents.responder.tool_output import render_summary, write_response

PLAN_ACTIONS = (
    "SetField updates a schema-checked field by index or unique name, optionally from accepted source; "
    "ConfigMap JSON/YAML entries preserve unrelated keys.\n"
    "CreateFromSource restores a named missing object from accepted source.\n"
    "ReconcileFromSource restores declared configuration, including source-named cluster objects, "
    "while preserving undeclared fields.\n"
    "DeleteObject irreversibly removes an allowed named source-absent object and never auto-recreates it."
)

WORKING_PROGRAM_HELP = (
    "Program commands use the current checkout, including shared libraries and new playbook IDs. "
    "Edits compile on demand; fix compile errors in place and retry. "
    "Revision receipts use working-tree:<source digest> for unpublished code. "
    "Reusable API reads use program.Kubernetes(view) with upstream client-go options; reads are recorded for replay."
)

CLI_EXAMPLES = (
    "sdo source Deployment/app/api",
    "sdo diagnose targetport --bind service=Service/app/api",
    "sdo plan targetport --bind service=Service/app/api",
)
ACT_EXAMPLE = "sdo act --resource Deployment/app/api -- kubectl -n app set image deployment/api api=img:1.2"

MUTATION_COMMANDS = frozenset({"act", "apply", "mitigate", "rollback"})

# Key response paths are part of the CLI help contract, shared with the generated
# responder reference. Fields are conditional, not a promise of success/presence.
RESPONSE_FIELDS = {
    "status": "healthy, findings[], detector_states[], drift_events[], program_runs[]",
    "detect": "healthy, findings[], detector_states[]",
    "check": "healthy, findings[], detector_states[], detection (fresh detect response)",
    "diagnose": "program_runs[].{program_id, program_revision, verdict, can_mitigate}, program_revisions",
    "plan": "plan_id, patches[], program_runs[], program_revisions",
    "menu": "menu.options[].{option_id, program_id, program_revision, disabled_reason, parameters}, "
    "selection_schema, selection_set_schema, unexplained_removal_blockers[]",
    "source": "source (accepted Kubernetes object)",
    "verify": "healthy, unresolved, findings[], detector_states[], drift_events[]",
    "report": "report.{status, attempt_id, rounds}, session",
    "act": "actions[].{success, exit_code, receipt, output, failure_reason}",
    "apply": "actions[].{application_id, targeted_healthy, mitigation_health, program_runs, program_revisions, report}",
    "mitigate": (
        "actions[].{application_id, targeted_healthy, mitigation_health, rollout_pending, program_revisions}, "
        "option_reports[]"
    ),
    "rollback": "actions[].{application_id, summary, error}",
}


def response_fields_help(command: str, *, markdown: bool = False) -> str:
    """Name which JSON document owns the fields, including bounded mutations."""
    if command in MUTATION_COMMANDS:
        location = "`details_file`" if markdown else "details_file"
        label = f"Complete report fields ({location})"
    else:
        label = "Key JSON fields"
    fields = RESPONSE_FIELDS[command]
    return label + ": " + (f"`{fields}`." if markdown else fields)


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


class _OptionArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        groups = list(getattr(namespace, "option_groups", None) or [])
        groups.append({"option_id": values, "parameters": {}})
        namespace.option_groups = groups


class _OptionParameter(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        groups = getattr(namespace, "option_groups", None)
        if groups:
            if not isinstance(values, tuple):
                raise argparse.ArgumentError(self, "parameter must be NAME=VALUE")
            name, value = values
            if name in groups[-1]["parameters"]:
                parser.error("duplicate parameter name for option")
            groups[-1]["parameters"][name] = value
        else:
            namespace.param = [*getattr(namespace, "param", []), values]


class _OptionFinding(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        groups = getattr(namespace, "option_groups", None)
        if groups:
            groups[-1].setdefault("finding_fingerprints", []).append(values)
        else:
            namespace.finding = [*getattr(namespace, "finding", []), values]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sdo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples (replace names with incident resources):\n  " + "\n  ".join(CLI_EXAMPLES),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    parser.add_argument(
        "--json",
        action="store_true",
        help="print JSON for scripts (mutations: compact envelope and complete report path)",
    )
    commands.add_parser("status", help="read fresh findings and detector confirmation; pending is not healthy")
    for name, help_text in (
        ("detect", "run fresh detectors; pending with a reason means health is unproven"),
        ("check", "run fresh detectors then independent closure verification"),
    ):
        check = commands.add_parser(name, help=help_text)
        check.add_argument(
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
            description=(PLAN_ACTIONS + "\n" + WORKING_PROGRAM_HELP) if name == "plan" else WORKING_PROGRAM_HELP,
            epilog="Example: " + CLI_EXAMPLES[1 if name == "diagnose" else 2],
        )
        command.add_argument("program_id", nargs=None if name == "plan" else "?")
        if name == "plan":
            command.add_argument(
                "--finding",
                action="append",
                default=[],
                metavar="FINGERPRINT",
                help="target finding fingerprint; repeat for each intended finding",
            )
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
        help="apply several independent options in one call; overlapping options refused; show progress and full JSON",
        description=(
            "Apply several independent options in one call; overlapping object write sets are refused before writes. "
            "Advertised option IDs remain valid for this session across filtered menu calls. "
            "Learned program preconditions are rechecked against live state before any batch writes. "
            "Use repeated --option ID followed by that option's --param NAME=VALUE, or --selection-set FILE "
            "with version 1 and options [{option_id, bindings, parameters, finding_fingerprints}]. "
            "Each option has its own receipt; one call uses one round. Pending rollout returns its application ID "
            "and read-only tools remain available. A single option also accepts --NAME VALUE parameters. "
            "Uses the ordinary executor and controller RBAC; up to five rounds per incident. "
            "A script fix is a first-class learnable mitigation with relation-derived Values, within incident budgets."
            + " "
            + WORKING_PROGRAM_HELP
        ),
        epilog=(
            "Parameters accept ordinary JSON objects, arrays, numbers and booleans, as well as strings. "
            "sdo menu exports recursive parameters[].schema for every option, plus selection_schema and "
            "selection_set_schema; nested properties, required fields and oneOf choices describe accepted shapes. "
            "parameters[].example supplies typed synthetic inputs; replace names/values using incident evidence. "
            "CLI --param NAME=VALUE accepts JSON text for structured values. "
            "EnsureDatabaseUser repairs a MongoDB user in one call. Its password and target.auth "
            "username/password take exactly one credential reference: {env: NAME} or "
            "{config_map: {name: CONFIGMAP, key: KEY, assignment: optional_NAME}} in the target Pod namespace. "
            "Assignment selects one literal NAME=value line. The executor resolves values privately; "
            "never put credential values in arguments, evidence or memory. Direct Secret keys are unavailable."
        ),
    )
    mitigate.add_argument("option", nargs="?", help="option ID or unambiguous program ID from the current menu")
    mitigate.add_argument(
        "--finding",
        action=_OptionFinding,
        default=[],
        metavar="FINGERPRINT",
        help="target finding fingerprint; repeat for each intended finding",
    )
    mitigate.add_argument("--rounds", type=int, help="maximum rounds within the remaining incident allowance")
    mitigate.add_argument("--selection", type=Path, metavar="FILE")
    mitigate.add_argument(
        "--selection-set",
        type=Path,
        metavar="FILE",
        help="version 1 selection set with per-option bindings and parameters",
    )
    mitigate.add_argument(
        "--option",
        action=_OptionArgument,
        dest="option_groups",
        metavar="ID",
        help="independent option ID; repeat, with per-option --param arguments",
    )
    mitigate.add_argument("--param", action=_OptionParameter, type=_binding, default=[], metavar="NAME=VALUE")
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
        help="record a live mutation; show progress and a complete JSON report",
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
    verify = commands.add_parser(
        "verify",
        help="read cached controller verification; optionally await a stable verdict",
        description="Reads the controller's cached health and inventory verdict immediately. "
        "Pending observation windows, including unproven synthetic scenarios, are not healthy. "
        "Wait on SDO's verification instead of sleeping. After closure, incident_verified is an "
        "informational warning on every response; reads and reports remain available, while mutations are rejected. "
        "After verified closure, report grace and outcome/reflection work continue during maintenance pauses.",
    )
    verify.add_argument(
        "--wait",
        type=float,
        default=0,
        metavar="SECONDS",
        help="wait for stable verification, a new/reactivated finding, or this timeout (0: cached and instant)",
    )
    commands.add_parser("rollback", help="roll back a plan application").add_argument("application_id")
    for name in ("mitigate", "act", "apply", "rollback"):
        specific_help = commands.choices[name].epilog
        commands.choices[name].epilog = (specific_help + "\n\n" if specific_help else "") + (
            "Action output separates target progress from incident verification and lists fixed, new "
            "(possible regression), and unchanged findings from authoritative controller progress. "
            "Mutation stdout is at most 2048 UTF-8 bytes, including JSON framing and newline. Every invocation saves "
            "the complete findings/evidence, program_runs, execution reports and receipts under "
            "$SDO_SESSION_SCRATCH/sdo-reports/<incident>/<sequence>-<command>.json. The response includes "
            "a readable path (a compact copy if needed) and a generated schema for querying its arrays. "
            "incident_verified is an extra "
            "informational warning; reads/reports keep their normal results, and new mutations are rejected."
        )
    for name, command in commands.choices.items():
        command.epilog = (command.epilog or "") + "\n\n" + response_fields_help(name)
        command.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    return parser


def verification_help() -> str:
    """Generate verification help from the same parser used for invocation."""
    parser = _parser()
    commands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    return commands.choices["verify"].format_help()


def command_surface() -> dict[str, object]:
    """Generate incident-facing signatures from the same parser that accepts commands."""
    parser = _parser()
    subcommands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    return {
        "commands": {
            name: " ".join(command.format_usage().removeprefix("usage: ").split())
            for name, command in subcommands.choices.items()
        },
        "mitigation": subcommands.choices["mitigate"].description,
        "output": "Full JSON: path retains complete evidence. Mutations print progress and the file schema; "
        "--json keeps mutations compact and prints other complete responses. Extract needed fields.",
    }


def _request(args: argparse.Namespace) -> dict[str, object]:
    request: dict[str, object] = {"command": args.command}
    if getattr(args, "finding", None):
        request["finding_fingerprints"] = args.finding
    if args.command == "verify" and args.wait:
        if not 0 <= args.wait <= 86400:
            raise ValueError("wait seconds must be between 0 and 86400")
        request["wait_seconds"] = args.wait
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
        groups = getattr(args, "option_groups", None)
        selection_set = getattr(args, "selection_set", None)
        if groups or selection_set:
            if (
                args.selection
                or args.option
                or (groups and selection_set)
                or args.param
                or getattr(args, "named_parameters", None)
                or args.finding
                or args.rounds not in (None, 1)
            ):
                raise ValueError("choose repeated options or a selection set; batch inputs belong to each option")
            request["selection_set"] = (
                _selection_set(selection_set) if selection_set else {"version": 1, "options": groups}
            )
            return request
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
        raise ValueError(str(response.get("error") or "current mitigation menu unavailable"))
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
            raise ValueError(str(response.get("error") or "bound mitigation menu unavailable"))
        options = [option for option in menu.get("options", []) if args.option == option.get("program_id")]
    if len(options) == 1 and options[0].get("disabled_reason"):
        raise ValueError(str(options[0]["disabled_reason"]))
    if len(options) != 1:
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


def _selection_set(path: Path) -> dict[str, object]:
    from google.protobuf.json_format import ParseError

    from sdo.agents.responder.selection_inputs import normalize_selection_set, validate_message_shape
    from sdo.contracts import MitigationSelectionSet, parse_json, to_canonical_json, validate

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError("selection_set: expected readable JSON object") from exc
    value = normalize_selection_set(value)
    validate_message_shape(value, MitigationSelectionSet(), "selection_set")
    try:
        selection = parse_json(json.dumps(value), MitigationSelectionSet())
    except (ValueError, ParseError) as exc:
        raise ValueError("selection_set: expected declared field shapes; see sdo menu schemas") from exc
    try:
        validate(selection)
    except ValueError as exc:
        raise ValueError(f"selection_set: {exc}") from exc
    return json.loads(to_canonical_json(selection))


def _selection(path: Path) -> dict[str, object]:
    # Keep help and all existing tools independent of protobuf startup costs.
    from google.protobuf.json_format import ParseError

    from sdo.agents.responder.selection_inputs import validate_message_shape
    from sdo.contracts import MitigationSelection, parse_json, to_canonical_json, validate

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError("selection: expected readable JSON object") from exc
    try:
        validate_message_shape(value, MitigationSelection(), "selection")
    except ValueError as exc:
        raise ValueError(f"invalid mitigation selection: {exc}") from exc
    try:
        selection = parse_json(json.dumps(value), MitigationSelection())
    except (ValueError, ParseError) as exc:
        raise ValueError(
            "invalid mitigation selection: expected declared field shapes; see sdo menu selection_schema"
        ) from exc
    try:
        validate(selection)
    except ValueError as exc:
        raise ValueError(f"invalid mitigation selection: {exc}") from exc
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
    deadline = time.monotonic() + max(180, float(str(request.get("wait_seconds", 0))) + 10)
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


class _PendingMutationConflict(ValueError):
    pass


def _act(args: argparse.Namespace) -> int:
    command = args.act_command[1:] if args.act_command[:1] == ["--"] else args.act_command
    try:
        if not command:
            raise ValueError("a command is required after --")
        response: dict[str, Any] = {}
        # Capture is evidence collection, not mutation authority. Keep repairs and
        # their receipts available even when the controller cannot capture.
        try:
            response = _exchange(
                {"command": "pre-apply", "refs": args.resource},
                Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")),
            )
            report = response.get("report")
            if isinstance(report, dict) and report.get("reason_code") == "pending_write_conflict":
                raise _PendingMutationConflict(str(response.get("error", "mutation overlaps a pending application")))
            if response.get("error"):
                raise ValueError(str(response["error"]))
        except _PendingMutationConflict:
            raise
        except (OSError, ValueError, TimeoutError) as exc:
            print(f"sdo: pre-apply evidence gap: {exc}; proceeding with repair", file=sys.stderr)

        before = response.get("findings", [])

        def completed(code: int, output: str, receipt: dict[str, object]) -> None:
            response: dict[str, object] = {
                "command": "act",
                "summary": receipt["summary"],
                "success": receipt["success"],
                "failure_reason": receipt.get("failure_reason", ""),
                "exit_code": code,
                "receipt": receipt,
                "output": output,
            }
            if os.environ.get("SDO_RESPONDER_SESSION") == "1":
                verified = _exchange(
                    {"command": "verify", "previous_findings": before},
                    Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")),
                )
                response["before_findings"] = before
                response["controller_findings"] = verified.get("findings", [])
                response["healthy"] = verified.get("healthy", False)
                if verified.get("progress") is not None:
                    response["progress"] = verified["progress"]
                if verified.get("incident_verified"):
                    response["incident_verified"] = verified["incident_verified"]
            _print_response(response, full_json=args.json)

        result = recorded_actions.run_and_record(
            command, args.resource, summary=args.summary, reversible=not args.irreversible, on_completed=completed
        )
        return result
    except (OSError, ValueError, TimeoutError) as exc:
        _print_client_error(args, exc)
        return 2


def _print_response(response: dict[str, Any], *, full_json: bool) -> None:
    history = os.environ.get("SDO_API_HISTORY_DIR")
    if history and "incident_verified" not in response:
        marker = Path(history) / "incident-verified.json"
        if marker.exists():
            response["incident_verified"] = json.loads(marker.read_text())
    if response.get("command") in MUTATION_COMMANDS:
        before = response.get("before_findings")
        after = response.get("controller_findings")
        from sdo.agents.responder.multi_output import format_selection_response

        text, path = format_selection_response(
            before, after, response, json_output=full_json, controller_progress=response.get("progress")
        )
        if full_json:
            # JSON callers receive the same bounded summary and discoverable schema.
            print(
                json.dumps(compact_mutation_envelope(text, path, response), ensure_ascii=False, separators=(",", ":"))
            )
        else:
            print(text)
        return
    output = write_response(response)
    print(json.dumps(response, ensure_ascii=False, sort_keys=True) if full_json else render_summary(output))


def _print_client_error(args: argparse.Namespace, error: Exception) -> None:
    print(f"sdo: {error}", file=sys.stderr)
    try:
        _print_response({"command": args.command, "error": str(error), "summary": str(error)}, full_json=args.json)
    except (OSError, ValueError) as persistence_error:
        print(f"sdo: cannot save full error output: {persistence_error}", file=sys.stderr)


def _check(args: argparse.Namespace, directory: Path) -> dict[str, object]:
    request: dict[str, object] = {"command": "detect"}
    if args.detector:
        request["detector_ids"] = args.detector
    detection = _exchange(request, directory)
    verification = _exchange({"command": "verify"}, directory)
    response = dict(verification)
    response.update(command="check", detection=detection)
    if detection.get("error") or verification.get("error"):
        response["error"] = "; ".join(str(item) for item in (detection.get("error"), verification.get("error")) if item)
        response["healthy"] = False
    return response


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
        directory = Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
        response = _check(args, directory) if args.command == "check" else _exchange(_request(args), directory)
        response.setdefault("command", args.command)
        _print_response(response, full_json=args.json)
    except (OSError, ValueError, TimeoutError) as exc:
        _print_client_error(args, exc)
        return 1
    return 1 if response.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
