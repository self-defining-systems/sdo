"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import uuid
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, NoReturn, TypedDict

from sdo.agents.context_output import budget_for, cap_text
from sdo.agents.responder.mutation_output import compact_mutation_envelope
from sdo.agents.responder.tool_output import render_summary, write_response

CLI_EXAMPLES = (
    "sdo source Deployment/app/api",
    "sdo diagnose targetport --bind service=Service/app/api",
    "sdo mitigate targetport --bind service=Service/app/api",
)
OBJECT_REFERENCE_HELP = (
    "Object references accept Kind/name, Kind/namespace/name, or JSON with kind, name and optional "
    "namespace and api_version; kubectl aliases are accepted."
)

MUTATION_COMMANDS = frozenset({"mitigate", "rollback"})
# Commands whose response carries the Go-rendered text (detect summary, CSV pages), printed as is.
TEXT_COMMANDS = frozenset({"detect", "changes", "timeline", "try", "map"})

# Key response paths are part of the CLI help contract, shared with the generated
# responder reference. Fields are conditional, not a promise of success/presence.
RESPONSE_FIELDS = {
    "status": "healthy, findings[], detector_states[], drift[], drift_events[], program_runs[]",
    "detect": "healthy, findings[], detector_states[]",
    "check": "healthy, findings[], detector_states[], detection (fresh detect response)",
    "changes": "text (CSV, oldest first; columns in its header row)",
    "timeline": "text (CSV, oldest first; columns in its header row)",
    "try": "text (user test list, or one test's latest recorded sample), healthy (the test passes now)",
    "map": "text (components and operations, one operation path, or a component's neighbours)",
    "diagnose": "program_runs[].{program_id, program_revision, verdict, can_mitigate, mitigate_command}, "
    "program_revisions",
    "menu": "menu.options[].{command, program_id, program_revision, disabled_reason, parameters}, "
    "unexplained_removal_blockers[]",
    "source": "source (accepted Kubernetes object)",
    "verify": "healthy, verification_status, findings[], new_since_dispatch[], detector_states[], "
    "verification_blockers[{detector_id,reason}], drift[], drift_events[], data_effects[]",
    "report": "report.{status, attempt_id, rounds}, session",
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


def _binding(value: str) -> tuple[str, str]:
    name, separator, target = value.partition("=")
    if not separator or not name or not target:
        raise argparse.ArgumentTypeError("binding must be NAME=VALUE (an object is NAME=Kind/NAMESPACE/NAME)")
    # Go owns object-reference parsing and API-version resolution.
    return name, target


class _CappedParser(argparse.ArgumentParser):
    """Help and usage errors share the same context boundary as command results."""

    def _limit(self) -> int:
        command = self.prog.split()[1:]
        return budget_for("tool_stdout." + command[0]) if command else 8192

    def print_help(self, file=None) -> None:
        self._print_message(cap_text(self.format_help(), self._limit() - 1) + "\n", file or sys.stdout)

    def error(self, message: str) -> NoReturn:
        text = self.format_usage() + f"{self.prog}: error: {message}\n"
        self.exit(2, cap_text(text, self._limit() - 1) + "\n")


def _parser() -> argparse.ArgumentParser:
    parser = _CappedParser(
        prog="sdo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples (incident resources):\n  " + "\n  ".join(CLI_EXAMPLES),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    parser.add_argument(
        "--json",
        action="store_true",
        help="print JSON for scripts (mutations: compact envelope and complete report path)",
    )
    commands.add_parser(
        "status",
        help="read findings and detector confirmation",
        description=(
            "Read fresh findings and detector confirmation without running programs or planning repairs. "
            "Use diagnose for program verdicts and plan or menu to prepare repairs. "
            "prior_stages lists what automatic stages already did."
        ),
    )
    for name, help_text in (
        ("detect", "run fresh detectors; pending health is unproven"),
        (
            "check",
            "run detectors and verify closure; --detector ID selects detectors",
        ),
    ):
        check = commands.add_parser(
            name,
            help=help_text,
            description=help_text
            + ". External incidents have no detector gate; return your completed result after live checks.",
        )
        check.add_argument(
            "--detector",
            action="extend",
            nargs="+",
            default=[],
            metavar="ID",
            help="detector IDs to run; repeat for more IDs (default: all)",
        )
        if name == "detect":
            check.add_argument(
                "--finding",
                metavar="ID",
                help="print one item of the summary in full: a test ID, component, clue ID, tests or background",
            )
    changes = commands.add_parser(
        "changes",
        help="list recent changes (CSV) from the incident onset window",
        description=(
            "Print the changes (CSV, oldest first) that SDO observed from --since before incident onset until now: "
            "application state, configuration and Kubernetes writes with before/after values and the writer "
            "when known. "
            "Computed from the controller's state each time you call it."
        ),
    )
    changes.add_argument(
        "--since",
        metavar="DURATION",
        help="how far before onset to reach, a Go duration such as 30m (default: 10m)",
    )
    changes.add_argument("--limit", type=int, metavar="N", help="keep only the newest N changes")
    timeline = commands.add_parser(
        "timeline",
        help="show signals over time (CSV) around the incident onset",
        description=(
            "Print failing operations and workload readiness, desired replicas and restarts over time (CSV, "
            "oldest first), from --since before incident onset until now. Computed when you call it."
        ),
    )
    timeline.add_argument(
        "--since",
        metavar="DURATION",
        help="how far before onset to reach, a Go duration such as 30m (default: 10m)",
    )
    try_command = commands.add_parser(
        "try",
        help="show a user test's status and latest recorded sample (read-only)",
        description=(
            "With no TEST, list the user tests (probe scenarios) with their current status. With TEST, print its "
            "latest, last failing and last passing recorded sample: request line, status, response and error, "
            "and whether it passes now. Reads what the controller's prober already recorded; sends no traffic and "
            "changes nothing. Probe credentials are never shown."
        ),
    )
    try_command.add_argument("test", nargs="?", metavar="TEST", help="user test name; omit to list them")
    map_command = commands.add_parser(
        "map",
        help="answer application-map questions: components, operation paths, neighbours (read-only)",
        description=(
            "Read the accepted application map. With no view, list components (workload, services, whether it "
            "holds data), operations and the edge count. `path OPERATION` shows the operation's path through "
            "components and edges in order, data holders marked, the probes covering it and their status. "
            "`near COMPONENT` shows what it depends on and what depends on it within two hops, and the "
            "operations through it. Possible edges (a Secret reference only suggests them) are labelled. "
            "Changes nothing."
        ),
    )
    map_command.add_argument(
        "view",
        nargs="?",
        choices=("path", "near"),
        help="path OPERATION or near COMPONENT; omit to list the map",
    )
    map_command.add_argument("name", nargs="?", metavar="NAME", help="operation (path) or component (near)")
    for name in ("diagnose", "menu"):
        command = commands.add_parser(
            name,
            help={
                "diagnose": "evaluate a playbook; a match names its fix",
                "menu": "list fixes as commands",
            }[name],
            description={
                "diagnose": "Evaluate a playbook against live state. A match with a fix prints the command to run. "
                "A wrong or missing role is an error that says what the playbook takes.",
                "menu": "List the fixes you can apply as commands; NAME narrows to one playbook.",
            }[name],
            epilog="Example: " + CLI_EXAMPLES[1],
        )
        command.add_argument("program_id", nargs="?", metavar="NAME")
        command.add_argument(
            "--bind",
            action="append",
            type=_binding,
            default=[],
            metavar="NAME=VALUE",
            help="a playbook parameter: an object is NAME=Kind/NAMESPACE/NAME; repeat for each",
        )
    mitigate = commands.add_parser(
        "mitigate",
        help="apply a fix by name, with the same --bind as diagnose",
        description=(
            "Apply a playbook's fix, or a built-in action such as DeletePod, by name. "
            "Take the name and --bind values from sdo diagnose, or omit --bind when sdo diagnose "
            "or coverage already matched the playbook once. "
            "To apply independent fixes together, separate the commands with --: "
            "sdo mitigate A --bind x=Kind/ns/a -- B --bind y=Kind/ns/b. "
            "Overlapping writes are refused." + " Programs use the current checkout; edits compile on demand."
        ),
        epilog="Example: " + CLI_EXAMPLES[2],
    )
    mitigate.add_argument("target", metavar="NAME", help="playbook or built-in action")
    mitigate.add_argument(
        "--bind",
        action="append",
        type=_binding,
        default=[],
        metavar="NAME=VALUE",
        help=(
            "a parameter: an object is NAME=Kind/NAMESPACE/NAME; structured values are JSON text; "
            "repeat a list parameter's NAME once per element, or give it a JSON array"
        ),
    )
    mitigate.add_argument(
        "--finding",
        action="append",
        default=[],
        metavar="FINGERPRINT",
        help="verify against this finding only (default: the findings the playbook explains)",
    )
    mitigate.add_argument("--rounds", type=int, help="maximum rounds within the remaining incident allowance")
    report = commands.add_parser("report", help="retrieve a settled mitigation report")
    report.add_argument("attempt_id")
    report.add_argument("--incident-id", required=True)
    report.add_argument("--session-id", required=True)
    report.add_argument("--epoch", type=int, required=True)
    commands.add_parser(
        "source", help="inspect accepted source for one resource", epilog="Example: " + CLI_EXAMPLES[0]
    ).add_argument("ref", metavar="Kind/namespace/name")
    verify = commands.add_parser(
        "verify",
        help="read cached controller verification; optionally await a stable verdict",
        description="External incidents have no detector gate: return your completed result after live checks. "
        "Otherwise reads the controller's shared recovery proof: fresh health after the last repair, "
        "observed write effects, and the unchanged 15-second stability window. drift[] lists objects that still "
        "differ from accepted source and never decides healthy. "
        "verification_status=awaiting_responder_completion: fresh health is clear; "
        "SDO ends your work when Recovered holds. "
        "Pending observation windows, including unproven synthetic scenarios, are not healthy. "
        "Settling traffic SLOs wait. "
        "verification_blockers names incomplete detectors and reasons. "
        "Fresh deployment-rollout-pending findings name owned ReplicaSet/Pod blockers and old-template Pods, "
        "including after recorded actions; inspect their evidence before repeating workload writes. "
        "Wait on SDO's verification instead of sleeping. After closure, incident_verified is an "
        "informational warning on every response; reads and reports remain available, while mutations are rejected. "
        "Verified recovery fences writes and publishes the outcome without waiting for final narration; "
        "worker quiescence and outcome/reflection work continue during maintenance pauses.",
    )
    verify.add_argument(
        "--workload", help="traffic workload to burst (default: the detection designer's verify workload)"
    )
    verify.add_argument("--scenario", action="append", default=[], help="traffic scenario ID to burst; repeatable")
    verify.add_argument(
        "--wait",
        type=float,
        default=0,
        metavar="SECONDS",
        help="wait for stable verification, a new/reactivated finding, or this timeout "
        "(0: cached and instant; also instant while you have made no write)",
    )
    commands.add_parser("rollback", help="roll back a plan application").add_argument("application_id")
    for name in ("mitigate", "rollback"):
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
        if name in {"source", "diagnose", "menu", "mitigate"}:
            command.epilog = (command.epilog or "") + "\n\n" + OBJECT_REFERENCE_HELP
        command.epilog = (command.epilog or "") + "\n\n" + response_fields_help(name)
        command.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    return parser


def verification_help() -> str:
    """Generate verification help from the same parser used for invocation."""
    parser = _parser()
    commands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    return commands.choices["verify"].format_help()


class CommandSurface(TypedDict):
    commands: dict[str, str]
    reference: str
    output: str


def command_purposes() -> dict[str, str]:
    """Each registered command's short listing description, in parser order."""
    parser = _parser()
    subcommands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    purposes: dict[str, str] = {}
    for action in subcommands._choices_actions:
        if not isinstance(action.help, str) or not action.help.strip():
            raise ValueError(f"{action.dest}: registered commands require a short listing description")
        purposes[action.dest] = action.help
    return {name: purposes[name] for name in subcommands.choices}


def command_surface() -> CommandSurface:
    """Generate incident-facing signatures from the same parser that accepts commands."""
    descriptions = {
        name: f"sdo {name}: {purpose}; read sdo {name} --help for arguments."
        for name, purpose in command_purposes().items()
    }
    for name, description in descriptions.items():
        if len(description) > budget_for("session_context.tool_description"):
            raise ValueError(f"{name}: listing description exceeds its budget")
    return {
        "commands": descriptions,
        "reference": "sdo-operating/SKILL.md",
        "output": (
            "Full JSON: read details_file; --json uses the same caps. "
            "Menu: menu.options[].{option_id,parameters[].{name,schema}}. "
            "Mutations: summary,details_file,error?,failure_reason?; "
            "report arrays: actions,findings,fixed,new,unchanged,option_reports. "
            "Use published parameter names and nested schemas; namespaces belong in object references."
        ),
    }


def _named_parameters(binds: list[tuple[str, str]]) -> dict[str, str]:
    parameters = dict(binds)
    if len(parameters) != len(binds):
        raise ValueError("duplicate binding name: playbook parameters take one value each")
    return parameters


def _target_parameters(binds: list[tuple[str, str]]) -> dict[str, str | list[str]]:
    """A repeated name becomes a list; Go accepts it only for a list-typed parameter."""
    grouped: dict[str, list[str]] = {}
    for name, value in binds:
        grouped.setdefault(name, []).append(value)
    return {name: values[0] if len(values) == 1 else values for name, values in grouped.items()}


def _target(args: argparse.Namespace) -> dict[str, object]:
    """One playbook or action by name; Go resolves it to the exact selection."""
    target: dict[str, object] = {"name": args.target}
    if args.bind:
        target["parameters"] = _target_parameters(args.bind)
    if args.finding:
        target["finding_fingerprints"] = args.finding
    return target


def _request(args: argparse.Namespace) -> dict[str, object]:
    request: dict[str, object] = {"command": args.command}
    if args.command == "verify" and args.wait:
        if not 0 <= args.wait <= 86400:
            raise ValueError("wait seconds must be between 0 and 86400")
        request["wait_seconds"] = args.wait
    if args.command == "detect":
        if args.detector:
            request["detector_ids"] = args.detector
        if args.finding:
            request["finding"] = args.finding
    elif args.command == "try":
        if args.test:
            request["test"] = args.test
    elif args.command == "map":
        if args.view in ("path", "near") and not args.name:
            raise ValueError(f"sdo map {args.view} needs a name")
        if args.view:
            request["map_view"] = args.view
        if args.name:
            request["name"] = args.name
    elif args.command in ("changes", "timeline"):
        if args.since:
            request["since"] = args.since
        if args.command == "changes" and args.limit is not None:
            if args.limit < 1:
                raise ValueError("--limit must be at least 1")
            request["limit"] = args.limit
    elif args.command in ("diagnose", "menu"):
        if args.program_id:
            request["program_id"] = args.program_id
        if args.bind:
            # diagnose binds the way mitigate does, so a wrong role is an error that names what the playbook takes.
            request["parameters" if args.command == "diagnose" else "bindings"] = _named_parameters(args.bind)
    elif args.command == "verify":
        if args.workload:
            request["workload"] = args.workload
        if args.scenario:
            request["scenarios"] = args.scenario
    elif args.command == "mitigate":
        targets = [_target(args), *(_target(more) for more in getattr(args, "more_targets", []))]
        request["targets"] = targets
        if args.rounds is not None:
            if args.rounds < 1:
                raise ValueError("--rounds must be at least 1")
            request["rounds"] = args.rounds
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


def verify_incident() -> dict[str, object]:
    """Run bounded verification through the controller's incident session."""
    return _exchange({"command": "verify"}, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))


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
    if response.get("command") in TEXT_COMMANDS and response.get("text") and not full_json:
        print(cap_text(str(response["text"]), budget_for("tool_stdout." + str(response["command"])) - 1))
        return
    text = json.dumps(response, ensure_ascii=False, sort_keys=True) if full_json else render_summary(output)
    limit = budget_for("tool_stdout." + str(response["command"]))
    print(cap_text(text, limit - 1, json_output=full_json))


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


def _split_fixes(arguments: list[str]) -> tuple[list[str], list[list[str]]]:
    """Fixes applied together are separated by --; only mitigate reads it as a separator."""
    if "mitigate" not in arguments:
        return arguments, []
    start = arguments.index("mitigate") + 1
    chunks: list[list[str]] = [[]]
    for token in arguments[start:]:
        if token == "--":
            chunks.append([])
        else:
            chunks[-1].append(token)
    return [*arguments[:start], *chunks[0]], chunks[1:]


def _main(argv: list[str] | None = None) -> int:
    parser = _parser()
    first, more = _split_fixes(list(sys.argv[1:] if argv is None else argv))
    args = parser.parse_args(first)
    if more:
        args.more_targets = [parser.parse_args(["mitigate", *chunk]) for chunk in more]
        if any(item.rounds is not None for item in args.more_targets):
            parser.error("--rounds belongs to a single fix")
    try:
        directory = Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
        response = _check(args, directory) if args.command == "check" else _exchange(_request(args), directory)
        response.setdefault("command", args.command)
        _print_response(response, full_json=args.json)
    except (OSError, ValueError, TimeoutError) as exc:
        _print_client_error(args, exc)
        return 1
    return 1 if response.get("error") else 0


def main(argv: list[str] | None = None) -> int:
    """Cap all emitted channels, including help, errors and future command handlers."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    commands = RESPONSE_FIELDS.keys()
    command = next((value for value in arguments if value in commands), None)
    limit = budget_for("tool_stdout." + command) if command else 8192
    stdout, stderr = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            return _main(arguments)
    finally:
        out, err = stdout.getvalue(), stderr.getvalue()
        combined = out + err
        size = len(combined.encode()) if command in MUTATION_COMMANDS else len(combined)
        if size <= limit:
            sys.stdout.write(out)
            sys.stderr.write(err)
        else:
            text = cap_text(
                combined, limit - 1, json_output="--json" in arguments, bytes_limit=command in MUTATION_COMMANDS
            )
            print(text)


print_tool_response = _print_response


if __name__ == "__main__":
    raise SystemExit(main())
