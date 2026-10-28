"""Incident-scoped SDO tools for the stock Codex and Claude responders."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
import uuid
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, NoReturn, TypedDict

from sdo.agents.context_output import budget_for, cap_text
from sdo.agents.responder import recorded_actions
from sdo.agents.responder.mutation_output import compact_mutation_envelope
from sdo.agents.responder.tool_output import render_summary, write_response

PLAN_ACTIONS = (
    "SetField updates a schema-checked field by index or unique name, optionally from accepted source; "
    "ConfigMap JSON/YAML entries preserve unrelated keys.\n"
    "Object edits write atomically per object; use strategic patches for keyed lists.\n"
    "CreateFromSource restores a named missing object, including declared file-generated ConfigMaps, "
    "from accepted source. Source/config restores may recycle blocked dependent Pods once; "
    "Pod deletion receipts are irreversible.\n"
    "ReconcileFromSource converges source-owned configuration, removes additions since acceptance, "
    "and reports residual source differences and remaining old-revision ReplicaSets/Pods. "
    "The Deployment controller and responder handle the rollout.\n"
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
ACT_USAGE = "sdo act --resource Kind/namespace/name [--summary TEXT] [--irreversible] [--json] -- COMMAND ARGS"
# SDO's own act options that kubectl never defines, mapped to whether each takes a value.
# A kubectl command may carry them in any position before its own --; kubectl keeps every
# flag it defines (--resource included) and other programs receive their arguments verbatim.
SDO_ACT_FLAGS = {"--json": False, "--irreversible": False, "--summary": True}
_USAGE_ERROR = re.compile(r"unknown (?:shorthand )?flag|unknown command|for usage\.")

OBJECT_REFERENCE_HELP = (
    "Object references accept Kind/name, Kind/namespace/name, or JSON with kind, name and optional "
    "namespace and api_version; kubectl aliases are accepted."
)

MUTATION_COMMANDS = frozenset({"act", "apply", "mitigate", "rollback"})

# Key response paths are part of the CLI help contract, shared with the generated
# responder reference. Fields are conditional, not a promise of success/presence.
RESPONSE_FIELDS = {
    "status": "healthy, findings[], detector_states[], drift[], drift_events[], program_runs[]",
    "detect": "healthy, findings[], detector_states[]",
    "check": "healthy, findings[], detector_states[], detection (fresh detect response)",
    "diagnose": "program_runs[].{program_id, program_revision, verdict, can_mitigate}, program_revisions",
    "plan": "plan_id, patches[], program_runs[], program_revisions",
    "menu": "menu.options[].{option_id, program_id, program_revision, disabled_reason, parameters}, "
    "selection_schema, selection_set_schema, unexplained_removal_blockers[]",
    "source": "source (accepted Kubernetes object)",
    "verify": "healthy, unresolved, findings[], detector_states[], "
    "verification_blockers[{detector_id,reason}], drift[], drift_events[]",
    "report": "report.{status, attempt_id, rounds}, session",
    "act": "actions[].{success, exit_code, receipt, output, failure_reason, hint}",
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
        epilog="Output: session schema; correct feedback once. "
        "prior_stages: per-repair receipts; prior_response_progress: bounded failed-episode progress with omissions. "
        "Keep repairs; admission_blocked names a rollout prerequisite.\n\n"
        "Examples (replace names with incident resources):\n  " + "\n  ".join(CLI_EXAMPLES),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    parser.add_argument(
        "--json",
        action="store_true",
        help="print JSON for scripts (mutations: compact envelope and complete report path)",
    )
    commands.add_parser("status", help="read findings and detector confirmation")
    for name, help_text in (
        ("detect", "run fresh detectors; pending health is unproven"),
        ("check", "run detectors and verify closure; --detector ID selects detectors"),
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
        help="apply independent options; --finding FINGERPRINT scopes verification; overlapping writes refused",
        description=(
            "Apply independent options; overlapping writes are refused. "
            "Repeat --option ID with --param NAME=VALUE, or use --selection-set FILE "
            "with version 1 and options [{option_id, bindings, parameters, finding_fingerprints}]. "
            "--match ID selects a playbook_coverage match; --bind narrows it. "
            "Each option has a receipt; one call uses one of five rounds. --NAME VALUE also sets parameters. "
            "Pending rollout retains its application ID and allows reads; "
            "rollout_stalled releases writes at its bound. "
            "Source reconciliation waits for old objects to drain; persistent differences return source_residual. "
            "Script fixes are learnable; unlinked bytes are refused as script_unprovenanced."
            + " "
            + WORKING_PROGRAM_HELP
        ),
        epilog=(
            "Advertised option IDs remain valid for this session across filtered menu calls. "
            "Learned program preconditions are rechecked against live state before any batch writes.\n\n"
            "Parameters accept ordinary JSON objects, arrays, numbers and booleans, as well as strings. "
            "sdo menu exports recursive parameters[].schema for every option, plus selection_schema and "
            "selection_set_schema; nested properties, required fields and oneOf choices describe accepted shapes. "
            "parameters[].example supplies typed synthetic inputs; replace names/values using incident evidence. "
            "CLI --param NAME=VALUE accepts JSON text for structured values. "
            "SyncWebhookCA privately copies a named Secret key into one webhook caBundle; "
            "only its reference and digest are returned. "
            "EnsureDatabaseUser repairs a MongoDB user in one call. Its password and target.auth "
            "username/password take exactly one credential reference: {env: NAME} or "
            "{config_map: {name: CONFIGMAP, key: KEY, assignment: optional_NAME}} in the target Pod namespace. "
            "Assignment selects one literal NAME=value line. The executor resolves values privately; "
            "never put credential values in arguments, evidence or memory. Direct Secret keys are unavailable. "
            "The result reads the catalog back: action_results[].database_user states the verified user and "
            "outcome (created, updated or already_correct). Failures use execution_failed, authorization_denied "
            "or outcome_unknown; details retain the safe database diagnostic. "
            "A verified user does not mean the application recovered."
        ),
    )
    mitigate.add_argument("option", nargs="?", help="advertised option ID or unambiguous program ID")
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
    mitigate.add_argument(
        "--match",
        metavar="ID",
        help="playbook ID of a ready playbook_coverage match",
    )
    mitigate.add_argument(
        "--bind",
        action="append",
        type=_binding,
        default=[],
        metavar="NAME=REF",
        help="with --match: only the match binding NAME to REF (Kind/namespace/name); repeat",
    )
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
        help="record a command with --resource Kind/namespace/name",
        description=(
            "Run one live mutation and record its repair receipt (target, resources, timing, outcome and "
            "command line). The command's exit code is returned. Do not hand-write receipts. "
            "Report one cause per independently fixable fault and record a separate act for each, declaring "
            "only that cause's resources, so each receipt is attributed to its cause; "
            "combine only faults that can't be fixed separately. "
            "When an admission control blocks recovery and its backend is gone, restore the backend if you can. "
            "Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. "
            "SDO options go before --; the command after it runs verbatim, except that a kubectl command "
            "may also carry SDO's --json, --summary and --irreversible, which kubectl never defines. "
            "A rejected command line returns a one-line hint with the correct form."
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
        description="Reads the controller's cached health verdict immediately; drift[] lists objects that still "
        "differ from accepted source and never decides healthy. "
        "After a healthy return, SDO may resume once for source differences when progress remains; "
        "terminal inventory and no-progress stops do not resume. "
        "restore them or explain the intended difference. "
        "Pending observation windows, including unproven synthetic scenarios, are not healthy. "
        "verification_blockers names incomplete detectors and reasons. SDO resumes once for these blockers, "
        "then stops any running blocker turn and records an unconfirmed outcome "
        "at the verification bound if they persist. "
        "Fresh deployment-rollout-pending findings name owned ReplicaSet/Pod blockers and old-template Pods, "
        "including after recorded actions; inspect their evidence before repeating workload writes. "
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
        if name in {"source", "act", "diagnose", "plan", "menu", "mitigate"}:
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


def command_surface() -> CommandSurface:
    """Generate incident-facing signatures from the same parser that accepts commands."""
    parser = _parser()
    subcommands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    purposes: dict[str, str] = {}
    for action in subcommands._choices_actions:
        if not isinstance(action.help, str) or not action.help.strip():
            raise ValueError(f"{action.dest}: registered commands require a short listing description")
        purposes[action.dest] = action.help
    descriptions = {
        name: f"sdo {name}: {purposes[name]}; read sdo {name} --help for arguments." for name in subcommands.choices
    }
    for name, description in descriptions.items():
        if len(description) > budget_for("session_context.tool_description"):
            raise ValueError(f"{name}: listing description exceeds its budget")
    return {
        "commands": descriptions,
        "reference": "sdo-incident-response/references/commands.md",
        "output": "Full JSON: read details_file; --json uses the same caps and complete evidence stays in files.",
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
        if args.match:
            request["selection_set"] = _match_selection_set(args, groups)
            return request
        if args.bind:
            raise ValueError("--bind belongs to --match")
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


def _match_selection_set(args: argparse.Namespace, groups: object) -> dict[str, object]:
    """Resolve a coverage match to the menu's ready options so no selection JSON is hand-built."""
    if (
        args.option
        or groups
        or args.selection
        or args.selection_set
        or args.param
        or getattr(args, "named_parameters", None)
        or args.finding
        or args.rounds not in (None, 1)
    ):
        raise ValueError("--match takes only PLAYBOOK_ID and optional --bind; it builds the selection itself")
    request: dict[str, object] = {"command": "menu", "program_id": args.match, "match": True}
    bindings = dict(args.bind)
    if len(bindings) != len(args.bind):
        raise ValueError("duplicate binding name")
    if bindings:
        request["bindings"] = bindings
    response = _exchange(request, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    menu = response.get("menu")
    if response.get("error") or not isinstance(menu, dict):
        raise ValueError(str(response.get("error") or "current mitigation menu unavailable"))
    options = [option for option in menu.get("options", []) if option.get("program_id") == args.match]
    ready = [option for option in options if not option.get("disabled_reason")]
    if not ready:
        reason = next((str(option["disabled_reason"]) for option in options), "")
        raise ValueError(reason or f"no ready option for playbook {args.match}; see sdo menu")
    return {"version": 1, "options": [{"option_id": option["option_id"]} for option in ready]}


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
    if not options:
        # Listings are views. Only the controller's session catalog can resolve
        # an earlier explicit binding, or authoritatively refuse an unknown ID.
        options = [{"option_id": args.option}]
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


def _act_command(args: argparse.Namespace) -> list[str]:
    """Return the wrapped command, moving SDO's own options out of a kubectl command into *args*."""
    command = args.act_command[1:] if args.act_command[:1] == ["--"] else list(args.act_command)
    if not command or Path(command[0]).name != "kubectl":
        return command
    kept = command[:1]
    tokens = iter(command[1:])
    for token in tokens:
        if token == "--":
            kept.extend(["--", *tokens])
            break
        name, has_value, value = token.partition("=")
        if name not in SDO_ACT_FLAGS or has_value and not SDO_ACT_FLAGS[name]:
            kept.append(token)
        elif not SDO_ACT_FLAGS[name]:
            setattr(args, name.removeprefix("--"), True)
        else:
            value = value if has_value else next(tokens, None)
            if value is None or args.summary not in (None, value):
                raise ValueError(f"{name} takes one value; usage: {ACT_USAGE}")
            args.summary = value
    return kept


def _usage_hint(command: list[str], output: str) -> str | None:
    """One line that says how to call act when the wrapped program rejected its arguments."""
    if not _USAGE_ERROR.search(output):
        return None
    program = Path(command[0]).name
    return (
        f"{program} rejected its arguments; SDO options go before --, the command after it runs verbatim: {ACT_USAGE}"
    )


def _act(args: argparse.Namespace) -> int:
    try:
        command = _act_command(args)
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
            hint = None if code == 0 else _usage_hint(command, output)
            if hint:
                response["hint"] = hint
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


def _main(argv: list[str] | None = None) -> int:
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
