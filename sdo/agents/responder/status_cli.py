"""Check incident closure through the controller's incident-scoped transport."""

import argparse
import json
import os
from pathlib import Path

from sdo.agents.responder import tools_cli


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check live incident closure using the controller's verification")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--workload")
    parser.add_argument("--scenario", action="append", default=[])
    args = parser.parse_args(argv)
    request: dict[str, object] = {"command": "verify"}
    if args.workload:
        request["workload"] = args.workload
    if args.scenario:
        request["scenarios"] = args.scenario
    try:
        response = tools_cli._exchange(request, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    except (OSError, ValueError, TimeoutError) as exc:
        response = {"healthy": False, "error": str(exc), "summary": str(exc)}
    state = "unavailable" if response.get("error") else "healthy" if response.get("healthy") else "unhealthy"
    response["state"] = state
    print(json.dumps(response, sort_keys=True) if args.json else response.get("summary", state))
    return {"healthy": 0, "unhealthy": 1, "unavailable": 3}[state]


if __name__ == "__main__":
    raise SystemExit(main())
