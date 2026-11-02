<<<<<<< HEAD
"""Forward bounded incident verification through the incident tool transport."""
=======
"""Check incident closure through the controller's incident-scoped transport."""
>>>>>>> origin/vic-lsh/dev/sdo-improve

import sys

<<<<<<< HEAD
from sdo.agents.responder.tools_cli import main as tools_main


def main(argv: list[str] | None = None) -> int:
    return tools_main(["verify", *(sys.argv[1:] if argv is None else argv)])
=======
import argparse
import json
import os
from pathlib import Path

from sdo.agents.responder import tools_cli


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check live incident closure using the controller's verification")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        response = tools_cli._exchange(
            {"command": "verify"}, Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
        )
    except (OSError, ValueError, TimeoutError) as exc:
        response = {"healthy": False, "error": str(exc), "summary": str(exc)}
    state = "unavailable" if response.get("error") else "healthy" if response.get("healthy") else "unhealthy"
    response["state"] = state
    print(json.dumps(response, sort_keys=True) if args.json else response.get("summary", state))
    return {"healthy": 0, "unhealthy": 1, "unavailable": 3}[state]
>>>>>>> origin/vic-lsh/dev/sdo-improve


if __name__ == "__main__":
    raise SystemExit(main())
