"""Fail a recorded provider session over the production message/output budgets."""

import argparse
import json
from pathlib import Path

from sdo.agents.session_context import measure_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_file", type=Path)
    parser.add_argument("--role", required=True, choices=("responder", "reflection"))
    args = parser.parse_args()
    measured = measure_session(args.session_file)
    print(json.dumps(measured.as_dict(), sort_keys=True))
    measured.check(args.role)


if __name__ == "__main__":
    main()
