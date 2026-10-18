"""Command-line entry point for the responder's independent incident status check."""

from __future__ import annotations

import argparse

from sdo.agents.responder.incident_status import run_incident_status_cli


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check live incident health")
    parser.add_argument("--workload")
    parser.add_argument("--scenario", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    return run_incident_status_cli(workload=args.workload, scenarios=args.scenario, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
