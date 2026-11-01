"""Forward bounded incident verification through the incident tool transport."""

import sys

from sdo.agents.responder.tools_cli import main as tools_main


def main(argv: list[str] | None = None) -> int:
    return tools_main(["verify", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    raise SystemExit(main())
