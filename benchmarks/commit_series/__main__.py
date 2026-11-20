"""``python -m benchmarks.commit_series <workload verb>`` (a lane's workload command) or ``... remote up|down``."""

from __future__ import annotations

import sys

from benchmarks.commit_series import adapter, remote

if __name__ == "__main__":
    args = sys.argv[1:]
    sys.exit(remote.main(args[1:]) if args[:1] == ["remote"] else adapter.main(args))
