"""Compatibility entry point for benchmark fault description discovery."""

from benchmarks.sregym.faults.worker import (
    describe as describe,
)
from benchmarks.sregym.faults.worker import (
    describe_problem as describe_problem,
)
from benchmarks.sregym.faults.worker import (
    main as main,
)

if __name__ == "__main__":
    main()
