#!/usr/bin/env python3
"""One inventory for local Go quality tests and prepared native CI execution."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class QualityCheck:
    name: str
    packages: tuple[str, ...]
    pattern: str
    bin: int
    timeout: str = "20s"

    def __post_init__(self) -> None:
        if not self.name or not self.packages or any(not package for package in self.packages):
            raise ValueError("quality check requires a name and packages")
        if type(self.bin) is not int or self.bin < 0:
            raise ValueError("quality check bin must be a nonnegative integer")
        if re.fullmatch(r"[1-9][0-9]*[smh]", self.timeout) is None:
            raise ValueError("quality check timeout must be a positive Go duration")
        re.compile(self.pattern)


QUALITY_CHECKS = (
    QualityCheck(
        "schema",
        ("sdo.dev/operator/builder", "sdo.dev/operator/operatorcli"),
        "^(TestSchemaIdentity.*|TestSDKBranchesMergeWithoutIdentityPins|"
        "TestControllerSchemaIdentityFailsClosed|TestParseValidateLaunchRequiresBoundSchema)$",
        0,
    ),
    QualityCheck(
        "git",
        ("sdo.dev/operator/internal/testutil",),
        "^TestGit(CreationPolicyNegativeControls|FixtureCreationUsesHelper)$",
        1,
    ),
    QualityCheck(
        "deadlines",
        ("sdo.dev/operator/internal/testutil",),
        "^Test(ExecuteDeadlinePolicyNegativeControls|NoFixedShortDeadlineAroundExecuteInTests|"
        "CountedPollingPolicyNegativeControls|CountedPollingAllowances|NoShortCountedPollingInTests)$",
        1,
    ),
    QualityCheck("capture", ("sdo.dev/operator/internal/capturefixture",), ".", 0),
    QualityCheck("layout", ("sdo.dev/operator/memory/validate",), "^TestMemoryLayoutReference$", 1),
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", required=True, choices=[check.name for check in QUALITY_CHECKS])
    parser.add_argument("--defer-prepared", action="store_true")
    args = parser.parse_args()
    check = next(check for check in QUALITY_CHECKS if check.name == args.group)
    if args.defer_prepared:
        print(f"Mandatory {check.name} tests assigned to prepared native bin {check.bin}")
        return
    subprocess.run(
        ["go", "test", *check.packages, "-run", check.pattern, "-count=1", "-timeout=" + check.timeout],
        cwd=ROOT / "sdo/operator",
        check=True,
    )


if __name__ == "__main__":
    main()
