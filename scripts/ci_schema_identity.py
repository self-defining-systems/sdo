#!/usr/bin/env python3
"""Read the independently checked native SDK pin without a second CLI build.

PR CI's TestSchemaIdentityMatchesCommittedSDK compares this pin to the current
Go sources. The image smoke also calls native validate --expect-schema against
the actual built sources; neither check is replaced by this build-time input.
"""

from __future__ import annotations

import re
from pathlib import Path


def read_identity(root: Path) -> str:
    source = (root / "sdo/operator/operatorcli/controller.go").read_text()
    pins = re.findall(r'^const sdkSchemaIdentity = "(sdo\.dev/v1alpha1\+sdk\.[0-9a-f]{16})"$', source, re.MULTILINE)
    if len(pins) != 1:
        raise ValueError("expected exactly one native SDK schema pin in operatorcli/controller.go")
    return pins[0]


if __name__ == "__main__":
    print(read_identity(Path(__file__).resolve().parents[1]))
