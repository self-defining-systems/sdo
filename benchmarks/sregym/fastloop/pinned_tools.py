"""Pinned-tools lanes: the controller and its workers run image code, never the development mount.

A measurement lane names a digest reference (``name@sha256:<64 hex>``) for its controller image, so
the run is bound to one immutable build.
"""

from __future__ import annotations

import re

_DIGEST_REFERENCE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


def immutable_image(reference: str) -> bool:
    """A ``name@sha256:<64 hex>`` reference."""

    return bool(_DIGEST_REFERENCE.fullmatch(reference))
