"""The one table of per-skill byte caps for the skills read in full on every incident."""

from __future__ import annotations

SKILL_BYTE_CAPS = {"incident-response": 3328, "sdo-operating": 1536}
SKILL_BYTE_FLOOR = 1024
