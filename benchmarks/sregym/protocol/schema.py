"""Shared conductor-API constants.

Single source of truth for endpoint paths, env var names, and stage-set
memberships that every SREGym agent agrees on.  The upstream contract
lives in ``third_party/sregym/sregym/conductor/constants.py``; this module
mirrors the subset the agents depend on so we don't import the submodule.
"""

from __future__ import annotations

from typing import Final

from sdo.contracts import CONDUCTOR_TERMINAL_STAGES, SUBMISSION_PHASES

# --- Env vars ---------------------------------------------------------------

API_HOSTNAME_ENV: Final = "API_HOSTNAME"
API_PORT_ENV: Final = "API_PORT"
API_HOSTNAME_DEFAULT: Final = "localhost"
API_PORT_DEFAULT: Final = "8000"

# --- Endpoint paths ---------------------------------------------------------

STATUS_ENDPOINT: Final = "/status"
GET_APP_ENDPOINT: Final = "/get_app"
GET_PROBLEM_ENDPOINT: Final = "/get_problem"
STAGES_ENDPOINT: Final = "/stages"
CLEANUP_ENDPOINT: Final = "/cleanup"

# --- Stage sets -------------------------------------------------------------
# "Ready" stages are the ones at which an agent can submit an answer.
# "Terminal" stages signal the conductor is done with this problem and the
# agent should stop polling.

READY_STAGES: Final[frozenset[str]] = frozenset(SUBMISSION_PHASES)
TERMINAL_STAGES: Final[frozenset[str]] = frozenset(CONDUCTOR_TERMINAL_STAGES)

# --- Submission limits ------------------------------------------------------
# Mirrors the cap in third_party/sregym/sregym/conductor/constants.py — kept in
# sync by value rather than import so the lib doesn't depend on the
# benchmark submodule layout.

MAX_DIAGNOSIS_CANDIDATES: Final = 5
