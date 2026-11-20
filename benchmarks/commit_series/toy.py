"""A three-commit series for the toy app, for fast tests and smoke checks: benign, benign, then one that breaks.

The commits are the toy's scripted patches (``apps/toy/commits``) plus one more benign change, built into a local Git
repository at run time. The series is synthetic on purpose: it shows the three outcomes (accepted, accepted, held or
incident) in a minute, not how SDO fares on a real project's history (see ``gitlab.py`` for that).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from benchmarks.sregym.fastloop.toy_upstream import COMMITS_DIR

TOY = Path(__file__).resolve().parents[2] / "apps" / "toy"
_IDENT = ("-c", "user.name=toy-author", "-c", "user.email=toy-author@invalid")
#: (commit message, how main.go changes)
STEPS = (
    ("toy: service name answers with a version", "benign.patch"),
    ("toy: journey answers with its version", ("journey ok", "journey ok v2")),
    ("toy: backend readiness answers 503", "broken.patch"),
)


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", *_IDENT, "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def build(dest: Path) -> Path:
    """Create ``dest`` with the toy source at the base commit and one commit per step; write ``dest/series.json``.

    Returns the spec path. ``upstream.repo`` is ``dest/upstream`` (the history the replayer fetches commits from); the
    base commit's tree is what the lane's SDO repository must hold when the series starts.
    """
    repo = dest / "upstream"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "trunk")
    (repo / "main.go").write_text((TOY / "main.go").read_text())
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "toy: base")
    base = _git(repo, "rev-parse", "HEAD")
    commits = []
    for message, change in STEPS:
        if isinstance(change, str):
            _git(repo, "apply", str(COMMITS_DIR / change))
        else:
            source = (repo / "main.go").read_text()
            if change[0] not in source:
                raise ValueError(f"toy main.go has no {change[0]!r} to change")
            (repo / "main.go").write_text(source.replace(*change, 1))
        _git(repo, "commit", "-q", "-am", message)
        commits.append(_git(repo, "rev-parse", "HEAD"))
    spec = {
        "name": "toy-series",
        "description": "Three commits to the toy app: two benign changes, then one that fails the backend readiness.",
        "source": "apps/toy/commits (scripted patches) plus one synthetic benign change; synthetic by design.",
        "upstream": {"repo": str(repo), "base": base, "commits": commits},
        "mode": "diff",
        "on_conflict": "stop",
    }
    path = dest / "series.json"
    path.write_text(json.dumps(spec, indent=1) + "\n")
    return path
