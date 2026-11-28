"""Rebuild the files an onboarding agent wrote, and snapshot them at every validator call.

A past session is replayed from its start commit: ``Write`` and ``Edit`` calls are applied to a
fresh checkout, and every ``Bash`` call runs again in a network-less sandbox with the checkout at
the path the agent used (``/work`` by default). Agents edit probes mostly with shell scripts, so
re-running their commands is the only faithful way to rebuild the files. Commands that need the
cluster fail at once; ``sleep`` returns at once. ``sdo-operator`` calls run through the session's
own kit (some of them write files), and the checkout is committed to a snapshot just before each
validator call, so a later check sees exactly what that call judged.

Fidelity is measured, never assumed: every commit the agent made in the session (from its
exported bundle) is looked up by tree among the replay's own commits.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path  # noqa: TC003

from benchmarks.onboarding_gate.sandbox import Sandbox, go_binds, go_environment, host_path
from benchmarks.onboarding_gate.transcript import ToolCall, read_tool_calls

SHIM_OPERATOR = """#!/bin/bash
# Replay shim: snapshot the checkout before a validator call, then run the session's own kit.
# There is no cluster: a live journeys run becomes --plan (it writes what the live run wrote:
# memory check's normalization, nothing else), and onboarding check is skipped.
if [[ "$*" =~ ^(memory\\ (check|test)|onboarding\\ (journeys|check)) ]]; then
  index=/tmp/.replay-index.$$.$RANDOM
  GIT_INDEX_FILE=$index git -C "$REPLAY_WORK" add -A >/dev/null 2>&1
  tree=$(GIT_INDEX_FILE=$index git -C "$REPLAY_WORK" write-tree)
  rm -f "$index"
  commit=$(git -C "$REPLAY_WORK" commit-tree "$tree" -p HEAD -m "replay snapshot $REPLAY_CALL")
  git -C "$REPLAY_WORK" update-ref "refs/replay/$REPLAY_CALL-$RANDOM$RANDOM" "$commit"
  printf '%s\\t%s\\t%s\\n' "$REPLAY_CALL" "$commit" "$*" >> /replay-log/calls.tsv
fi
if [[ "$1 $2" == "onboarding check" ]]; then echo 'replay: no cluster' >&2; exit 2; fi
if [[ "$1 $2" == "onboarding journeys" ]]; then
  args=(onboarding journeys --plan); shift 2
  while [[ $# -gt 0 ]]; do
    case $1 in
      --repo|--namespace|--operator-root|--seed) args+=("$1" "$2"); shift 2 ;;
      --repo=*|--namespace=*|--operator-root=*|--seed=*) args+=("$1"); shift ;;
      --only|--soak|--kubeconfig) shift 2 ;;
      *) shift ;;
    esac
  done
  exec timeout 150 /kit/bin/sdo-operator "${args[@]}"
fi
exec timeout 150 /kit/bin/sdo-operator "$@"
"""
SHIM_SLEEP = "#!/bin/sh\nexit 0\n"
SHIM_CLUSTER = "#!/bin/sh\necho 'replay: no cluster' >&2\nexit 1\n"


class ReplayError(RuntimeError):
    """A session cannot be replayed (missing start commit, kit or transcript)."""


@dataclass(frozen=True)
class Session:
    """What a replay needs from one recorded onboarding session."""

    name: str
    transcript: Path
    start_repo: Path  # a Git repository or bundle that contains the start commit
    base: str  # the start commit (the onboarding base the export must descend from)
    kit: Path  # the session's own kit: bin/sdo-operator and src/sdo/operator
    workdir: str = "/work"

    def __post_init__(self) -> None:
        if not self.transcript.is_file():
            raise ReplayError(f"{self.name}: transcript {self.transcript} is missing")
        if not (self.kit / "bin" / "sdo-operator").is_file():
            raise ReplayError(f"{self.name}: kit {self.kit} has no bin/sdo-operator")


@dataclass
class Snapshot:
    """The checkout as one validator call saw it."""

    call: int
    commit: str
    tree: str
    args: str


@dataclass
class Replayed:
    session: str
    repo: str
    base: str
    snapshots: list[Snapshot] = field(default_factory=list)
    edit_misses: list[int] = field(default_factory=list)
    agent_commits: int = 0
    matched_commits: int = 0

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=1) + "\n")

    @classmethod
    def load(cls, path: Path) -> Replayed:
        data = json.loads(path.read_text())
        data["snapshots"] = [Snapshot(**item) for item in data["snapshots"]]
        return cls(**data)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def _host_path(session: Session, work: Path, path: str) -> Path | None:
    prefix = session.workdir.rstrip("/") + "/"
    if not path.startswith(prefix):
        return None
    return work / path[len(prefix) :]


def apply_file_call(session: Session, work: Path, call: ToolCall) -> bool:
    """Apply a Write, Edit or MultiEdit to the checkout; False when an edit no longer applies."""
    target = _host_path(session, work, call.file_path)
    if target is None or call.is_error:
        return True
    if call.name == "Write":
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(call.input.get("content", "")))
        return True
    edits = call.input.get("edits") if call.name == "MultiEdit" else [call.input]
    if not target.is_file():
        return False
    text = target.read_text()
    for edit in edits or []:
        old, new = str(edit.get("old_string", "")), str(edit.get("new_string", ""))
        if old not in text:
            return False
        text = text.replace(old, new) if edit.get("replace_all") else text.replace(old, new, 1)
    target.write_text(text)
    return True


def _prepare(session: Session, root: Path) -> tuple[Path, Sandbox]:
    work, tmp, home, shim, log = (root / name for name in ("work", "tmp", "home", "shim", "log"))
    if root.exists():
        subprocess.run(["rm", "-rf", str(root)], check=True)
    for directory in (tmp, home, shim, log):
        directory.mkdir(parents=True)
    subprocess.run(["git", "clone", "-q", "--no-checkout", str(session.start_repo), str(work)], check=True)
    git(work, "checkout", "-q", "--detach", session.base)
    # The recorded head (a bundle may carry only HEAD) is what fidelity compares against.
    git(work, "fetch", "-q", str(session.start_repo), "+HEAD:refs/recorded/head")
    git(work, "config", "user.name", "replay")
    git(work, "config", "user.email", "replay@invalid")
    for name, body in {"sdo-operator": SHIM_OPERATOR, "sleep": SHIM_SLEEP, "kubectl": SHIM_CLUSTER}.items():
        (shim / name).write_text(body)
        (shim / name).chmod(0o755)
    env = go_environment()
    env |= {
        "PATH": f"/replay-shim:/kit/bin:{host_path()}",
        "HOME": "/replay-home",
        "TMPDIR": "/tmp",
        "SDO_ACTOR": "health_judge",
        "SDO_PHASE": "initial",
        "SDO_BASE_COMMIT": session.base,
        "SDO_OPERATOR_ROOT": "/kit/src/sdo/operator",
        "REPLAY_WORK": session.workdir,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_AUTHOR_NAME": "replay",
        "GIT_AUTHOR_EMAIL": "replay@invalid",
        "GIT_COMMITTER_NAME": "replay",
        "GIT_COMMITTER_EMAIL": "replay@invalid",
    }
    binds = {work: session.workdir, tmp: "/tmp", home: "/replay-home", log: "/replay-log", **go_binds(env)}
    sandbox = Sandbox(
        binds=binds, env=env, read_only={shim: "/replay-shim", session.kit: "/kit"}, chdir=session.workdir
    )
    return work, sandbox


def replay(session: Session, root: Path, bash_timeout: float = 200.0) -> Replayed:
    """Replay one session into root (replacing it) and return its validator snapshots."""
    calls = read_tool_calls(session.transcript)
    work, sandbox = _prepare(session, root)
    result = Replayed(session.name, str(work), session.base)
    for call in calls:
        if call.name in ("Write", "Edit", "MultiEdit"):
            if not apply_file_call(session, work, call):
                result.edit_misses.append(call.index)
        elif call.name == "Bash" and call.command.strip():
            script = f"export REPLAY_CALL={call.index}\n{call.command}"
            limit = bash_timeout if "sdo-operator" in call.command else 30.0
            sandbox.run(["bash", "-c", script], timeout=limit)
    final = _snapshot(work, "final")
    calls_log = root / "log" / "calls.tsv"
    for line in calls_log.read_text().splitlines() if calls_log.exists() else []:
        index, commit, args = line.split("\t", 2)
        result.snapshots.append(Snapshot(int(index), commit, git(work, "rev-parse", f"{commit}^{{tree}}"), args))
    result.snapshots.append(Snapshot(len(calls), final, git(work, "rev-parse", f"{final}^{{tree}}"), "final"))
    result.agent_commits, result.matched_commits = _fidelity(work, session.base)
    return result


def _snapshot(work: Path, label: str) -> str:
    index = work / ".git" / "replay-index"
    env = {"GIT_INDEX_FILE": str(index)}
    subprocess.run(["git", "-C", str(work), "add", "-A"], env=env, check=True, capture_output=True)
    tree = subprocess.run(
        ["git", "-C", str(work), "write-tree"], env=env, check=True, capture_output=True, text=True
    ).stdout.strip()
    index.unlink()
    commit = git(work, "commit-tree", tree, "-p", "HEAD", "-m", f"replay {label}")
    git(work, "update-ref", f"refs/replay/{label}", commit)
    return commit


def _fidelity(work: Path, base: str) -> tuple[int, int]:
    """How many of the agent's own commits (the recorded head's, after base) the replay rebuilt exactly."""
    recorded = git(work, "log", "refs/recorded/head", "--format=%T", f"^{base}").split()
    rebuilt = set(git(work, "log", "--glob=refs/replay/*", "--format=%T").split())
    return len(recorded), sum(tree in rebuilt for tree in recorded)
