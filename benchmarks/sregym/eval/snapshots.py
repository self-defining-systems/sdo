"""Memory snapshots as pinned git commits, and the offline merge of first-try deltas.

Per app the store keeps one repository holding the onboarding ``base`` commit, every first try's accepted memory
(``refs/first-try/<problem>``), and the snapshots built from them: ``own:<problem>`` (base plus that problem's own
delta) and ``merged`` (base plus every first try's delta). Snapshots are built with git plumbing on a temporary
index, so no checkout is touched. A delta is what a first try changed under the memory roots in ``DELTA_ROOTS``; its
unit is the directory directly under a root (a playbook, an incident detector, a corpus case, a lib directory), the
same layout the Go operational-memory reader classifies. A file directly under a root is not a unit: the playbook
index (``.sdo/playbooks/README.md``) is derived, never merged, and is regenerated from the merged playbooks' READMEs
(as ``layout.GeneratePlaybookIndex`` does); any other such file is not part of the layout and raises
``MemoryLayoutError``. Nothing that is not a directory unit is ever renamed.

Merge rules, per delta unit in recorded problem order:

- absent from the merge: copied;
- identical to a unit already merged: a duplicate, recorded;
- a playbook, detector or corpus unit that collides with different content: renamed ``<id>-<hash of problem>``, with
  the id rewritten in every text file of that problem's delta (so a detector keeps naming its renamed playbook),
  except a corpus case's captures, which are digest-bound evidence: only its ``labels.json`` is rewritten;
- a lib directory that collides with different content: kept as first merged, a conflict recorded (it cannot be
  renamed because other code imports it by name);
- units that are equal once their own id is masked are near-duplicates, recorded and kept.

After each problem's delta the memory check runs (an injected callable; ``memory_check_command`` is the real one: the
operator's accepted-checkout audit, the same structural, semantic and compile validation dispatch and the broker rely
on, without controller-only context); a delta that fails it is excluded and recorded, so the merged snapshot always
passes. Conflicts, renames, duplicates and near-duplicates go to
``merge-report.json`` as data, with the number of corpus cases the merged snapshot carries and the playbooks no case
names. This module never writes into a lane or the accepted repository of an install.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import subprocess
import tempfile
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

#: Where a first try's learning lives. The Go memory layout owns these paths; keep the list in one place.
DELTA_ROOTS = (
    ".sdo/playbooks/",
    ".sdo/diagnostics/detectors/incidents/",
    ".sdo/lib/",
    ".sdo/corpus/",
)
RENAMEABLE = (".sdo/playbooks/", ".sdo/diagnostics/detectors/incidents/", ".sdo/corpus/")
PLAYBOOKS_ROOT = ".sdo/playbooks/"
CORPUS_ROOT = ".sdo/corpus/"
#: Derived files directly under a delta root: regenerated from the merged tree, never taken from a delta.
PLAYBOOK_INDEX = ".sdo/playbooks/README.md"
PLAYBOOK_FILE = "README.md"
_IDENTITY = {
    "GIT_AUTHOR_NAME": "sdo-eval",
    "GIT_AUTHOR_EMAIL": "sdo-eval@localhost",
    "GIT_COMMITTER_NAME": "sdo-eval",
    "GIT_COMMITTER_EMAIL": "sdo-eval@localhost",
}


class MemoryLayoutError(ValueError):
    """A delta holds a file under a memory root that is neither a unit directory nor a derived file."""


@dataclass(frozen=True)
class CheckResult:
    ok: bool
    ran: bool = True
    output: str = ""


def no_check(repo: Path, commit: str) -> CheckResult:
    return CheckResult(ok=True, ran=False, output="memory check not configured")


def memory_check_command(operator: Path) -> list[str]:
    """The offline memory check: ``sdo-operator memory check --accepted`` on a checkout of the commit.

    The accepted audit is the structural, semantic and compile validation of a clean accepted tree (layout, derived
    playbook index, ownership-free), so it needs no SDO_ACTOR/SDO_PHASE/SDO_BASE_COMMIT or controller telemetry
    context. The environment (``command_check(..., env=...)``) carries ``SDO_OPERATOR_ROOT`` (operator sources) and
    ``SDO_TELEMETRY_POLICY`` (the install's telemetry policy, so detector queries resolve against its provided sources
    as in the broker; without it ``telemetry.unbound`` fails). Replay is off: the
    check is for the tree's shape and compilability, not for re-running recorded incidents."""
    return [str(operator), "memory", "check", "--accepted", "--replay", "off", "--root", "{root}"]


def command_check(
    command: Sequence[str], *, timeout: float = 600.0, env: Mapping[str, str] | None = None
) -> Callable[[Path, str], CheckResult]:
    """Run ``command`` in a checkout of the commit; ``{root}`` in an argument is replaced by its path."""

    def check(repo: Path, commit: str) -> CheckResult:
        with tempfile.TemporaryDirectory(prefix="eval-check-") as scratch:
            root = Path(scratch) / "memory"
            subprocess.run(["git", "clone", "-q", "--shared", "--no-checkout", str(repo), str(root)], check=True)
            subprocess.run(host_git_command(root, "checkout", "-q", "--detach", commit), check=True)
            argv = [part.replace("{root}", str(root)) for part in command]
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                cwd=root,
                env={**os.environ, **(env or {})},
            )
        # Diagnostics are stdout; stderr is progress logging that would push them out of a tail.
        return CheckResult(completed.returncode == 0, True, completed.stdout[-3000:] + completed.stderr[-1000:])

    return check


@dataclass
class MergeReport:
    app: str
    base: str
    merged: str
    included: list[str] = field(default_factory=list)
    excluded: list[dict[str, str]] = field(default_factory=list)
    missing_first_try: list[str] = field(default_factory=list)
    empty_delta: list[str] = field(default_factory=list)
    renames: dict[str, dict[str, str]] = field(default_factory=dict)
    duplicates: list[dict[str, str]] = field(default_factory=list)
    near_duplicates: list[dict[str, str]] = field(default_factory=list)
    conflicts: list[dict[str, str]] = field(default_factory=list)
    check: dict[str, Any] = field(default_factory=dict)
    #: Replay cases the merged snapshot carries, and the merged playbooks no case names: a playbook without a case is
    #: never replayed against anything, so the report says so rather than leaving it for a reader to discover.
    corpus_cases: int = 0
    playbooks_without_case: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Entry:
    mode: str
    sha: str


Unit = dict[str, Entry]  # path relative to the unit directory -> blob


class GitError(RuntimeError):
    pass


def _git(repo: Path, *args: str, env: Mapping[str, str] | None = None, data: bytes | None = None) -> bytes:
    completed = subprocess.run(
        host_git_command(repo, *args),
        input=data,
        capture_output=True,
        check=False,
        env={**os.environ, **_IDENTITY, **(env or {})},
    )
    if completed.returncode:
        raise GitError(f"git {' '.join(args)}: {completed.stderr.decode(errors='replace').strip()}")
    return completed.stdout


def _slug(problem: str) -> str:
    return hashlib.sha1(problem.encode()).hexdigest()[:6]


def _unit_of(path: str) -> tuple[str, str] | None:
    """(root, unit directory) for a path under a delta root, else ``None``.

    The playbook index is derived and has no unit (``None``). Any other file directly under a root is not part of the
    memory layout and raises, so a merge never renames or carries a file the reader would refuse."""
    for root in DELTA_ROOTS:
        if path.startswith(root):
            first, slash, _ = path[len(root) :].partition("/")
            if path == PLAYBOOK_INDEX:
                return None
            if not slash:
                raise MemoryLayoutError(f"{path}: not a memory unit (a directory below {root}) nor a derived file")
            return root, root + first
    return None


def _index_title(readme: str, playbook: str) -> str:
    """The title ``layout.GeneratePlaybookIndex`` gives a playbook: its first ``# `` heading, else its id."""
    text = readme
    if text.startswith("---\n"):
        _, closed, body = text[4:].partition("---\n")
        if closed:
            text = body
    for line in text.split("\n"):
        if line.startswith("# "):
            title = line[2:].strip()
            if title:
                return title.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")
            break
    return playbook


def _is_retired(readme: str) -> bool:
    """Whether the README's front matter retires the playbook; malformed metadata stays listed (as in Go)."""
    if not readme.startswith("---\n"):
        return False
    parts = readme.split("---\n", 2)
    if len(parts) != 3:
        return False
    try:
        meta = yaml.safe_load(parts[1]) or {}
    except yaml.YAMLError:
        return False
    if not isinstance(meta, dict) or meta.get("status", "active") != "retired":
        return False
    reason = meta.get("retired_reason")
    return isinstance(reason, str) and bool(reason.strip())


def generate_playbook_index(readmes: Mapping[str, str]) -> str | None:
    """The playbook index for ``{playbook id: README text}``: one line per active playbook, sorted by id.

    Mirrors ``layout.GeneratePlaybookIndex`` (the Go check refuses an index that differs from it); ``None`` when no
    playbook has a README."""
    if not readmes:
        return None
    lines = [
        f"- [{_index_title(text, playbook)}]({playbook}/{PLAYBOOK_FILE})\n"
        for playbook, text in sorted(readmes.items())
        if not _is_retired(text)
    ]
    return "# Playbooks\n" + ("\n" + "".join(lines) if lines else "")


def _digest(unit: Unit) -> str:
    return hashlib.sha256(json.dumps(sorted((k, v.sha) for k, v in unit.items())).encode()).hexdigest()


class GitSnapshotStore:
    def __init__(
        self,
        root: Path,
        bases: Mapping[str, tuple[Path, str]],
        *,
        check: Callable[[Path, str], CheckResult] = no_check,
    ) -> None:
        self._root = root
        self._bases = dict(bases)
        self._check = check
        self._lock = threading.RLock()
        self._first_try: dict[tuple[str, str], str | None] = {}
        self._own: dict[tuple[str, str], str] = {}
        self._merged: dict[str, str] = {}
        self._base_checked: set[str] = set()
        self.reports: dict[str, MergeReport] = {}
        root.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_config(cls, table: Mapping[str, Any]) -> GitSnapshotStore:
        bases = {app: (Path(spec["repo"]), spec["commit"]) for app, spec in table["bases"].items()}
        command = table.get("check_command")
        env = table.get("check_env") or None
        return cls(Path(table["root"]), bases, check=command_check(command, env=env) if command else no_check)

    def repositories(self) -> dict[str, Path]:
        return {app: self._repo(app) for app in self._bases}

    # -- repository ---------------------------------------------------------------------------------------------

    def _repo(self, app: str) -> Path:
        with self._lock:
            repo = self._root / app
            if not (repo / ".git").exists():
                repo.mkdir(parents=True, exist_ok=True)
                subprocess.run(["git", "init", "-q", str(repo)], check=True)
                source, commit = self._bases[app]
                self._fetch(repo, source, commit, "refs/base")
            elif app not in self._base_checked:
                # A store root outlives its stage config: a repository created for another base would silently
                # serve that base to every cell, so the configured base must be the stored one.
                _, commit = self._bases[app]
                stored = _git(repo, "rev-parse", "refs/base^{commit}").decode().strip()
                if not stored.startswith(commit) and not commit.startswith(stored):
                    raise ValueError(
                        f"eval: snapshot store {repo} holds base {stored[:12]} but the config pins {commit[:12]}; "
                        "use a new store root or move refs/base and delete refs/first-try/*"
                    )
                self._base_checked.add(app)
            return repo

    def _fetch(self, repo: Path, source: Path, commit: str, ref: str) -> None:
        src = source.resolve()
        upload = shlex.join(
            ["git", "-c", f"safe.directory={src}", "-c", f"safe.directory={src / '.git'}", "upload-pack"]
        )
        _git(repo, "fetch", "--no-tags", f"--upload-pack={upload}", str(src), f"+{commit}:{ref}")

    def base(self, app: str) -> str:
        repo = self._repo(app)
        return _git(repo, "rev-parse", "refs/base^{commit}").decode().strip()

    # -- first tries --------------------------------------------------------------------------------------------

    def import_first_try(self, app: str, problem: str, workspace: Path, commit: str) -> None:
        """Make a first try's accepted memory available here (called by the lane runner after the row)."""
        repo = self._repo(app)
        with self._lock:
            self._fetch(repo, workspace, commit, f"refs/first-try/{problem}")
            self._first_try[(app, problem)] = commit

    def load_first_tries(self, app: str) -> dict[str, str]:
        """Rebuild the first-try table of ``app`` from the repository's ``refs/first-try/*`` (a later process)."""
        repo = self._repo(app)
        out = _git(repo, "for-each-ref", "--format=%(refname) %(objectname)", "refs/first-try/").decode()
        found: dict[str, str] = {}
        for line in out.splitlines():
            ref, _, sha = line.partition(" ")
            found[ref.removeprefix("refs/first-try/")] = sha
        with self._lock:
            for problem, sha in found.items():
                self._first_try[(app, problem)] = sha
        return found

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        with self._lock:
            if memory_commit_after is None:
                self._first_try.setdefault((app, problem), None)
            else:
                self._first_try[(app, problem)] = memory_commit_after

    # -- deltas -------------------------------------------------------------------------------------------------

    def _units(self, repo: Path, base: str, commit: str) -> dict[str, Unit]:
        """Units the commit added or changed under the delta roots, with their files."""
        out = _git(repo, "diff", "--name-status", "--no-renames", "-z", base, commit, "--", *DELTA_ROOTS).split(b"\0")
        changed: set[str] = set()
        for index in range(0, len(out) - 1, 2):
            status, path = out[index].decode(), out[index + 1].decode()
            if status.startswith("D"):
                continue
            unit = _unit_of(path)
            if unit:
                changed.add(unit[1])
        units: dict[str, Unit] = {}
        for unit_path in sorted(changed):
            listing = _git(repo, "ls-tree", "-r", "-z", commit, "--", unit_path).split(b"\0")
            files: Unit = {}
            for line in listing:
                if not line:
                    continue
                meta, path = line.decode().split("\t", 1)
                mode, _, sha = meta.split(" ")
                files[path[len(unit_path) :].lstrip("/") if path != unit_path else ""] = Entry(mode, sha)
            if files:
                units[unit_path] = files
        return units

    def _tree_unit(self, repo: Path, commit: str, unit_path: str) -> Unit | None:
        listing = _git(repo, "ls-tree", "-r", "-z", commit, "--", unit_path).split(b"\0")
        files: Unit = {}
        for line in listing:
            if line:
                meta, path = line.decode().split("\t", 1)
                mode, _, sha = meta.split(" ")
                files[path[len(unit_path) :].lstrip("/") if path != unit_path else ""] = Entry(mode, sha)
        return files or None

    def _commit(
        self, repo: Path, parent: str, writes: Mapping[str, Entry], removes: Sequence[str], message: str
    ) -> str:
        with tempfile.TemporaryDirectory(prefix="eval-index-") as scratch:
            env = {"GIT_INDEX_FILE": str(Path(scratch) / "index")}
            _git(repo, "read-tree", parent, env=env)
            for path in removes:
                _git(repo, "update-index", "--force-remove", "--", path, env=env)
            for path, entry in writes.items():
                _git(repo, "update-index", "--add", "--cacheinfo", f"{entry.mode},{entry.sha},{path}", env=env)
            if any(path.startswith(PLAYBOOKS_ROOT) for path in (*writes, *removes)):
                self._regenerate_index(repo, env)
            tree = _git(repo, "write-tree", env=env).decode().strip()
        return _git(repo, "commit-tree", tree, "-p", parent, "-m", message).decode().strip()

    def _regenerate_index(self, repo: Path, env: Mapping[str, str]) -> None:
        """Rewrite the playbook index in the temporary index from the playbook READMEs it holds."""
        listing = _git(repo, "ls-files", "-s", "-z", "--", PLAYBOOKS_ROOT, env=env).split(b"\0")
        readmes: dict[str, str] = {}
        for line in listing:
            if not line:
                continue
            meta, path = line.decode().split("\t", 1)
            playbook, _, name = path[len(PLAYBOOKS_ROOT) :].partition("/")
            if name == PLAYBOOK_FILE:
                readmes[playbook] = _git(repo, "cat-file", "blob", meta.split(" ")[1]).decode("utf-8", "replace")
        index = generate_playbook_index(readmes)
        if index is None:
            return
        sha = _git(repo, "hash-object", "-w", "--stdin", data=index.encode()).decode().strip()
        _git(repo, "update-index", "--add", "--cacheinfo", f"100644,{sha},{PLAYBOOK_INDEX}", env=env)

    def _apply(self, repo: Path, parent: str, units: Mapping[str, Unit], message: str) -> str:
        """``parent`` with each unit replaced by the given files."""
        writes: dict[str, Entry] = {}
        removes: list[str] = []
        for unit_path, files in units.items():
            existing = self._tree_unit(repo, parent, unit_path) or {}
            removes += [f"{unit_path}/{rel}" if rel else unit_path for rel in existing if rel not in files]
            for rel, entry in files.items():
                writes[f"{unit_path}/{rel}" if rel else unit_path] = entry
        return self._commit(repo, parent, writes, removes, message)

    # -- snapshots ----------------------------------------------------------------------------------------------

    def resolve(self, app: str, key: str) -> str:
        if key == "base":
            return self.base(app)
        if key == "merged":
            with self._lock:
                if app not in self._merged:
                    raise ValueError(f"eval: the merged snapshot of {app} has not been built")
                return self._merged[app]
        if key.startswith("own:"):
            return self._own_snapshot(app, key[4:])
        raise ValueError(f"unknown snapshot {key!r}")

    def _own_snapshot(self, app: str, problem: str) -> str:
        with self._lock:
            cached = self._own.get((app, problem))
            if cached:
                return cached
            repo, base = self._repo(app), self.base(app)
            commit = self._first_try.get((app, problem))
            if commit is None:
                self._own[(app, problem)] = base  # no first try, so no delta: base itself
                return base
            units = self._units(repo, base, commit)
            snapshot = self._apply(repo, base, units, f"own delta of {problem}") if units else base
            self._own[(app, problem)] = snapshot
            return snapshot

    def merge(self, app: str, problems: Sequence[str]) -> MergeReport:
        repo, base = self._repo(app), self.base(app)
        report = MergeReport(app=app, base=base, merged=base)
        current = base
        merged_units: dict[str, Unit] = {}
        masked: dict[str, tuple[str, str]] = {}
        for problem in problems:
            with self._lock:
                commit = self._first_try.get((app, problem))
            if commit is None:
                report.missing_first_try.append(problem)
                continue
            try:
                units = self._units(repo, base, commit)
            except GitError as exc:
                report.excluded.append({"problem": problem, "reason": f"delta unreadable: {exc}"})
                continue
            if not units:
                report.empty_delta.append(problem)
                continue
            candidate, notes = self._merge_one(repo, problem, units, merged_units)
            if candidate is None:
                report.duplicates += [{"problem": problem, "unit": unit} for unit in notes["duplicates"]]
                report.conflicts += [{"problem": problem, "unit": unit, "kind": "lib"} for unit in notes["conflicts"]]
                if notes["duplicates"] and not notes["conflicts"]:
                    report.included.append(problem)  # nothing new to add: every unit is already merged
                else:
                    report.excluded.append({"problem": problem, "reason": "all units conflict"})
                continue
            snapshot = self._apply(repo, current, candidate, f"merge delta of {problem}")
            verdict = self._check(repo, snapshot)
            report.check[problem] = {"ok": verdict.ok, "ran": verdict.ran, "output": verdict.output[-500:]}
            if not verdict.ok:
                report.excluded.append({"problem": problem, "reason": "memory check failed"})
                continue
            current = snapshot
            report.included.append(problem)
            if notes["renames"]:
                report.renames[problem] = notes["renames"]
            report.duplicates += [{"problem": problem, "unit": unit} for unit in notes["duplicates"]]
            report.conflicts += [{"problem": problem, "unit": unit, "kind": "lib"} for unit in notes["conflicts"]]
            for unit_path, files in candidate.items():
                merged_units[unit_path] = files
                key = self._masked_digest(repo, unit_path, files)
                if key in masked and masked[key][0] != problem:
                    report.near_duplicates.append(
                        {"problem": problem, "unit": unit_path, "of_problem": masked[key][0], "of_unit": masked[key][1]}
                    )
                masked.setdefault(key, (problem, unit_path))
        report.merged = current
        report.corpus_cases, report.playbooks_without_case = self._case_coverage(repo, current)
        with self._lock:
            self._merged[app] = current
            self.reports[app] = report
        (repo / "merge-report.json").write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
        return report

    def _merge_one(
        self, repo: Path, problem: str, units: Mapping[str, Unit], merged: Mapping[str, Unit]
    ) -> tuple[dict[str, Unit] | None, dict[str, Any]]:
        notes: dict[str, Any] = {"renames": {}, "duplicates": [], "conflicts": []}
        colliding = {u for u, files in units.items() if u in merged and _digest(files) != _digest(merged[u])}
        renames: dict[str, str] = {}
        for unit_path in colliding:
            root = next(r for r in DELTA_ROOTS if unit_path.startswith(r))
            if root in RENAMEABLE:
                old = unit_path[len(root) :]
                renames[old] = f"{old}-{_slug(problem)}"
        out: dict[str, Unit] = {}
        for unit_path, files in units.items():
            root = next(r for r in DELTA_ROOTS if unit_path.startswith(r))
            if unit_path in merged and _digest(files) == _digest(merged[unit_path]):
                notes["duplicates"].append(unit_path)
                continue
            if unit_path in colliding and root not in RENAMEABLE:
                notes["conflicts"].append(unit_path)
                continue
            if renames:
                new_id = renames.get(unit_path[len(root) :], unit_path[len(root) :])
                # A corpus case's captures are digest-bound evidence; only its labels name a playbook.
                only = "labels.json" if root == CORPUS_ROOT else None
                out[root + new_id] = self._rewrite(repo, files, renames, only=only)
            else:
                out[unit_path] = files
        if renames:
            notes["renames"] = renames
        return (out or None), notes

    def _case_coverage(self, repo: Path, commit: str) -> tuple[int, list[str]]:
        """The corpus cases in the commit, and its playbooks that no case's ``root_cause_program_ids`` names."""
        named: set[str] = set()
        cases = 0
        playbooks: set[str] = set()
        for line in _git(repo, "ls-tree", "-r", "-z", commit, "--", ".sdo/corpus/", PLAYBOOKS_ROOT).split(b"\0"):
            if not line:
                continue
            meta, path = line.decode().split("\t", 1)
            sha = meta.split(" ")[2]
            if path.startswith(PLAYBOOKS_ROOT):
                parts = path[len(PLAYBOOKS_ROOT) :].split("/")
                if len(parts) > 1:
                    playbooks.add(parts[0])
            elif path.endswith("/labels.json"):
                cases += 1
                try:
                    labels = json.loads(_git(repo, "cat-file", "blob", sha))
                except ValueError:
                    continue
                named.update(labels.get("root_cause_program_ids") or [])
        return cases, sorted(playbooks - named)

    def _rewrite(self, repo: Path, files: Unit, renames: Mapping[str, str], *, only: str | None = None) -> Unit:
        """``files`` with each renamed id replaced in their text; ``only`` limits that to the one file naming ids."""
        result: Unit = {}
        for rel, entry in files.items():
            if only is not None and rel != only:
                result[rel] = entry
                continue
            blob = _git(repo, "cat-file", "blob", entry.sha)
            try:
                text = blob.decode("utf-8")
            except UnicodeDecodeError:
                result[rel] = entry
                continue
            for old, new in renames.items():
                text = re.sub(rf"(?<![\w-]){re.escape(old)}(?![\w-])", new, text)
            sha = _git(repo, "hash-object", "-w", "--stdin", data=text.encode()).decode().strip()
            result[rel] = Entry(entry.mode, sha)
        return result

    def _masked_digest(self, repo: Path, unit_path: str, files: Unit) -> str:
        unit_id = unit_path.rsplit("/", 1)[-1]
        digest = hashlib.sha256()
        for rel, entry in sorted(files.items()):
            text = _git(repo, "cat-file", "blob", entry.sha).replace(unit_id.encode(), b"<ID>")
            digest.update(rel.encode() + b"\0" + text)
        return digest.hexdigest()
