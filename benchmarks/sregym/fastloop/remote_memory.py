"""Shared accepted memory through one private GitHub branch per app and run.

Each lane keeps its own accepted repository on its private PVC. Its Go broker
fetches from and pushes to the app's run branch inside the publishing lock, so
lanes on any host share one accepted history and a non-fast-forward push is
replayed through the broker's concurrent-publish path. The harness only names
the branch, seeds it, binds lanes to it and closes it at the end of the run.

The credential is the host's existing GitHub ssh key. The harness reads it by
path, copies it into a lane Secret mounted read-only into the controller, and
never prints it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, control_namespace_for, kubectl, wait_for

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

ENDPOINTS_FILENAME = "memory-endpoints.json"
SSH_SECRET = "sdo-memory-upstream-ssh"
#: Must equal install.MemoryUpstreamSSHDir in the Go installer.
SSH_DIR = "/sdo/memory-upstream-ssh"
RESERVED_BRANCHES = frozenset({"main", "master"})
_REPO = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9._-]{1,100}$")


@dataclass(frozen=True)
class MemoryEndpoint:
    """One app's shared memory: a GitHub repository and this run's branch."""

    repo: str
    branch: str

    def __post_init__(self) -> None:
        if not isinstance(self.repo, str) or not isinstance(self.branch, str):
            raise TypeError("eval: memory endpoint repo and branch must be strings")
        if not _REPO.match(self.repo) or self.repo.endswith(".git"):
            raise ValueError(f"eval: memory endpoint repo must be OWNER/NAME, got {self.repo!r}")
        if not self.branch or self.branch in RESERVED_BRANCHES:
            raise ValueError(f"eval: memory endpoint branch must name this run's branch, never {self.branch!r}")
        checked = subprocess.run(
            ["git", "check-ref-format", f"refs/heads/{self.branch}"], capture_output=True, check=False
        )
        if checked.returncode:
            raise ValueError(f"eval: memory endpoint branch {self.branch!r} is not a valid branch name")

    @property
    def url(self) -> str:
        return f"git@github.com:{self.repo}.git"


def run_branch(label: str, when: datetime | None = None) -> str:
    """One branch per run, ``live/<yyyymmdd>-<run-label>``."""

    slug = re.sub(r"[^a-z0-9._-]+", "-", label.lower()).strip("-.")
    if not slug:
        raise ValueError("eval: a run label is required for the memory branch")
    return f"live/{(when or datetime.now(UTC)).strftime('%Y%m%d')}-{slug}"


def load_memory_endpoints(path: Path) -> dict[str, MemoryEndpoint]:
    """Read ``{app: {repo, branch}}``; every entry needs both fields and nothing else."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"eval: {path} must map each app to its memory repo and branch")
    endpoints = {}
    for app, entry in raw.items():
        if not isinstance(entry, dict) or set(entry) != {"repo", "branch"}:
            raise ValueError(f"eval: {path}: app {app!r} needs exactly 'repo' and 'branch'")
        endpoints[app] = MemoryEndpoint(repo=entry["repo"], branch=entry["branch"])
    return endpoints


def write_memory_endpoints(path: Path, endpoints: dict[str, MemoryEndpoint]) -> Path:
    payload = {app: {"repo": e.repo, "branch": e.branch} for app, e in sorted(endpoints.items())}
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def endpoint_for(endpoints: dict[str, MemoryEndpoint], app: str) -> MemoryEndpoint:
    """The app's endpoint; a run never starts on a default or unnamed branch."""

    try:
        return endpoints[app]
    except KeyError:
        raise ValueError(
            f"eval: no memory repo/branch for app {app!r}; every run must set one per app in {ENDPOINTS_FILENAME}"
        ) from None


def _git(repository: Path | None, *args: str, timeout: float | None = None) -> str:
    command = ["git", *(["-C", str(repository)] if repository is not None else []), *args]
    return subprocess.check_output(
        command,
        text=True,
        stderr=subprocess.PIPE,
        timeout=timeout or wait_for(WaitKind.COMMAND),
        env=_git_env(),
    ).strip()


def _git_env() -> dict[str, str]:
    return {**os.environ, "GIT_TERMINAL_PROMPT": "0"}


def remote_head(endpoint: MemoryEndpoint, url: str | None = None) -> str:
    """The run branch head, or ``""`` when the branch does not exist yet."""

    listing = _git(None, "ls-remote", "--", url or endpoint.url, f"refs/heads/{endpoint.branch}")
    return listing.split()[0] if listing else ""


def ensure_run_branch(endpoint: MemoryEndpoint, seed: Path, url: str | None = None) -> str:
    """Create the run branch from the seed lane's accepted HEAD unless it exists; return its head."""

    head = remote_head(endpoint, url)
    if head:
        return head
    commit = _git(seed, "rev-parse", "--verify", "HEAD^{commit}")
    _git(seed, "push", "--quiet", "--", url or endpoint.url, f"{commit}:refs/heads/{endpoint.branch}")
    return commit


def matching_commit(repository: Path, tree: str, head: str) -> str:
    """Newest first-parent commit of ``head`` whose tree is ``tree``; lanes join only without private memory."""

    for line in _git(repository, "log", "--first-parent", "--format=%H %T", head).splitlines():
        commit, commit_tree = line.split()
        if commit_tree == tree:
            return commit
    raise ValueError("eval: lane accepted memory matches no commit of the run branch; prepare it from the run seed")


def adopt_run_branch(repository: Path, endpoint: MemoryEndpoint, url: str | None = None) -> str:
    """Point a lane's accepted repository at the run branch history with its own tree.

    The lane's tree must equal a commit of the branch, so nothing it holds is lost; its
    broker fast-forwards to the branch head on its first operation.
    """

    tree = _git(repository, "rev-parse", "HEAD^{tree}")
    _git(
        repository,
        "fetch",
        "--no-tags",
        "--quiet",
        "--",
        url or endpoint.url,
        f"+refs/heads/{endpoint.branch}:refs/sdo/upstream",
    )
    commit = matching_commit(repository, tree, "refs/sdo/upstream")
    _git(repository, "reset", "--quiet", "--soft", commit)
    if _git(repository, "status", "--porcelain"):
        raise ValueError("eval: lane accepted checkout is not clean")
    for remote in _git(repository, "remote").split():
        _git(repository, "remote", "remove", remote)
    return commit


def controller_job_with_upstream(job: dict[str, Any], endpoint: MemoryEndpoint) -> dict[str, Any]:
    """The controller Job with the run branch and the read-only key mount, as the Go installer builds it."""

    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    args = list(container["args"])
    for flag in ("--memory-upstream-url", "--memory-upstream-branch", "--memory-upstream-ssh-dir"):
        while flag in args:
            index = args.index(flag)
            del args[index : index + 2]
    container["args"] = [
        *args,
        *("--memory-upstream-url", endpoint.url, "--memory-upstream-branch", endpoint.branch),
        *("--memory-upstream-ssh-dir", SSH_DIR),
    ]
    pod["volumes"] = [v for v in pod.get("volumes", []) if v["name"] != "memory-upstream-ssh"] + [
        {"name": "memory-upstream-ssh", "secret": {"secretName": SSH_SECRET, "defaultMode": 0o444}}
    ]
    container["volumeMounts"] = [m for m in container.get("volumeMounts", []) if m["name"] != "memory-upstream-ssh"] + [
        {"name": "memory-upstream-ssh", "mountPath": SSH_DIR, "readOnly": True}
    ]
    return job


def github_known_hosts(known_hosts: Path) -> str:
    """GitHub's host keys from the host's own known_hosts (hashed or plain)."""

    found = subprocess.run(
        ["ssh-keygen", "-F", "github.com", "-f", str(known_hosts)], capture_output=True, text=True, check=False
    )
    rows = [line for line in found.stdout.splitlines() if line and not line.startswith("#")]
    if not rows:
        raise ValueError(f"eval: {known_hosts} has no github.com host key; run `ssh -T git@github.com` on this host")
    return "\n".join(rows) + "\n"


def apply_ssh_secret(environment: FastloopEnvironment, identity: Path, known_hosts: Path) -> None:
    """Copy the host's existing key into the lane's control namespace; the key is never printed."""

    if not identity.is_file():
        raise ValueError(f"eval: ssh identity {identity} does not exist")
    with tempfile.TemporaryDirectory() as scratch:
        hosts = Path(scratch) / "known_hosts"
        hosts.write_text(github_known_hosts(known_hosts), encoding="utf-8")
        rendered = kubectl(
            [
                *("create", "secret", "generic", SSH_SECRET, "--dry-run=client", "-o", "json"),
                f"--from-file=identity={identity}",
                f"--from-file=known_hosts={hosts}",
            ],
            namespace=control_namespace_for(environment.namespace),
            kubeconfig=environment.kubeconfig,
        ).stdout
    kubectl(
        ["apply", "-f", "-"],
        namespace=control_namespace_for(environment.namespace),
        kubeconfig=environment.kubeconfig,
        input_text=rendered,
    )


REPOSITORY = "/workspace/application"


def _pod_tar(environment: FastloopEnvironment, pod: str, script: str, data: bytes | None = None) -> bytes:
    """Run a tar pipeline in the lane's controller container; bytes in and out."""

    command = [
        *("kubectl", "--kubeconfig", str(environment.kubeconfig), "--namespace"),
        *(control_namespace_for(environment.namespace), "exec", *(["-i"] if data is not None else []), pod),
        *("-c", "controller", "--", "sh", "-c", script),
    ]
    completed = subprocess.run(
        command, input=data, capture_output=True, check=False, timeout=wait_for(WaitKind.SLOW_COMMAND)
    )
    if completed.returncode:
        raise RuntimeError(f"eval: lane repository copy failed: {completed.stderr.decode(errors='replace')[-1024:]}")
    return completed.stdout


def bind_lane_to_branch(
    environment: FastloopEnvironment,
    endpoint: MemoryEndpoint,
    *,
    identity: Path,
    known_hosts: Path,
    seed: bool = False,
) -> str:
    """Bind one prepared, idle lane's accepted repository to the run branch.

    Pauses the lane's controller, copies its accepted repository out, seeds the branch
    from it (first lane of the app) or adopts the branch history with the lane's own tree,
    copies it back through the paused controller, and recreates the controller Job with
    the upstream and the read-only key. Works for any lane PVC; nothing is shared on disk.
    """
    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop.shared_memory import JOB_NAME, _json, lane_environment, shared_controller_job

    control = control_namespace_for(environment.namespace)
    pods = _json(environment, "get", "pods", "--selector", f"job-name={JOB_NAME}", "-o", "json", namespace=control)
    running = [p["metadata"]["name"] for p in pods["items"] if p.get("status", {}).get("phase") == "Running"]
    if len(running) != 1:
        raise ValueError("eval: binding requires one running controller")
    pod = running[0]
    job = _json(environment, "get", "job", JOB_NAME, "-o", "json", namespace=control)
    # Incident IDs derive from the application name, so lanes sharing a branch need distinct ones.
    application = f"{environment.problem_application or environment.application}-{environment.cluster}"
    application = application[:30].rstrip("-") + "-" + uuid.uuid4().hex
    with lane_environment(environment):
        adapter.pause_for_recovery(adapter.KubectlClusterOps(), control)
    with tempfile.TemporaryDirectory(prefix="sdo-memory-bind-") as scratch:
        local = Path(scratch) / "application"
        local.mkdir()
        archive = _pod_tar(environment, pod, f"tar -C {REPOSITORY} -cf - .")
        subprocess.run(["tar", "-C", str(local), "-xf", "-"], input=archive, check=True)
        if seed:
            ensure_run_branch(endpoint, local)
        commit = adopt_run_branch(local, endpoint)
        packed = subprocess.run(["tar", "-C", str(local), "-cf", "-", "."], capture_output=True, check=True).stdout
        staged = REPOSITORY + ".bind"
        _pod_tar(
            environment,
            pod,
            f'rm -rf "{staged}" && mkdir "{staged}" && tar -C "{staged}" -xf - && '
            f'rm -rf "{REPOSITORY}.old" && mv "{REPOSITORY}" "{REPOSITORY}.old" && mv "{staged}" "{REPOSITORY}" && '
            f'rm -rf "{REPOSITORY}.old"',
            packed,
        )
    kubectl(
        ["delete", "job", JOB_NAME, "--cascade=foreground", "--wait=true", "--timeout=120s"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    apply_ssh_secret(environment, identity, known_hosts)
    recreated = controller_job_with_upstream(shared_controller_job(job, application), endpoint)
    kubectl(
        ["create", "-f", "-"], namespace=control, kubeconfig=environment.kubeconfig, input_text=json.dumps(recreated)
    )
    kubectl(
        ["wait", "--for=condition=Ready", "pod", "--selector", f"job-name={JOB_NAME}", "--timeout=120s"],
        namespace=control,
        kubeconfig=environment.kubeconfig,
    )
    environment.application = application
    environment.memory_repo, environment.memory_branch = endpoint.repo, endpoint.branch
    environment.state_path.unlink(missing_ok=True)
    environment.save()
    return commit


def close_run_branch(endpoint: MemoryEndpoint, row: str, *, message: str, url: str | None = None) -> str:
    """End of run: tag the run branch head and add one README index row on main."""

    head = remote_head(endpoint, url)
    if not head:
        raise ValueError(f"eval: run branch {endpoint.branch} does not exist")
    if "\n" in row or not row.startswith("|"):
        raise ValueError("eval: the README row must be one Markdown table row")
    tag = f"{endpoint.branch}-final"
    with tempfile.TemporaryDirectory(prefix="sdo-memory-close-") as scratch:
        index = Path(scratch) / "index"
        _git(None, "clone", "--quiet", "--depth", "1", "--branch", "main", "--", url or endpoint.url, str(index))
        _git(index, "fetch", "--quiet", "--depth", "1", "origin", head)
        _git(
            index, "-c", "user.name=SDO run", "-c", "user.email=sdo-run@invalid", "tag", "-a", tag, "-m", message, head
        )
        readme = index / "README.md"
        text = readme.read_text(encoding="utf-8") if readme.exists() else ""
        if endpoint.branch not in text:
            readme.write_text(text.rstrip("\n") + "\n" + row + "\n", encoding="utf-8")
            _git(index, "add", "README.md")
            _git(
                index,
                "-c",
                "user.name=SDO run",
                "-c",
                "user.email=sdo-run@invalid",
                "commit",
                "--quiet",
                "-m",
                f"index {endpoint.branch}",
            )
        _git(index, "push", "--quiet", "origin", "HEAD:refs/heads/main", f"refs/tags/{tag}")
    return tag
