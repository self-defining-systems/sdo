"""Onboard SDO onto a kind cluster for a fuzzops pack: SDO deploys the pack's manifests and operates them.

``python -m benchmarks.fuzzops.install --pack <fuzzops>/examples/<app> --work <dir> --namespace <ns> ...``

1. writes the application repository (``<work>/app``) and the attached pack (``<work>/pack``) from the pack;
2. creates the application and control namespaces, the agent credentials Secret (``--from-file``; the adapter
   never reads the value) and an empty registry push Secret, and gives the kind nodes' preloaded app images their
   digest names (SDO pins every image to a digest);
3. runs ``sdo-operator host --deployment-plan``: SDO applies exactly the pack's manifests, writes the goal and
   architecture, the detection designer authors health detectors, and the long-running controller starts.

Operator configuration is the benchmark profile (``sdo-bench.yaml``) and any ``--config`` overlay files, passed to
``host`` as ``--config`` in that order; the agent backend, model and effort, the evidence sink and the watch-only mode
are ``SDO_*`` variables (``docs/config.md``).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from benchmarks.fuzzops import config
from benchmarks.fuzzops.pack import SourcePack, deployment_plan, images, write_app_repository, write_attached_pack

BUILDKIT_IMAGE = "moby/buildkit@sha256:ef31818fffd0b9133d710742e06b492553f7ebc54f5fd66cd21a1a9bf2e51f76"
CREDENTIALS_SECRET = "sdo-agent-credentials"
REGISTRY_SECRET = "sdo-registry-push"
REPOSITORY_PVC = "sdo-application-repository"


@dataclass(frozen=True)
class InstallConfig:
    """Everything ``sdo-operator host`` needs for one attached app."""

    pack: Path
    work: Path
    namespace: str
    control_namespace: str
    objective: str
    controller_image: str
    responder_image: str
    operator_binary: Path
    kubeconfig: Path
    auth_file: Path
    provider: str = "codex"
    #: The agent model; the experiment names it (the spec's operator settings), there is no default.
    model: str = ""
    effort: str = ""
    operation_id: str = "fuzzops-onboard"
    source: Path | None = None
    #: Deployment attempts; ``None`` keeps the configured value.
    max_attempts: int | None = None
    kind_cluster: str = "kind"
    watch_only: bool = False
    #: Node directory that receives SDO's run evidence and outlives the control namespace (for kind, the
    #: ``containerPath`` of an ``extraMounts`` entry made when the cluster was created); empty: no sink.
    evidence_sink_host_path: str = ""
    #: Run directory inside the sink; empty: the control namespace.
    evidence_run_id: str = ""
    #: Operator config overlay files passed after the benchmark profile, in order.
    config_files: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        self.host_env()
        for name in ("namespace", "control_namespace", "objective", "controller_image", "responder_image", "model"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if self.namespace == self.control_namespace:
            raise ValueError("the control namespace must differ from the application namespace")
        for name in ("work", "operator_binary", "kubeconfig", "auth_file"):
            if not getattr(self, name).is_absolute():
                raise ValueError(f"{name} must be an absolute path")

    @property
    def application(self) -> str:
        return self.namespace

    def host_args(self, repository: Path, plan: dict[str, object]) -> list[str]:
        """The ``sdo-operator host`` command line."""
        args = [
            str(self.operator_binary),
            "host",
            "--repository",
            str(repository),
            "--namespace",
            self.namespace,
            "--control-namespace",
            self.control_namespace,
            "--application",
            self.application,
            "--health-objective",
            self.objective,
            "--operation-id",
            self.operation_id,
            "--controller-image",
            self.controller_image,
            "--responder-image",
            self.responder_image,
            "--buildkit-image",
            BUILDKIT_IMAGE,
            "--repository-pvc",
            REPOSITORY_PVC,
            "--credentials-secret",
            CREDENTIALS_SECRET,
            "--registry-push-secret",
            REGISTRY_SECRET,
            "--deployment-plan",
            json.dumps(plan, separators=(",", ":"), sort_keys=True),
            "--kubeconfig",
            str(self.kubeconfig),
        ]
        return [*args, *config.config_args((config.BENCH_PROFILE, *self.config_files))]

    def host_env(self) -> dict[str, str]:
        """The ``SDO_*`` variables of the ``host`` launch: agent, evidence sink, attempts and mode."""
        env = config.operator_env(
            provider=self.provider,
            model=self.model,
            effort=self.effort,
            evidence_host_path=self.evidence_sink_host_path,
            evidence_run_id=self.evidence_run_id,
            # Detection and closure only: no response, no reflection, accepted memory unchanged.
            mode="watch-only" if self.watch_only else "",
        )
        if self.max_attempts is not None:
            env["SDO_DEPLOYMENT_MAX_ATTEMPTS"] = str(self.max_attempts)
        return env


def _kubectl(cfg: InstallConfig, *args: str, stdin: str | None = None) -> None:
    subprocess.run(["kubectl", "--kubeconfig", str(cfg.kubeconfig), *args], check=True, input=stdin, text=True)


def provision_namespaces(cfg: InstallConfig) -> None:
    """Both namespaces, agent credentials (copied by kubectl from the file) and an empty push Secret."""
    ns = cfg.control_namespace
    for name in (cfg.namespace, ns):
        _kubectl(
            cfg,
            "apply",
            "-f",
            "-",
            stdin=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}}),
        )
    _kubectl(cfg, "-n", ns, "delete", "secret", CREDENTIALS_SECRET, REGISTRY_SECRET, "--ignore-not-found")
    key = "auth.json" if cfg.provider == "codex" else ".credentials.json"
    _kubectl(cfg, "-n", ns, "create", "secret", "generic", CREDENTIALS_SECRET, f"--from-file={key}={cfg.auth_file}")
    push = json.dumps(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "kubernetes.io/dockerconfigjson",
            "metadata": {"name": REGISTRY_SECRET, "namespace": ns},
            "stringData": {".dockerconfigjson": json.dumps({"auths": {}})},
        }
    )
    _kubectl(cfg, "apply", "-f", "-", stdin=push)


def normalized(image: str) -> str:
    """containerd's full name of an image reference (``mongo:4`` is ``docker.io/library/mongo:4``)."""
    name = image
    first = name.split("/", 1)[0]
    if "/" not in name:
        name = "docker.io/library/" + name
    elif "." not in first and ":" not in first and first != "localhost":
        name = "docker.io/" + name
    return name


def digest_tags(images: list[str], node_listing: str) -> list[tuple[str, str]]:
    """(tag reference, digest reference) for each image the node holds by tag (``ctr images ls`` output).

    A kind-loaded image is on the node only under its tag; SDO pins every image to a digest it reads from the
    nodes (or a public registry), so the node must also list the digest name of the bytes it already has.
    """
    held: dict[str, str] = {}
    for line in node_listing.splitlines()[1:]:
        fields = line.split()
        if len(fields) >= 3 and fields[2].startswith("sha256:"):
            held[fields[0]] = fields[2]
    pairs = []
    for image in images:
        full = normalized(image)
        if "@" in full or full not in held:
            continue
        repository = full.rsplit(":", 1)[0]
        pairs.append((full, f"{repository}@{held[full]}"))
    return pairs


def declare_node_images(cfg: InstallConfig, images: list[str]) -> list[str]:
    """Give every kind node's preloaded app images their digest names (benchmark image delivery)."""
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", cfg.kind_cluster], check=True, capture_output=True, text=True
    ).stdout.split()
    declared = []
    for node in nodes:
        ctr = ["docker", "exec", node, "ctr", "-n", "k8s.io", "images"]
        listing = subprocess.run([*ctr, "ls"], check=True, capture_output=True, text=True).stdout
        held = {line.split()[0] for line in listing.splitlines()[1:] if line.split()}
        for tag, digest in digest_tags(images, listing):
            if digest not in held:
                tagged = subprocess.run([*ctr, "tag", "--force", tag, digest], check=False, capture_output=True)
                # A concurrent install on the same node may have just created it.
                if (
                    tagged.returncode != 0
                    and digest
                    not in subprocess.run([*ctr, "ls", "-q"], check=True, capture_output=True, text=True).stdout.split()
                ):
                    raise subprocess.CalledProcessError(tagged.returncode, tagged.args, tagged.stdout, tagged.stderr)
            declared.append(f"{node}: {digest}")
    return declared


def install(cfg: InstallConfig, *, description: str) -> int:
    pack = SourcePack(cfg.pack)
    repository = cfg.work / "app"
    if not repository.exists():
        write_app_repository(pack, repository, description=description, source=cfg.source)
    write_attached_pack(pack, cfg.work / "pack")
    plan = deployment_plan(pack.manifests(), cfg.namespace)
    for line in declare_node_images(cfg, images(pack.manifests())):
        print(f"declared {line}", flush=True)
    (cfg.work / "deployment-plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    provision_namespaces(cfg)
    with (cfg.work / "host-receipt.json").open("w") as receipt, (cfg.work / "host.log").open("w") as log:
        return subprocess.run(
            cfg.host_args(repository, plan),
            stdout=receipt,
            stderr=log,
            check=False,
            env={**os.environ, **cfg.host_env()},
        ).returncode


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pack", type=Path, required=True, help="fuzzops app pack directory")
    p.add_argument("--work", type=Path, required=True, help="work directory (app repository, attached pack, logs)")
    p.add_argument("--namespace", required=True, help="application namespace SDO deploys into")
    p.add_argument("--control-namespace", required=True)
    p.add_argument("--objective", required=True, help="human health objective")
    p.add_argument("--description", default="Application source: plain Kubernetes manifests under k8s/.")
    p.add_argument("--controller-image", required=True)
    p.add_argument("--responder-image", required=True)
    p.add_argument("--operator-binary", type=Path, required=True, help="host-built sdo-operator")
    p.add_argument("--kubeconfig", type=Path, required=True)
    p.add_argument("--auth-file", type=Path, default=Path.home() / ".codex" / "auth.json")
    p.add_argument("--provider", default="codex")
    p.add_argument("--model", required=True, help="agent model (the experiment's setting)")
    p.add_argument("--effort", default="")
    p.add_argument("--source", type=Path, help="the app's source code, copied under src/ (no vendor or deploy files)")
    p.add_argument("--max-attempts", type=int, help="deployment and designer attempts (default: the configured value)")
    p.add_argument("--operation-id", default="fuzzops-onboard", help="durable operation identity (new per attempt)")
    p.add_argument("--kind-cluster", default="kind", help="kind cluster whose nodes hold the preloaded app images")
    p.add_argument(
        "--watch-only", action="store_true", help="SDO detects and records incidents but never responds (frozen memory)"
    )
    p.add_argument(
        "--evidence-sink-host-path",
        default="",
        help="node directory (a kind extraMounts target) that keeps SDO's run evidence after the namespace is gone",
    )
    p.add_argument("--evidence-run-id", default="", help="run directory inside the evidence sink")
    p.add_argument(
        "--config",
        type=Path,
        action="append",
        default=[],
        help="operator config overlay file passed after sdo-bench.yaml (repeatable; docs/config.md)",
    )
    a = p.parse_args(argv)
    cfg = InstallConfig(
        pack=a.pack.resolve(),
        work=a.work.resolve(),
        namespace=a.namespace,
        control_namespace=a.control_namespace,
        objective=a.objective,
        controller_image=a.controller_image,
        responder_image=a.responder_image,
        operator_binary=a.operator_binary.resolve(),
        kubeconfig=a.kubeconfig.resolve(),
        auth_file=a.auth_file.resolve(),
        provider=a.provider,
        model=a.model,
        effort=a.effort,
        kind_cluster=a.kind_cluster,
        operation_id=a.operation_id,
        source=a.source.resolve() if a.source else None,
        max_attempts=a.max_attempts,
        watch_only=a.watch_only,
        evidence_sink_host_path=a.evidence_sink_host_path,
        evidence_run_id=a.evidence_run_id,
        config_files=tuple(path.resolve() for path in a.config),
    )
    cfg.work.mkdir(parents=True, exist_ok=True)
    return install(cfg, description=a.description)


if __name__ == "__main__":
    sys.exit(main())
