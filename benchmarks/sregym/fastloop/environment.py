"""The warm environment a fast loop runs against, persisted by ``fastloop up``."""

from __future__ import annotations

import json
import re
from collections.abc import MutableMapping  # noqa: TC003
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from benchmarks.sregym.fastloop.lane_installability import LaneTopology  # noqa: TC001 - Pydantic runtime schema.

ENVIRONMENT_FILENAME = "fastloop.json"
DEFAULT_PROBLEM = "missing_configmap_hotel_reservation"


class Images(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    controller: str = "sdo-controller:fastloop"
    responder: str = "sdo-sregym-responder:fastloop"


class FastloopEnvironment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lane_topology: LaneTopology | None = None
    image_delivery: str = "unknown"
    image_delivery_reason: str = ""
    image_delivery_seconds: float = 0
    image_delivery_images: list[str] = Field(default_factory=list)
    app: str = "sregym"
    run_dir: Path
    cluster: str
    kubeconfig: Path
    namespace: str
    application: str
    #: The problem's application. Shared attachment renames ``application`` per lane; this never changes.
    problem_application: str = ""
    description: str = ""
    workspace: Path
    sregym_dir: Path
    #: Private ``/tmp`` for the SREGym worker; ``None`` runs it unsandboxed.
    private_tmp: Path | None
    deploy_problem: str = DEFAULT_PROBLEM
    seed_commit: str | None = None
    dev_mount: Path | None = None
    dev_mount_snapshot: bool = False
    images: Images = Field(default_factory=Images)
    #: Controller, compiler and validator run from immutable digests with no dev mount, so the
    #: broker may reuse publication attestations. Node mounts (shared memory) stay; each row
    #: checks the rendered controller Job before injection.
    pinned_tools: bool = False
    #: Environment for the SREGym worker process (deploy-from-source, kind, builder settings).
    worker_env: dict[str, str] = Field(default_factory=dict)
    #: Optional host directory mounted on every kind node before shared attachment.
    parallel_root: Path | None = None
    parallel_lane: str = ""
    shared_accepted_id: str = ""
    #: GitHub repository and run branch the lane's accepted memory replicates (remote sharing).
    memory_repo: str = ""
    memory_branch: str = ""

    @field_validator("application")
    @classmethod
    def dns_application_name(cls, value: str) -> str:
        if not value:  # ``up`` has no application name until the worker returns app_info.
            return value
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        if not slug or len(slug) > 63:
            raise ValueError(f"application cannot be represented as a Kubernetes DNS label: {value!r}")
        return slug

    @model_validator(mode="after")
    def record_problem_application(self) -> FastloopEnvironment:
        # An unattached environment's application is the problem application. An attached one
        # (parallel_lane set) was renamed, so only an explicit record can say what it deploys.
        if not self.problem_application and not self.parallel_lane:
            self.problem_application = self.application
        return self

    @model_validator(mode="after")
    def pinned_tools_are_digests(self) -> FastloopEnvironment:
        from benchmarks.sregym.fastloop.pinned_tools import immutable_image

        if self.pinned_tools:
            for role in ("controller", "validator"):
                image = getattr(self.images, role)
                if not immutable_image(image):
                    raise ValueError(f"pinned_tools requires an immutable {role} digest, not {image!r}")
        return self

    @property
    def controller_dev_mount(self) -> bool:
        """Whether the controller (and the workers it launches) run the dev-mounted sources."""

        return self.dev_mount is not None and not self.pinned_tools

    def apply_operator_env(self, environ: MutableMapping[str, str]) -> None:
        """Point the host operator CLI at the lane's binary and its controller at the right sources."""

        if self.dev_mount is not None:
            environ["SDO_OPERATOR_BIN"] = str(self.dev_mount / ".fastloop/sdo-operator")
        if self.controller_dev_mount:
            environ["SDO_DEV_MOUNT"] = "1"
        else:
            environ.pop("SDO_DEV_MOUNT", None)

    @property
    def path(self) -> Path:
        return self.run_dir / ENVIRONMENT_FILENAME

    @property
    def state_path(self) -> Path:
        """The persistent controller registry; it outlives single ``run`` invocations."""

        return self.run_dir / "sdo_persistent_controller.json"

    @property
    def baseline_path(self) -> Path:
        return self.run_dir / "cluster_baseline_state.json"

    def save(self) -> Path:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(self.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.replace(self.path)
        return self.path

    @classmethod
    def load(cls, run_dir: Path) -> FastloopEnvironment:
        path = run_dir / ENVIRONMENT_FILENAME
        if not path.is_file():
            raise FileNotFoundError(f"{path} does not exist; run `fastloop up --run-dir {run_dir}` first")
        return cls.model_validate(json.loads(path.read_text(encoding="utf-8")))

    def worker_process_env(self) -> dict[str, str]:
        return {
            **self.worker_env,
            "KUBECONFIG": str(self.kubeconfig),
            "PYTHONUNBUFFERED": "1",
            "FASTLOOP_ORACLE_BASELINES": str(self.run_dir / "oracle-baselines.json"),
            "FASTLOOP_ORACLE_IDENTITY": json.dumps(
                [self.app, self.namespace, self.seed_commit, str(self.sregym_dir.resolve())]
            ),
        }
