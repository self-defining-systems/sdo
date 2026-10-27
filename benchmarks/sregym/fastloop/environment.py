"""The warm environment a fast loop runs against, persisted by ``fastloop up``."""

from __future__ import annotations

import json
import re
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.

from pydantic import BaseModel, ConfigDict, Field, field_validator

ENVIRONMENT_FILENAME = "fastloop.json"
DEFAULT_PROBLEM = "missing_configmap_hotel_reservation"


class Images(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    controller: str = "sdo-controller:fastloop"
    responder: str = "sdo-sregym-responder:fastloop"
    validator: str = "sdo-detector-validator:fastloop"


class FastloopEnvironment(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    #: Environment for the SREGym worker process (deploy-from-source, kind, builder settings).
    worker_env: dict[str, str] = Field(default_factory=dict)
    #: Optional host directory mounted on every kind node before shared attachment.
    parallel_root: Path | None = None
    parallel_lane: str = ""
    shared_accepted_id: str = ""

    @field_validator("application")
    @classmethod
    def dns_application_name(cls, value: str) -> str:
        if not value:  # ``up`` has no application name until the worker returns app_info.
            return value
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        if not slug or len(slug) > 63:
            raise ValueError(f"application cannot be represented as a Kubernetes DNS label: {value!r}")
        return slug

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
