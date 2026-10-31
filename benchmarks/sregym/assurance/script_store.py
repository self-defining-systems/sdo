"""Where the scripted Codex CLI reads its script and records its turns.

In a pod the store is the controller namespace, reached with the pod's service
account (the responder may get and create ConfigMaps there; the controller
pod may too). Outside a pod, ``SDO_SCRIPTED_STATE_DIR`` names a directory with
the same layout, for unit tests.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

SCRIPTED_LABEL = "sdo.dev/agent-script"
TURN_KEY = "turn.json"


@dataclass(frozen=True)
class ScriptBundle:
    cold: str
    warm: str
    seed: int

    def __post_init__(self) -> None:
        if not isinstance(self.cold, str) or not isinstance(self.warm, str):
            raise TypeError("script bundle contents must be strings")
        if not isinstance(self.seed, int) or isinstance(self.seed, bool):
            raise TypeError("usage seed must be an integer")
        if self.seed < 0:
            raise ValueError("usage seed must not be negative")

    def to_json(self) -> str:
        return json.dumps({"cold": self.cold, "warm": self.warm, "seed": self.seed})

    @classmethod
    def from_json(cls, text: str) -> ScriptBundle:
        return cls(**json.loads(text))


SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
STATE_DIR_ENV = "SDO_SCRIPTED_STATE_DIR"


class StoreError(RuntimeError):
    """The script or turn store could not be read or written."""


def binding_name(incident_id: str) -> str:
    return "sdo-scripted-bind-" + hashlib.sha256(incident_id.encode()).hexdigest()[:16]


def turn_name(turn_id: str) -> str:
    return "sdo-scripted-turn-" + hashlib.sha256(turn_id.encode()).hexdigest()[:16]


class Store(Protocol):
    def script(self) -> ScriptBundle: ...

    def binding(self, incident_id: str) -> ScriptBundle | None: ...

    def bind(self, incident_id: str, binding: ScriptBundle) -> ScriptBundle: ...

    def record_turn(self, turn_id: str, record: dict[str, object]) -> None: ...

    def claim(self, marker: str) -> bool:
        """Create ``marker`` once; True only for the caller that created it (a one-shot fault)."""
        ...


def marker_name(marker: str) -> str:
    return "sdo-scripted-mark-" + hashlib.sha256(marker.encode()).hexdigest()[:16]


class DirectoryStore:
    """A directory with ``bundle.json``, ``bindings/`` and ``turns/``."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def script(self) -> ScriptBundle:
        data = json.loads((self.root / "bundle.json").read_text())
        return ScriptBundle(**data)

    def binding(self, incident_id: str) -> ScriptBundle | None:
        path = self.root / "bindings" / f"{binding_name(incident_id)}.json"
        return ScriptBundle.from_json(path.read_text(encoding="utf-8")) if path.is_file() else None

    def bind(self, incident_id: str, binding: ScriptBundle) -> ScriptBundle:
        existing = self.binding(incident_id)
        if existing is not None:
            return existing
        path = self.root / "bindings" / f"{binding_name(incident_id)}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(binding.to_json(), encoding="utf-8")
        return binding

    def record_turn(self, turn_id: str, record: dict[str, object]) -> None:
        path = self.root / "turns" / f"{turn_name(turn_id)}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, sort_keys=True), encoding="utf-8")

    def claim(self, marker: str) -> bool:
        path = self.root / "marks" / marker_name(marker)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.open("x").close()
        except FileExistsError:
            return False
        return True


class ConfigMapStore:
    """ConfigMaps in the pod's own namespace, through the Kubernetes API."""

    def __init__(self, *, namespace: str, base_url: str, token: str, ca_file: Path) -> None:
        self.namespace = namespace
        self.base_url = base_url
        self._token = token
        self._context = ssl.create_default_context(cafile=str(ca_file))

    @classmethod
    def in_cluster(cls) -> ConfigMapStore:
        token_file = SERVICE_ACCOUNT_ROOT / "token"
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
        if not token_file.is_file() or not host:
            raise StoreError("not in a pod: no service-account token or KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        # SDO_NAMESPACE is the controller namespace in a responder Job; the
        # service account's own namespace is the same one in the controller pod.
        namespace = os.environ.get("SDO_NAMESPACE", "").strip() or (
            (SERVICE_ACCOUNT_ROOT / "namespace").read_text(encoding="utf-8").strip()
        )
        return cls(
            namespace=namespace,
            base_url=f"https://{host}:{port}",
            token=token_file.read_text(encoding="utf-8").strip(),
            ca_file=SERVICE_ACCOUNT_ROOT / "ca.crt",
        )

    def _request(self, method: str, path: str, body: dict[str, object] | None = None) -> dict[str, object]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"{self.base_url}/api/v1/namespaces/{self.namespace}/configmaps{path}",
            data=data,
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(request, context=self._context, timeout=30) as response:
            payload = json.loads(response.read())
        return payload if isinstance(payload, dict) else {}

    def _get(self, name: str) -> dict[str, object] | None:
        try:
            return self._request("GET", f"/{name}")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise StoreError(f"GET configmap {name}: HTTP {exc.code}") from exc
        except (urllib.error.URLError, OSError) as exc:
            raise StoreError(f"GET configmap {name}: {exc}") from exc

    def _create(self, name: str, key: str, value: str, labels: dict[str, str]) -> bool:
        body: dict[str, object] = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": self.namespace, "labels": labels},
            "data": {key: value},
        }
        try:
            self._request("POST", "", body)
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                return False
            raise StoreError(f"POST configmap {name}: HTTP {exc.code}") from exc
        except (urllib.error.URLError, OSError) as exc:
            raise StoreError(f"POST configmap {name}: {exc}") from exc
        return True

    @staticmethod
    def _data(configmap: dict[str, object], key: str) -> str:
        data = configmap.get("data")
        value = data.get(key) if isinstance(data, dict) else None
        if not isinstance(value, str):
            raise StoreError(f"configmap has no {key!r}")
        return value

    def script(self) -> ScriptBundle:
        configmap = self._get("sdo-agent-script")
        if configmap is None:
            raise StoreError("missing sdo-agent-script ConfigMap")
        return ScriptBundle(
            self._data(configmap, "script.json"),
            self._data(configmap, "warm.json"),
            int(self._data(configmap, "usage_seed")),
        )

    def binding(self, incident_id: str) -> ScriptBundle | None:
        configmap = self._get(binding_name(incident_id))
        return None if configmap is None else ScriptBundle.from_json(self._data(configmap, "binding.json"))

    def bind(self, incident_id: str, binding: ScriptBundle) -> ScriptBundle:
        labels = {SCRIPTED_LABEL: "binding"}
        if self._create(binding_name(incident_id), "binding.json", binding.to_json(), labels):
            return binding
        existing = self.binding(incident_id)
        if existing is None:
            raise StoreError(f"binding for {incident_id!r} vanished after a conflict")
        return existing

    def record_turn(self, turn_id: str, record: dict[str, object]) -> None:
        self._create(turn_name(turn_id), TURN_KEY, json.dumps(record, sort_keys=True), {SCRIPTED_LABEL: "turn"})

    def claim(self, marker: str) -> bool:
        return self._create(marker_name(marker), "marker", marker, {SCRIPTED_LABEL: "marker"})


def default_store() -> Store:
    directory = os.environ.get(STATE_DIR_ENV, "").strip()
    if directory:
        return DirectoryStore(Path(directory))
    return ConfigMapStore.in_cluster()
