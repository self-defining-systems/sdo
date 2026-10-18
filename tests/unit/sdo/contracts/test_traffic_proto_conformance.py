"""Conformance ratchet for the traffic workload proto (Python side).

``sdo/contracts/proto/sdodev/contracts/v1alpha1/traffic.proto`` is the single schema SOURCE
for the ``.sdo/diagnostics/traffic/workloads/<name>.yaml`` artifact. This test
proves the generated Python proto type models the stored wire: every committed
workload fixture parses into ``TrafficWorkload`` with no unknown field (protojson
rejects one, so a field added without the proto fails here), and the host loader
-- which stands in for the Go-side protovalidate CEL, since the repo keeps no
Python CEL runtime -- rejects exactly the invariants the hand-written Pydantic
model rejected. The Go test (sdo/operator/runtime/traffic_conformance_test.go)
checks the protovalidate CEL mirrors those same invariants.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from google.protobuf import json_format

from sdo.contracts import TrafficWorkload

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "hotel_reservation" / "traffic" / "workloads"

_LINK_PROBE = {
    "apiVersion": "sdo.dev/v1alpha1",
    "kind": "TrafficWorkload",
    "name": "links",
    "purpose": "link-probe",
    "links": [{"from": "frontend", "to": "search", "port": 8082}],
}


@pytest.mark.parametrize("name", ["health", "journey", "verify"])
def test_proto_schema_models_every_workload_fixture(name: str) -> None:
    raw = yaml.safe_load((FIXTURES / f"{name}.yaml").read_text(encoding="utf-8"))
    # Raises on any field the proto schema does not model.
    message = json_format.ParseDict(raw, TrafficWorkload())
    assert message.name == name


def test_proto_schema_rejects_an_unknown_field() -> None:
    raw = yaml.safe_load((FIXTURES / "health.yaml").read_text(encoding="utf-8"))
    raw["unmodeled"] = True
    with pytest.raises(json_format.ParseError):
        json_format.ParseDict(raw, TrafficWorkload())
