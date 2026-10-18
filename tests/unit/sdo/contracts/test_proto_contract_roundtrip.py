"""Stage 1 vertical slice of the proto single-source-of-truth track.

``sdo/operator/runtime/contract_proto_test.go`` encodes an ``ObjectRef`` through
the generated Go type and protojson, normalizes it, and writes
``tests/fixtures/sdo/contracts/proto/object_ref.json``. Here the generated
Python type parses that exact file, re-encodes it through the same canonical
protojson convention, and asserts the bytes are identical — the
Go-encode -> Python-decode -> re-encode -> identical round trip that makes a
schema drift between the two generated types a test failure.
"""

from __future__ import annotations

from pathlib import Path

from sdo.contracts.proto import ObjectRef, parse_json, to_canonical_json

PROTO_FIXTURES = Path(__file__).resolve().parents[3] / "fixtures" / "sdo" / "contracts" / "proto"


def test_object_ref_go_fixture_round_trips_through_python() -> None:
    go_encoded = (PROTO_FIXTURES / "object_ref.json").read_text(encoding="utf-8")

    decoded = parse_json(go_encoded, ObjectRef())
    assert decoded.api_version == "apps/v1"
    assert decoded.kind == "Deployment"
    assert decoded.namespace == "hotel-reservation"
    assert decoded.name == "frontend"

    reencoded = to_canonical_json(decoded)
    assert reencoded == go_encoded, "Python re-encoding diverged from the Go protojson fixture"


def test_empty_optional_fields_are_omitted() -> None:
    encoded = to_canonical_json(ObjectRef(kind="Service", name="api"))
    assert "api_version" not in encoded
    assert "namespace" not in encoded
    # Re-parsing yields the same canonical bytes.
    assert to_canonical_json(parse_json(encoded, ObjectRef())) == encoded
