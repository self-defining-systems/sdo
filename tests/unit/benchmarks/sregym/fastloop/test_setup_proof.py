"""Setup reuse is exact-state-bound; live health is never cached."""

import json
import time
from dataclasses import replace

import pytest

from benchmarks.sregym.fastloop import setup_proof as proof


@pytest.fixture
def fingerprint():
    return proof.SetupFingerprint(
        lane="fixture",
        code_revision="a" * 40,
        images={"controller": {"node": "sha256:" + "1" * 64}},
        accepted_memory_commit="b" * 40,
        installed_memory_commit="b" * 40,
        app_manifests="manifest-hash",
        live_objects={"app/Service/web": "uid-spec-hash"},
        tools={"dry_run.py": "tool-hash"},
    )


def save(tmp_path, fingerprint):
    report = {
        "passed": True,
        "fault_injected": False,
        "live_model_calls": False,
        "invariants": {"traffic": {"passed": True}},
    }
    return proof.save_receipt(tmp_path, fingerprint, report)


def test_same_fingerprint_reuses_original_passing_receipt(tmp_path, fingerprint):
    path = save(tmp_path, fingerprint)
    decision = proof.reuse_decision(tmp_path, fingerprint)
    assert decision == {"reused": True, "receipt": str(path), "changed": []}
    receipt = json.loads(path.read_text())
    assert receipt["fingerprint"]["images"] == fingerprint.images
    assert receipt["fingerprint"]["accepted_memory_commit"] == fingerprint.accepted_memory_commit


@pytest.mark.parametrize(
    "field,value",
    [
        ("code_revision", "c" * 40),
        ("images", {"controller": {"node": "sha256:" + "2" * 64}}),
        ("accepted_memory_commit", "c" * 40),
        ("installed_memory_commit", "c" * 40),
        ("app_manifests", "changed-manifest"),
        ("live_objects", {"app/Service/web": "same-uid-drifted-port"}),
        ("tools", {"dry_run.py": "changed"}),
    ],
)
def test_changed_component_requires_full_proof_and_names_change(tmp_path, fingerprint, field, value):
    save(tmp_path, fingerprint)
    decision = proof.reuse_decision(tmp_path, replace(fingerprint, **{field: value}))
    assert decision["reused"] is False
    assert field in decision["changed"]


def test_reset_with_recreated_object_cannot_reuse(tmp_path, fingerprint):
    save(tmp_path, fingerprint)
    changed = replace(fingerprint, live_objects={"app/Service/web": "new-uid-same-spec"})
    assert not proof.reuse_decision(tmp_path, changed)["reused"]


def test_corrupt_or_failed_receipt_never_reuses(tmp_path, fingerprint):
    path = save(tmp_path, fingerprint)
    receipt = json.loads(path.read_text())
    receipt["report"]["passed"] = False
    path.write_text(json.dumps(receipt))
    assert not proof.reuse_decision(tmp_path, fingerprint)["reused"]


def test_uncommitted_code_cannot_be_certified(tmp_path, fingerprint):
    with pytest.raises(ValueError, match="committed"):
        save(tmp_path, replace(fingerprint, code_revision="a-dirty"))


def test_recorded_lane_fingerprint_is_fast(fingerprint):
    objects = [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"namespace": "app", "name": f"web-{i}", "uid": str(i)},
            "spec": {"ports": [{"port": 80, "targetPort": 8080}]},
        }
        for i in range(1000)
    ]
    started = time.monotonic()
    identities = proof.object_identities(objects)
    assert len(identities) == 1000
    assert time.monotonic() - started < 15


def test_status_and_resource_version_churn_preserve_identity():
    obj = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"namespace": "app", "name": "web", "uid": "1"},
        "spec": {"ports": [{"port": 80}]},
    }
    before = proof.object_identities([obj])
    obj["status"] = {"loadBalancer": {}}
    obj["metadata"]["resourceVersion"] = "22"
    assert proof.object_identities([obj]) == before
    obj["spec"]["ports"][0]["port"] = 81
    assert proof.object_identities([obj]) != before
