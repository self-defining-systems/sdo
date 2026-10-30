from __future__ import annotations

from dataclasses import replace

import pytest

from benchmarks.sregym.fastloop.setup_proof import INVARIANTS, SetupFingerprint, save_receipt
from benchmarks.sregym.fastloop.warm_scope import check_cluster_scope


def fingerprint():
    return SetupFingerprint(
        lane="synthetic",
        code_revision="a" * 40,
        images={"controller": "synthetic"},
        accepted_memory_commit="b" * 40,
        installed_memory_commit="b" * 40,
        app_manifests="synthetic",
        tools={"fixture": "synthetic"},
        live_objects={"<cluster>/ClusterRole/base": "baseline", "app/Service/app": "service-before"},
    )


def test_setup_receipt_producer_binds_cluster_scope_while_memory_and_app_advance(tmp_path):
    before = fingerprint()
    report = {
        "passed": True,
        "fault_injected": False,
        "live_model_calls": False,
        "code_revision": before.code_revision,
        "lane": before.lane,
        "invariants": {name: {"passed": True} for name in INVARIANTS},
    }
    save_receipt(tmp_path, before, report)
    current = replace(
        before,
        accepted_memory_commit="c" * 40,
        installed_memory_commit="c" * 40,
        live_objects={**before.live_objects, "app/Service/app": "service-after"},
    )
    check_cluster_scope(tmp_path, current, "app")
    changed = replace(current, live_objects={**current.live_objects, "<cluster>/ClusterRole/base": "changed"})
    with pytest.raises(ValueError, match="cluster configuration"):
        check_cluster_scope(tmp_path, changed, "app")


@pytest.mark.parametrize("failure", ["missing", "extra", "deleted", "tampered"])
def test_incomplete_or_untrusted_cluster_proof_is_rejected(tmp_path, failure):
    current = fingerprint()
    if failure != "missing":
        report = {
            "passed": True,
            "fault_injected": False,
            "live_model_calls": False,
            "code_revision": current.code_revision,
            "lane": current.lane,
            "invariants": {name: {"passed": True} for name in INVARIANTS},
        }
        receipt = save_receipt(tmp_path, current, report)
        if failure == "tampered":
            receipt.write_text(receipt.read_text().replace("baseline", "tampered"))
        elif failure == "extra":
            current = replace(current, live_objects={**current.live_objects, "<cluster>/StorageClass/new": "new"})
        else:
            current = replace(current, live_objects={"app/Service/app": "service-before"})
    with pytest.raises(ValueError, match="cluster"):
        check_cluster_scope(tmp_path, current, "app")


@pytest.mark.parametrize("namespace", ["kube-system", "calico-system", "other-app"])
@pytest.mark.parametrize("change", ["edited", "deleted", "added"])
def test_retained_namespace_configuration_cannot_escape_scope(tmp_path, namespace, change):
    from benchmarks.sregym.fastloop.setup_proof import object_identities

    configuration = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"namespace": namespace, "name": "routing", "uid": "synthetic-infra"},
        "data": {"route": "accepted"},
    }
    before = replace(fingerprint(), live_objects={**fingerprint().live_objects, **object_identities([configuration])})
    report = {
        "passed": True,
        "fault_injected": False,
        "live_model_calls": False,
        "code_revision": before.code_revision,
        "lane": before.lane,
        "invariants": {name: {"passed": True} for name in INVARIANTS},
    }
    save_receipt(tmp_path, before, report)
    current_objects = dict(before.live_objects)
    if change == "deleted":
        del current_objects[f"{namespace}/ConfigMap/routing"]
    else:
        if change == "added":
            configuration["metadata"]["name"] = "unexpected"
        configuration["data"]["route"] = "fault"
        current_objects.update(object_identities([configuration]))
    with pytest.raises(ValueError, match="configuration"):
        check_cluster_scope(tmp_path, replace(before, live_objects=current_objects), "app")


@pytest.mark.parametrize(
    ("field", "value"),
    [("code_revision", "d" * 40), ("images", {"controller": "replaced"}), ("tools", {"fixture": "replaced"})],
)
def test_runtime_change_requires_new_no_fault_setup(tmp_path, field, value):
    before = fingerprint()
    report = {
        "passed": True,
        "fault_injected": False,
        "live_model_calls": False,
        "code_revision": before.code_revision,
        "lane": before.lane,
        "invariants": {name: {"passed": True} for name in INVARIANTS},
    }
    save_receipt(tmp_path, before, report)
    with pytest.raises(ValueError, match="setup"):
        check_cluster_scope(tmp_path, replace(before, **{field: value}), "app")
