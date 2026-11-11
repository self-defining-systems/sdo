from pathlib import Path

import pytest

from benchmarks.fuzzops.evidence import EvidenceTarget, snapshot_id_for


def test_evidence_target_builds_the_runtime_artifacts_command() -> None:
    target = EvidenceTarget(Path("/bin/sdo-operator"), "app-sdo", "sdo-controller:tag", Path("/k/config"))
    args = target.command("--mode", "snapshot", "--snapshot-id", "close-x")
    assert args[:2] == ["/bin/sdo-operator", "runtime-artifacts"]
    assert args[args.index("--control-namespace") + 1] == "app-sdo"
    assert args[args.index("--repository-pvc") + 1] == "sdo-application-repository"
    assert args[-2:] == ["--kubeconfig", "/k/config"]


def test_evidence_target_validates_paths() -> None:
    with pytest.raises(ValueError, match="absolute"):
        EvidenceTarget(Path("relative"), "app-sdo", "img")
    with pytest.raises(ValueError, match="control namespace"):
        EvidenceTarget(Path("/bin/sdo-operator"), "", "img")


def test_snapshot_ids_are_dns_labels() -> None:
    sid = snapshot_id_for("GitLab_Incident:1794364828514981859-" + "x" * 80)
    assert len(sid) <= 63
    assert sid.startswith("close-")
    assert all(c.isalnum() and c.islower() or c.isdigit() or c == "-" for c in sid)
