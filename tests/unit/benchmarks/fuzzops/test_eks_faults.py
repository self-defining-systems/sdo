"""Which fuzzops faults an EKS lane can run: a fault whose steps include a node-level action cannot."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops import eks_faults

if TYPE_CHECKING:
    from pathlib import Path

ACTION_GO = """package {pkg}

func (a *act) Spec() action.Spec {{
	return action.Spec{{
		Name:     "{name}",
		Describe: "synthetic",
		Blast:    action.BlastPod,
{node}	}}
}}
"""


def _fuzzops(tmp_path: Path, *, mirror: bool = False) -> Path:
    """A synthetic checkout: two plain actions, one node-level, and faults using them."""
    root = tmp_path / "fuzzops"
    for pkg, name, node in (
        ("patchfield", "patch-field", ""),
        ("throttle", "cgroup-throttle", "\t\tNodeLevel:   true,\n"),
        (
            "builder",
            "build-and-deploy-patched-image",
            "\t\tNodeLevel:   true,\n" + ("\t\tImageMirror: true,\n" if mirror else ""),
        ),
    ):
        (root / "internal" / "actions" / pkg).mkdir(parents=True)
        (root / "internal" / "actions" / pkg / f"{pkg}.go").write_text(ACTION_GO.format(pkg=pkg, name=name, node=node))
    faults = root / "examples" / "demo" / "faults"
    faults.mkdir(parents=True)
    (faults / "plain-fault.yaml").write_text("name: plain-fault\nsteps:\n  - action: patch-field\n")
    (faults / "slow-fault.yaml").write_text(
        "name: slow-fault\nsteps:\n  - action: patch-field\n  - action: cgroup-throttle\n    args: {}\n"
    )
    (faults / "built-fault.yaml").write_text("action: build-and-deploy-patched-image\n")
    (root / "templates").mkdir()
    (root / "templates" / "shared-fault.yaml").write_text("steps:\n- action: patch-field\n")
    return root


def test_node_level_actions_are_read_from_the_checkout(tmp_path: Path) -> None:
    assert eks_faults.node_level_actions(_fuzzops(tmp_path)) == {"cgroup-throttle", "build-and-deploy-patched-image"}


def test_a_fault_with_a_node_level_step_is_excluded_with_its_action_named(tmp_path: Path) -> None:
    verdicts = eks_faults.classify(
        _fuzzops(tmp_path),
        ["plain-fault@process:deploy/x/c?n=1", "slow-fault@process:deploy/y/c", "built-fault@delivery:deploy/z/c"],
    )
    assert verdicts["plain-fault"].ok
    assert not verdicts["slow-fault"].ok
    assert verdicts["slow-fault"].actions == ("cgroup-throttle",)
    assert "node" in verdicts["slow-fault"].reason
    assert not verdicts["built-fault"].ok
    assert "patched image" in verdicts["built-fault"].reason


def test_a_template_in_the_shared_catalog_is_found(tmp_path: Path) -> None:
    assert eks_faults.classify(_fuzzops(tmp_path), ["shared-fault@entry:svc/a"])["shared-fault"].ok


def test_an_unknown_template_is_an_error_not_a_pass(tmp_path: Path) -> None:
    with pytest.raises(eks_faults.FaultSpecError, match="no-such"):
        eks_faults.classify(_fuzzops(tmp_path), ["no-such@x"])


def test_filter_keeps_instance_ids_of_ok_templates_in_order(tmp_path: Path) -> None:
    ids = ["slow-fault@a", "plain-fault@b", "shared-fault@c", "built-fault@d"]
    assert eks_faults.keep_eks_ok(_fuzzops(tmp_path), ids) == ["plain-fault@b", "shared-fault@c"]


def test_the_matrix_file_carries_each_family_and_the_revision(tmp_path: Path) -> None:
    root = _fuzzops(tmp_path)
    points = tmp_path / "points.tsv"
    points.write_text(
        "# app\tfamily\tshort\tA_id\tB_id\tB_kind\n"
        "demo\tplain-fault\tdp\tplain-fault@a\tplain-fault@b\tmild\n"
        "demo\tslow-fault\tds\tslow-fault@a\tslow-fault@b\tmild\n"
    )
    matrix = eks_faults.matrix(root, eks_faults.read_points(points), revision="abc1234")
    assert matrix["schema"] == "fuzzops.eks-faults/v1"
    assert matrix["fuzzops_rev"] == "abc1234"
    assert matrix["families"]["dp"] == {"app": "demo", "template": "plain-fault", "eks": True, "reason": ""}
    assert matrix["families"]["ds"]["eks"] is False
    assert matrix["families"]["ds"]["actions"] == ["cgroup-throttle"]
    json.dumps(matrix)


def test_a_node_level_action_that_only_delivers_an_image_runs_on_eks(tmp_path: Path) -> None:
    root = _fuzzops(tmp_path, mirror=True)
    assert eks_faults.node_level_actions(root) == {"cgroup-throttle"}
    verdicts = eks_faults.classify(root, ["built-fault@d", "slow-fault@x"])
    assert verdicts["built-fault"].ok
    assert not verdicts["slow-fault"].ok
