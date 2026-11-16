"""Which protobuf contract changes a pool lane may take by a swap: additive ones, decided from the descriptors."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from benchmarks.fuzzops import contractdiff, devtree

F = descriptor_pb2.FieldDescriptorProto
Edit = Callable[[descriptor_pb2.FileDescriptorProto], None]


def _base() -> descriptor_pb2.FileDescriptorProto:
    file = descriptor_pb2.FileDescriptorProto(name="t/v1/a.proto", package="t.v1", syntax="proto3")
    enum = file.enum_type.add(name="Kind")
    enum.value.add(name="KIND_UNSPECIFIED", number=0)
    enum.value.add(name="KIND_A", number=1)
    message = file.message_type.add(name="Task")
    message.field.add(name="id", number=1, type=F.TYPE_STRING, label=F.LABEL_OPTIONAL, json_name="id")
    message.field.add(name="tags", number=2, type=F.TYPE_STRING, label=F.LABEL_REPEATED, json_name="tags")
    message.field.add(
        name="kind", number=3, type=F.TYPE_ENUM, label=F.LABEL_OPTIONAL, type_name=".t.v1.Kind", json_name="kind"
    )
    message.nested_type.add(name="Inner").field.add(
        name="x", number=1, type=F.TYPE_INT32, label=F.LABEL_OPTIONAL, json_name="x"
    )
    service = file.service.add(name="Worker")
    service.method.add(name="Run", input_type=".t.v1.Task", output_type=".t.v1.Task")
    return file


def _write(root: Path, file: descriptor_pb2.FileDescriptorProto) -> None:
    """A generated-module stand-in: the serialized descriptor in the call ``protoc`` output makes."""
    module = root / contractdiff.GEN_ROOT / "t/v1/a_pb2.py"
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(f"DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile({file.SerializeToString()!r})\n")


def _sets(edit: Edit, tmp_path: Path) -> tuple[descriptor_pb2.FileDescriptorSet, descriptor_pb2.FileDescriptorSet]:
    old, new = _base(), _base()
    edit(new)
    _write(tmp_path / "old", old)
    _write(tmp_path / "new", new)
    return contractdiff.from_directory(tmp_path / "old"), contractdiff.from_directory(tmp_path / "new")


def _add_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field.add(
        name="sample_interval", number=4, type=F.TYPE_STRING, label=F.LABEL_OPTIONAL, json_name="sampleInterval"
    )


def _add_message(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type.add(name="Extra").field.add(
        name="y", number=1, type=F.TYPE_BOOL, label=F.LABEL_OPTIONAL, json_name="y"
    )


def _add_enum_value(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.enum_type[0].value.add(name="KIND_B", number=2)


def _add_method(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.service[0].method.add(name="Stop", input_type=".t.v1.Task", output_type=".t.v1.Task")


def _add_nested_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].nested_type[0].field.add(
        name="z", number=2, type=F.TYPE_INT32, label=F.LABEL_OPTIONAL, json_name="z"
    )


@pytest.mark.parametrize("edit", [_add_field, _add_message, _add_enum_value, _add_method, _add_nested_field])
def test_additions_are_not_breaking(edit: Edit, tmp_path: Path) -> None:
    old, new = _sets(edit, tmp_path)
    assert contractdiff.breaking(old, new) == []


def test_identical_contracts_are_not_breaking(tmp_path: Path) -> None:
    old, new = _sets(lambda f: None, tmp_path)
    assert contractdiff.breaking(old, new) == []


def test_a_new_file_is_not_breaking(tmp_path: Path) -> None:
    old, new = _sets(lambda f: None, tmp_path)
    new.file.add(name="t/v1/b.proto", package="t.v1", syntax="proto3")
    assert contractdiff.breaking(old, new) == []


def _remove_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.message_type[0].field[1]


def _retype_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field[0].type = F.TYPE_INT64


def _renumber_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field[0].number = 9


def _rename_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field[0].name = "ident"


def _repeat_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field[0].label = F.LABEL_REPEATED


def _retarget_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field[2].type_name = ".t.v1.Other"


def _into_oneof(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].oneof_decl.add(name="choice")
    f.message_type[0].field[0].oneof_index = 0


def _remove_message(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.message_type[0]


def _remove_nested_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.message_type[0].nested_type[0].field[0]


def _remove_enum_value(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.enum_type[0].value[1]


def _rename_enum_value(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.enum_type[0].value[1].name = "KIND_Z"


def _remove_enum(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.enum_type[0]


def _remove_method(f: descriptor_pb2.FileDescriptorProto) -> None:
    del f.service[0].method[0]


def _restream_method(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.service[0].method[0].server_streaming = True


def _new_required_field(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.message_type[0].field.add(name="must", number=4, type=F.TYPE_STRING, label=F.LABEL_REQUIRED, json_name="must")


def _repackage(f: descriptor_pb2.FileDescriptorProto) -> None:
    f.package = "t.v2"


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        (_remove_field, "field removed"),
        (_retype_field, "type changed"),
        (_renumber_field, "field removed"),
        (_rename_field, "name changed"),
        (_repeat_field, "label changed"),
        (_retarget_field, "type name changed"),
        (_into_oneof, "oneof changed"),
        (_remove_message, "message removed"),
        (_remove_nested_field, "field removed"),
        (_remove_enum_value, "enum value removed"),
        (_rename_enum_value, "enum value renamed"),
        (_remove_enum, "enum removed"),
        (_remove_method, "method removed"),
        (_restream_method, "server_streaming changed"),
        (_new_required_field, "new required field"),
        (_repackage, "package changed"),
    ],
)
def test_incompatible_changes_are_breaking(edit: Edit, expected: str, tmp_path: Path) -> None:
    old, new = _sets(edit, tmp_path)
    assert any(expected in problem for problem in contractdiff.breaking(old, new))


def test_a_removed_file_is_breaking(tmp_path: Path) -> None:
    old, _new = _sets(lambda f: None, tmp_path)
    assert contractdiff.breaking(old, descriptor_pb2.FileDescriptorSet()) == ["t/v1/a.proto: file removed"]


def test_the_snapshot_round_trips(tmp_path: Path) -> None:
    old, _new = _sets(_add_field, tmp_path)
    assert contractdiff.decode(contractdiff.encode(old)) == old


def test_the_real_contracts_read_and_equal_themselves() -> None:
    root = Path(__file__).resolve().parents[4]
    files = contractdiff.from_directory(root)
    assert len(files.file) > 5
    assert contractdiff.breaking(files, files) == []


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args], check=True, capture_output=True
    )


def test_command_line_exit_codes_follow_git_revisions(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write(repo, _base())
    _git(tmp_path, "init", "-q", str(repo))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    added = _base()
    _add_field(added)
    _write(repo, added)
    _git(repo, "commit", "-qam", "additive")
    removed = _base()
    _remove_field(removed)
    _write(repo, removed)
    _git(repo, "commit", "-qam", "breaking")
    script = Path(contractdiff.__file__)

    def run(old: str, new: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(script), str(repo), old, new], capture_output=True, text=True, check=False
        )

    assert run("HEAD~2", "HEAD~1").returncode == 0
    breaking = run("HEAD~2", "HEAD")
    assert breaking.returncode == 1
    assert "field removed" in breaking.stdout
    assert run("HEAD~2", "nonexistent").returncode == 2


def _swap_repo(tmp_path: Path, edit: Edit) -> tuple[Path, dict[str, str], str]:
    """A checkout whose lane was built from the base contracts; the checkout's HEAD then carries ``edit``."""
    repo = tmp_path / "repo"
    _write(repo, _base())
    (repo / "sdo/operator").mkdir(parents=True)
    (repo / "sdo/operator/a.go").write_text("x\n")
    _git(tmp_path, "init", "-q", str(repo))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    lane, snapshot = devtree.fingerprint(repo), devtree.contracts_snapshot(repo)
    changed = _base()
    edit(changed)
    _write(repo, changed)
    (repo / "sdo/operator/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "change")
    return repo, lane, snapshot


def test_a_swap_accepts_an_added_contract_field_and_rebuilds_the_binary(tmp_path: Path) -> None:
    repo, lane, snapshot = _swap_repo(tmp_path, _add_field)
    assert devtree.swap_problem(repo, lane, rebuild_binary=True, contracts=snapshot) == ""
    assert devtree.binary_changed(repo, lane)


@pytest.mark.parametrize("edit", [_remove_field, _retype_field])
def test_a_swap_refuses_a_breaking_contract_change(edit: Edit, tmp_path: Path) -> None:
    repo, lane, snapshot = _swap_repo(tmp_path, edit)
    problem = devtree.swap_problem(repo, lane, rebuild_binary=True, contracts=snapshot)
    assert problem.startswith("breaking contract change")
    assert "Task.id" in problem or "Task.tags" in problem


def test_a_swap_refuses_a_contract_change_for_a_lane_without_descriptors(tmp_path: Path) -> None:
    repo, lane, _snapshot = _swap_repo(tmp_path, _add_field)
    assert "no contract descriptors" in devtree.swap_problem(repo, lane, rebuild_binary=True)
