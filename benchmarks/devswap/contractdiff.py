"""Whether a protobuf contract change is additive, read from the descriptors (not from text).

A pool lane is built at one revision of ``sdo/contracts``. A later revision whose contracts only *add* (fields,
messages, enum values, services, methods, files) runs on the same lane by a swap: the Go operator binary is rebuilt
from the new sources and the Python ``_gen`` modules are projected like the rest of ``sdo/``. Anything that could make
data the lane already holds unreadable, or change what an old peer sends, is breaking and the lane takes the full build.

The descriptors are the serialized ``FileDescriptorProto`` bytes embedded in every generated ``*_pb2.py``
(``AddSerializedFile``). ``buf generate`` output is checked against the ``.proto`` sources by
``scripts/check_proto.sh``, so the generated modules are the contract. Nothing here imports the generated modules, so
two revisions can be compared in one process.

Breaking, per message field number: removal; a different name, scalar/message/enum type, referenced type name, label
(repeated/optional), proto3-optional flag, containing oneof or JSON name. Per enum value number: removal or rename.
Removal of a message, enum, service, method or file; a different method input, output or streaming mode; a new
required field; a changed package or syntax. Option changes (validation rules, deprecation) are not wire changes.

Command line: ``contractdiff.py REPO OLD_REV NEW_REV`` exits 0 when ``NEW_REV`` is additive over ``OLD_REV`` (or
equal), 1 and prints each breaking change otherwise, 2 on an error.
"""

from __future__ import annotations

import ast
import base64
import subprocess
import sys
import zlib
from pathlib import Path

from google.protobuf import descriptor_pb2

GEN_ROOT = "sdo/contracts/_gen"
_FieldProto = descriptor_pb2.FieldDescriptorProto


def _serialized(source: str) -> bytes | None:
    """The bytes given to ``AddSerializedFile`` in a generated module's source, or ``None`` for a module without one."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "AddSerializedFile" and node.args:
            value = ast.literal_eval(node.args[0])
            if isinstance(value, bytes):
                return value
    return None


def _set_from_sources(sources: dict[str, str]) -> descriptor_pb2.FileDescriptorSet:
    files = descriptor_pb2.FileDescriptorSet()
    for _name, source in sorted(sources.items()):
        raw = _serialized(source)
        if raw is not None:
            files.file.add().ParseFromString(raw)
    return files


def from_directory(root: Path) -> descriptor_pb2.FileDescriptorSet:
    """The descriptors of the generated Python modules under ``root/sdo/contracts/_gen``."""
    gen = root / GEN_ROOT
    return _set_from_sources({str(p): p.read_text() for p in gen.rglob("*_pb2.py")})


def from_git(repo: Path, revision: str) -> descriptor_pb2.FileDescriptorSet:
    """The same, at ``revision`` of the Git repository ``repo``."""

    def git(*args: str) -> str:
        proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)}: {proc.stderr.strip()}")
        return proc.stdout

    names = [
        n for n in git("ls-tree", "-r", "--name-only", revision, "--", GEN_ROOT).splitlines() if n.endswith("_pb2.py")
    ]
    return _set_from_sources({n: git("show", f"{revision}:{n}") for n in names})


def encode(files: descriptor_pb2.FileDescriptorSet) -> str:
    """A text form of a descriptor set to keep in a lane's prepared record."""
    return base64.b64encode(zlib.compress(files.SerializeToString(deterministic=True))).decode()


def decode(text: str) -> descriptor_pb2.FileDescriptorSet:
    files = descriptor_pb2.FileDescriptorSet()
    files.ParseFromString(zlib.decompress(base64.b64decode(text)))
    return files


def _field_shape(field: _FieldProto, oneofs: list[str]) -> tuple[object, ...]:
    oneof = oneofs[field.oneof_index] if field.HasField("oneof_index") else ""
    return (
        field.name,
        field.type,
        field.label,
        field.type_name,
        field.proto3_optional,
        oneof,
        field.json_name,
    )


def _compare_enum(
    old: descriptor_pb2.EnumDescriptorProto, new: descriptor_pb2.EnumDescriptorProto, path: str
) -> list[str]:
    found = {v.number: v.name for v in new.value}
    problems = []
    for value in old.value:
        if value.number not in found:
            problems.append(f"{path}.{value.name} = {value.number}: enum value removed")
        elif found[value.number] != value.name:
            problems.append(f"{path} = {value.number}: enum value renamed {value.name} -> {found[value.number]}")
    return problems


def _compare_message(old: descriptor_pb2.DescriptorProto, new: descriptor_pb2.DescriptorProto, path: str) -> list[str]:
    problems = []
    old_oneofs = [o.name for o in old.oneof_decl]
    new_oneofs = [o.name for o in new.oneof_decl]
    fresh = {f.number: f for f in new.field}
    for field in old.field:
        where = f"{path}.{field.name} (#{field.number})"
        if field.number not in fresh:
            problems.append(f"{where}: field removed")
            continue
        was, now = _field_shape(field, old_oneofs), _field_shape(fresh[field.number], new_oneofs)
        names = ("name", "type", "label", "type name", "proto3 optional", "oneof", "json name")
        problems += [f"{where}: {n} changed {a!r} -> {b!r}" for n, a, b in zip(names, was, now, strict=True) if a != b]
    known = {f.number for f in old.field}
    problems += [
        f"{path}.{field.name} (#{field.number}): new required field"
        for field in new.field
        if field.number not in known and field.label == _FieldProto.LABEL_REQUIRED
    ]
    problems += _compare_nested(old.nested_type, new.nested_type, old.enum_type, new.enum_type, path)
    return problems


def _compare_nested(old_msgs, new_msgs, old_enums, new_enums, path: str) -> list[str]:
    problems = []
    messages = {m.name: m for m in new_msgs}
    for message in old_msgs:
        if message.name not in messages:
            problems.append(f"{path}.{message.name}: message removed")
        else:
            problems += _compare_message(message, messages[message.name], f"{path}.{message.name}")
    enums = {e.name: e for e in new_enums}
    for enum in old_enums:
        if enum.name not in enums:
            problems.append(f"{path}.{enum.name}: enum removed")
        else:
            problems += _compare_enum(enum, enums[enum.name], f"{path}.{enum.name}")
    return problems


def _compare_service(
    old: descriptor_pb2.ServiceDescriptorProto, new: descriptor_pb2.ServiceDescriptorProto, path: str
) -> list[str]:
    methods = {m.name: m for m in new.method}
    problems = []
    for method in old.method:
        where = f"{path}.{method.name}"
        if method.name not in methods:
            problems.append(f"{where}: method removed")
            continue
        now = methods[method.name]
        problems += [
            f"{where}: {n} changed {getattr(method, n)!r} -> {getattr(now, n)!r}"
            for n in ("input_type", "output_type", "client_streaming", "server_streaming")
            if getattr(method, n) != getattr(now, n)
        ]
    return problems


def breaking(old: descriptor_pb2.FileDescriptorSet, new: descriptor_pb2.FileDescriptorSet) -> list[str]:
    """Each way ``new`` is not an additive extension of ``old``; empty when it is (or they are equal)."""
    files = {f.name: f for f in new.file}
    problems = []
    for was in old.file:
        now = files.get(was.name)
        if now is None:
            problems.append(f"{was.name}: file removed")
            continue
        if now.package != was.package:
            problems.append(f"{was.name}: package changed {was.package!r} -> {now.package!r}")
        if now.syntax != was.syntax:
            problems.append(f"{was.name}: syntax changed {was.syntax!r} -> {now.syntax!r}")
        package = f"{was.name}:{was.package}"
        problems += _compare_nested(was.message_type, now.message_type, was.enum_type, now.enum_type, package)
        services = {s.name: s for s in now.service}
        for service in was.service:
            if service.name not in services:
                problems.append(f"{package}.{service.name}: service removed")
            else:
                problems += _compare_service(service, services[service.name], f"{package}.{service.name}")
    return problems


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    repo, old, new = argv[1], argv[2], argv[3]
    try:
        before = from_git(Path(repo), old)
        if not before.file:
            raise ValueError(f"no generated contracts at {old}")
        problems = breaking(before, from_git(Path(repo), new))
    except (RuntimeError, SyntaxError, ValueError) as err:
        print(f"contractdiff: {err}", file=sys.stderr)
        return 2
    for problem in problems:
        print(problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
