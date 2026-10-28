"""Build and verify the test CLI using all inputs selected by the Go toolchain."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

# Include non-Go inputs (especially go:embed) and local replace dependencies.
_SOURCE_FIELDS = (
    "GoFiles",
    "CgoFiles",
    "CFiles",
    "CXXFiles",
    "MFiles",
    "HFiles",
    "FFiles",
    "SFiles",
    "SwigFiles",
    "SwigCXXFiles",
    "SysoFiles",
    "EmbedFiles",
)
_BUILD_ENV = (
    "GOVERSION",
    "GOOS",
    "GOARCH",
    "GOAMD64",
    "GOARM",
    "GOARM64",
    "GO386",
    "GOMIPS",
    "GOMIPS64",
    "GOPPC64",
    "GORISCV64",
    "GOEXPERIMENT",
    "CGO_ENABLED",
    "CC",
    "CXX",
    "CGO_CFLAGS",
    "CGO_CPPFLAGS",
    "CGO_CXXFLAGS",
    "CGO_FFLAGS",
    "CGO_LDFLAGS",
    "GOFLAGS",
    "GOWORK",
)


def _go_json_objects(output: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    objects = []
    remaining = output.lstrip()
    while remaining:
        value, end = decoder.raw_decode(remaining)
        objects.append(value)
        remaining = remaining[end:].lstrip()
    return objects


def source_identity(module: Path) -> str:
    """Hash selected source bytes, module metadata and effective build settings."""
    environment = subprocess.check_output(["go", "env", "-json", *_BUILD_ENV], cwd=module, text=True)
    packages = _go_json_objects(
        subprocess.check_output(
            ["go", "list", "-mod=readonly", "-deps", "-json", "./cmd/sdo-operator"],
            cwd=module,
            text=True,
        )
    )
    digest = hashlib.sha256(b"sdo-test-cli-v1\0-buildvcs=false\0-mod=readonly\0")

    def add(label: str, content: bytes) -> None:
        # Length framing avoids ambiguity between paths and file contents.
        for item in (label.encode(), content):
            digest.update(len(item).to_bytes(8, "big"))
            digest.update(item)

    settings = json.loads(environment)
    # The link-cache wrapper serves equal links; it never changes the binary.
    settings["GOFLAGS"] = " ".join(f for f in settings.get("GOFLAGS", "").split() if not f.startswith("-toolexec="))
    # Workspace paths differ across checkouts; hash workspace contents instead.
    workspace = settings.pop("GOWORK", "")
    add("build environment", json.dumps(settings, sort_keys=True).encode())
    if workspace and workspace != "off":
        add("go.work", Path(workspace).read_bytes())
        workspace_sum = Path(workspace + ".sum")
        if workspace_sum.is_file():
            add("go.work.sum", workspace_sum.read_bytes())
    for package in sorted(packages, key=lambda value: value["ImportPath"]):
        if package.get("Goroot"):
            continue  # GOVERSION and build settings identify the standard library.
        directory = Path(package["Dir"])
        identity = package["ImportPath"]
        files = {name for field in _SOURCE_FIELDS for name in package.get(field, [])}
        for name in sorted(files):
            add(f"{identity}/{name}", (directory / name).read_bytes())
        metadata = package.get("Module", {})
        module_file = metadata.get("Replace", metadata).get("GoMod")
        if module_file:
            path = Path(module_file)
            add(f"{metadata['Path']}/go.mod", path.read_bytes())
            sums = path.with_name("go.sum")
            if sums.is_file():
                add(f"{metadata['Path']}/go.sum", sums.read_bytes())
    return digest.hexdigest()


def verify_operator(binary: Path, expected: str) -> None:
    """Fail before any tests if a cached executable came from other inputs."""
    try:
        result = subprocess.run([str(binary), "source-hash"], capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"operator source identity mismatch: {binary}: {exc}") from exc
    actual = result.stdout.strip()
    if result.returncode or actual != expected:
        raise RuntimeError(f"operator source identity mismatch: {binary}: expected {expected}, got {actual!r}")


def build_operator(module: Path, directory: Path) -> Path:
    expected = source_identity(module)
    directory.mkdir(parents=True, exist_ok=True)
    binary = directory / f"sdo-operator-{expected}"
    with (directory / "build.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not binary.is_file():
            with tempfile.TemporaryDirectory(prefix="build-", dir=directory) as temporary:
                staged = Path(temporary) / "sdo-operator"
                subprocess.run(
                    [
                        "go",
                        "build",
                        "-mod=readonly",
                        "-buildvcs=false",
                        "-ldflags",
                        f"-X main.sourceHash={expected}",
                        "-o",
                        str(staged),
                        "./cmd/sdo-operator",
                    ],
                    cwd=module,
                    check=True,
                )
                verify_operator(staged, expected)
                if source_identity(module) != expected:
                    raise RuntimeError("operator source inputs changed during build")
                staged.replace(binary)
        verify_operator(binary, expected)
    return binary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path, help="verify an existing binary against this checkout")
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    module = repository / "sdo/operator"
    if arguments.check:
        verify_operator(arguments.check, source_identity(module))
    else:
        print(build_operator(module, repository / ".fastloop/test-bin"))


if __name__ == "__main__":
    main()
