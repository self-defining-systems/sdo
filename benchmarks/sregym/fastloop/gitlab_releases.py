"""The GitLab chart releases the release replay pushes, as data (``apps/gitlab/releases.json``).

Each release records the upstream archive (URL, sha256) and what it changes relative to the release before it,
measured by rendering both through the lane's umbrella chart. The list is benchmark data: SDO never reads it, and the
replay only compares what SDO did with what the list says the release changed.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from apps.gitlab import assemble

if TYPE_CHECKING:
    from pathlib import Path

RELEASES_PATH = assemble.APP / "releases.json"
_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class Changes:
    """What a release changes in the lane's rendered output relative to the release before it."""

    images_changed: int
    values_keys_added: int
    values_keys_removed: int
    values_schema: bool
    workloads_added: tuple[str, ...]
    workloads_removed: tuple[str, ...]
    objects: int

    def __post_init__(self) -> None:
        if min(self.images_changed, self.values_keys_added, self.values_keys_removed, self.objects) < 0:
            raise ValueError("release change counts must not be negative")

    @property
    def changes_images(self) -> bool:
        return self.images_changed > 0

    @property
    def changes_shape(self) -> bool:
        """Workloads appear or disappear: the designer's concern (new or removed components)."""
        return bool(self.workloads_added or self.workloads_removed)

    @property
    def breaks_values(self) -> bool:
        """Values the lane may set were removed or are now schema-checked: the deployer's concern."""
        return self.values_keys_removed > 0 or self.values_schema


@dataclass(frozen=True)
class Release:
    version: str
    app_version: str
    created: str
    url: str
    sha256: str
    changes: Changes | None = None  # None only for the baseline
    baseline: bool = False
    note: str = ""

    def __post_init__(self) -> None:
        if not _VERSION.fullmatch(self.version):
            raise ValueError(f"release version {self.version!r} is not major.minor.patch")
        if not _SHA256.fullmatch(self.sha256):
            raise ValueError(f"release {self.version}: sha256 must be 64 lowercase hex characters")
        if not self.url.endswith(f"-{self.version}.tgz") or not self.url.startswith("https://"):
            raise ValueError(f"release {self.version}: url {self.url!r} is not an https archive of this version")
        datetime.fromisoformat(self.created)
        if self.baseline == (self.changes is not None):
            raise ValueError(f"release {self.version}: exactly the baseline has no changes")

    @property
    def expects_zero_agent_turns(self) -> bool:
        """The common case: same workloads, no removed or schema-checked values, so Go advances without an agent."""
        return self.changes is not None and not self.changes.changes_shape and not self.changes.breaks_values

    def pins(self, base: dict[str, Any]) -> dict[str, Any]:
        """``pins.json`` with its upstream chart replaced by this release (for assembling a lane at this release)."""
        pinned = json.loads(json.dumps(base))
        pinned["gitlab_chart"] = {
            **base["gitlab_chart"],
            "version": self.version,
            "app_version": self.app_version,
            "url": self.url,
            "sha256": self.sha256,
        }
        return pinned


@dataclass(frozen=True)
class ReleaseList:
    chart: str
    line: str
    releases: tuple[Release, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.releases or not self.releases[0].baseline or any(r.baseline for r in self.releases[1:]):
            raise ValueError("a release list starts with exactly one baseline release")
        versions = [r.version for r in self.releases]
        if len(set(versions)) != len(versions):
            raise ValueError("release versions must be unique")
        created = [datetime.fromisoformat(r.created) for r in self.releases]
        if created != sorted(created):
            raise ValueError("releases must be listed in publication order")

    def get(self, version: str) -> Release:
        for release in self.releases:
            if release.version == version:
                return release
        raise ValueError(f"release {version} is not in the list ({', '.join(r.version for r in self.releases)})")

    def after(self, version: str) -> tuple[Release, ...]:
        """The releases to replay on a lane whose followed ref holds chart ``version``."""
        index = [r.version for r in self.releases].index(self.get(version).version)
        return self.releases[index + 1 :]


def parse_releases(document: dict[str, Any]) -> ReleaseList:
    releases = []
    for entry in document["releases"]:
        changes = entry.get("changes")
        releases.append(
            Release(
                version=entry["version"],
                app_version=entry["app_version"],
                created=entry["created"],
                url=entry["url"],
                sha256=entry["sha256"],
                changes=None
                if changes is None
                else Changes(
                    images_changed=changes["images_changed"],
                    values_keys_added=changes["values_keys_added"],
                    values_keys_removed=changes["values_keys_removed"],
                    values_schema=changes["values_schema"],
                    workloads_added=tuple(changes["workloads_added"]),
                    workloads_removed=tuple(changes["workloads_removed"]),
                    objects=changes["objects"],
                ),
                baseline=bool(entry.get("baseline", False)),
                note=entry.get("note", ""),
            )
        )
    return ReleaseList(document["chart"], document["line"], tuple(releases))


def load_releases(path: Path = RELEASES_PATH) -> ReleaseList:
    return parse_releases(json.loads(path.read_text(encoding="utf-8")))
