"""Throwaway refs must never publish Go caches that evict their base branches."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]


def test_go_cache_writes_belong_only_to_durable_base_pushes() -> None:
    configuration = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    for job in configuration["jobs"].values():
        steps = job.get("steps", [])
        for step in steps:
            action = step.get("uses", "")
            if action.startswith("actions/setup-go@"):
                assert step["with"].get("cache") is False
            if action.startswith(("actions/cache@", "actions/cache/save@")):
                condition = step.get("if", "")
                assert "github.event_name == 'push'" in condition
                assert "github.ref == 'refs/heads/main'" in condition
                assert "github.ref == 'refs/heads/vic-lsh/dev/sdo-improve'" in condition
                assert any(
                    restore.get("uses", "").startswith("actions/cache/restore@")
                    and restore["with"]["key"] == step["with"]["key"]
                    and restore["with"]["path"] == step["with"]["path"]
                    for restore in steps
                )
