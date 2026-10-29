#!/usr/bin/env python3
"""Qualify a model worker with one real Claude turn using the health judge's access.

This spends provider quota and requires provisioned credentials. The pod or
container provides isolation; the CLI has no inner workspace sandbox. A small
source fixture and health-diagnostic write exercise the same structured runner
and shell tools as health preparation. This proves startup, not detector quality.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from libs.agent_cli.structured import run_structured_turn, turn_usage
from sdo.agents.responder.credentials import prepare_claude_home


def main() -> None:
    prepare_claude_home(skills=("sdo-memory", "sdo-detector-authoring"))
    with tempfile.TemporaryDirectory(prefix="sdo-health-worker-") as temporary:
        workspace = Path(temporary)
        (workspace / "app.yaml").write_text(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: synthetic-health-probe\n"
        )
        proof = workspace / ".sdo/diagnostics/health/proof"
        turn = run_structured_turn(
            "claude",
            "Inspect app.yaml, then author .sdo/diagnostics/health/proof containing exactly health-worker-ok. "
            "Use Bash to write it. Return the structured answer health-worker-ok.",
            output_schema={
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": False,
            },
            cwd=workspace,
            access="danger-full-access",
            model="claude-opus-5-5",
            reasoning_effort="medium",
            timeout_seconds=180,
        )
        if not proof.is_file() or proof.read_text().strip() != "health-worker-ok" or not turn.shell_commands:
            raise RuntimeError("live Claude worker did not author its health diagnostic through Bash")
        if json.loads(turn.output_json) != {"answer": "health-worker-ok"}:
            raise RuntimeError("live Claude worker returned an unexpected structured result")
        print(
            json.dumps(
                {
                    "passed": True,
                    "access": "danger-full-access",
                    "provider": "claude",
                    "model": "claude-opus-5-5",
                    "effort": "medium",
                    "usage": turn_usage(turn),
                    "session_id": turn.session_id,
                    "shell_commands": len(turn.shell_commands),
                    "rate_limit_wait_seconds": turn.rate_limit_wait_seconds,
                    "concurrency_wait_seconds": turn.concurrency_wait_seconds,
                }
            )
        )


if __name__ == "__main__":
    main()
