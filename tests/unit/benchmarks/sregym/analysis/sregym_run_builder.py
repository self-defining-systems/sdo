"""Synthetic SREGym run directories for the validity and incident-cost tests.

They follow SREGym's parallel layout (``runs/<seq>_<problem>/worker_<n>/``);
a :class:`Run` left at its defaults is a valid stock Codex run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

PROBLEM = "missing_configmap_hotel_reservation"
INJECTED = 1_790_000_000.0
CSV_HEADER = (
    "Diagnosis.success,Mitigation.success,TTL,TTM,"
    "diagnosis_submitted_at,fault_injected_at,mitigation_submitted_at,problem_id"
)
GUARD = (
    "INFO     all.infra.k8s_proxy - Agent kubeconfig\n"
    "                             /tmp/sregym-agent-kubeconfig-p16443 verified: port\n"
    "                             16443 reaches only luna-w0\n"
)


def usage(input_tokens: int = 1000, cached: int = 800, output: int = 50) -> dict[str, int]:
    return {"input_tokens": input_tokens, "cached_input_tokens": cached, "output_tokens": output}


def _rollout(turns: list[dict[str, int]], *, tool_output: str = "pod/geo-1 Running on luna-w0-worker") -> str:
    lines = [{"timestamp": "2026-09-27T21:00:00.000Z", "type": "event_msg", "payload": {"type": "task_started"}}]
    total = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    for turn in turns:
        total = {key: total[key] + turn[key] for key in total}
        lines.append(
            {
                "timestamp": "2026-09-27T21:00:01.000Z",
                "type": "event_msg",
                "payload": {"type": "token_count", "info": {"last_token_usage": turn, "total_token_usage": total}},
            }
        )
    lines.append(
        {
            "timestamp": "2026-09-27T21:00:02.000Z",
            "type": "response_item",
            "payload": {"type": "function_call_output", "call_id": "c1", "output": tool_output},
        }
    )
    return "\n".join(json.dumps(line) for line in lines) + "\n"


@dataclass
class Run:
    """One synthetic problem run; attributes left at their defaults form a valid run."""

    agent: str = "codex"
    diagnosis: str = "True"
    mitigation: str = "True"
    ttl: str = "30"
    diagnosed: str = str(INJECTED + 20)
    mitigated: str = str(INJECTED + 60)
    worker_log: str = GUARD + "== Fault Injection ==\n" + GUARD
    manifest: dict[str, Any] | None = field(
        default_factory=lambda: {"schema_version": 1, "preflight": {"ok": True, "waived": False, "checks": []}}
    )
    install_rc: str = "0"
    codex_events: str = '{"type":"turn.completed"}\n'
    usage_metrics: dict[str, int] | None = field(default_factory=usage)
    rollout_turns: list[dict[str, int]] | None = field(
        default_factory=lambda: [usage(600, 500, 20), usage(400, 300, 30)]
    )
    tool_output: str = "pod/geo-1 Running on luna-w0-worker"
    receipt: dict[str, Any] | None = None
    controller_log: str = ""
    #: ``agent-resolved.jsonl`` records the SDO workers wrote beside their incident session.
    resolved_agents: list[dict[str, Any]] | None = None

    def write(self, experiment_dir: Path, sequence: int = 0) -> Path:
        experiment_dir.mkdir(parents=True, exist_ok=True)
        (experiment_dir / "experiment_config.toml").write_text(
            f'[runner]\nagent = "{self.agent}"\nmodel = "gpt-6-luna"\n', encoding="utf-8"
        )
        if self.manifest is not None:
            (experiment_dir / "run_manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")
        worker = experiment_dir / "runs" / f"{sequence:06d}_{PROBLEM}" / "worker_0"
        run = worker / "results" / self.agent / PROBLEM / "run_1"
        run.mkdir(parents=True)
        (worker / "worker.log").write_text(self.worker_log, encoding="utf-8")
        row = f"{self.diagnosis},{self.mitigation},{self.ttl},90,{self.diagnosed},{INJECTED},{self.mitigated},{PROBLEM}"
        (worker / "results" / f"{self.agent}_ALL_results.csv").write_text(f"{CSV_HEADER}\n{row}\n", encoding="utf-8")
        if self.agent == "codex":
            (run / "install.rc").write_text(self.install_rc + "\n", encoding="utf-8")
            (run / "install.log").write_text("npm error 404 Not Found - @openai/codex-linux-x64\n", encoding="utf-8")
            (run / "codex.txt").write_text(self.codex_events, encoding="utf-8")
            if self.usage_metrics is not None:
                (run / f"codex_results_{PROBLEM}_1.json").write_text(
                    json.dumps({"usage_metrics": self.usage_metrics}), encoding="utf-8"
                )
            if self.rollout_turns is not None:
                sessions = run / "sessions" / "2026" / "09" / "27"
                sessions.mkdir(parents=True)
                (sessions / "rollout-2026-09-27T21-00-00-s1.jsonl").write_text(
                    _rollout(self.rollout_turns, tool_output=self.tool_output), encoding="utf-8"
                )
        else:
            if self.receipt is not None:
                (run / "sdo_production_receipt_strict.json").write_text(json.dumps(self.receipt), encoding="utf-8")
            if self.rollout_turns is not None:
                sessions = run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27"
                sessions.mkdir(parents=True)
                (sessions / "rollout-2026-09-27T21-00-00-resp1.jsonl").write_text(
                    _rollout(self.rollout_turns, tool_output=self.tool_output), encoding="utf-8"
                )
            if self.resolved_agents is not None:
                session = run / "sdo_runtime" / "sessions" / "abc123"
                session.mkdir(parents=True)
                (session / "agent-resolved.jsonl").write_text(
                    "".join(json.dumps(record) + "\n" for record in self.resolved_agents), encoding="utf-8"
                )
            if self.controller_log:
                logs = run / "sdo_runtime" / "controller_logs"
                logs.mkdir(parents=True)
                (logs / "sdo-controller-run-x.log").write_text(self.controller_log, encoding="utf-8")
        return experiment_dir
