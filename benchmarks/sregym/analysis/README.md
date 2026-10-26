# Offline analysis commands

Run commands from the repository root with `uv run python -m` and the module below.
These tools read saved evidence; they do not contact a model or mutate a lane.

| Module (`benchmarks.sregym.analysis.` prefix) | Evidence and output |
| --- | --- |
| `run_validity` | Run manifests, receipts and results → validity classification |
| `incident_cost` | Receipts and provider rollouts → judge-free incident clocks and usage |
| `stream_curve` | Incident stream results → learning/coverage tables |
| `encounter_timeline` | Encounter audit, lifecycle stream, broker ledger and Pod exports → phase durations with missing/unfinished clocks preserved |

Example:

```bash
uv run python -m benchmarks.sregym.analysis.encounter_timeline RUN/gate2/encounters/00 \
  --incident-id hotel-reservation-123 \
  --runtime RUN/gate2/encounters/00/terminal-runtime-export/sdo_runtime \
  --workers RUN/gate2/workers \
  --rollout RUN/gate2/encounters/00/terminal-runtime-export/sdo_runtime/sessions/DIGEST/codex/sessions/DATE/rollout.jsonl
```

`--receipt` optionally supplies the strict production receipt's native ledger.
`--rows` reads the matching incident from `incidents.jsonl`, preserving driver
phase durations and the separate closure-to-grade delay. `--controller-log`
reads machine-framed validator stage durations from a timestamped controller log.
Use `--help` for the complete interface. The output includes its source paths;
`complete`, `incomplete`, `unknown`, and `invalid` distinguish absent evidence,
missing endpoints, and clock reversals. Durations overlap and must not be added.
A worker's MCP `check_memory` calls use execution clocks, scoped to reflection;
these are nested checks, not additional model time. Stage timings use elapsed
clocks rather than invented wall-clock positions. The controller ledger's
relaunch/registry marks occur after successful startup and cannot time a rebuild.
