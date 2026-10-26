# Encounter phase analysis

`uv run python -m benchmarks.sregym.analysis.encounter_timeline ENCOUNTER
--incident-id ID --runtime EXPORT/sdo_runtime --workers ARM/workers` reads saved
OP encounter evidence and emits `sdo.encounter-timeline/v1`. Add `--receipt` for
a strict receipt and repeat `--rollout` for scoped provider transcripts. See the
[analysis command index](../../../../benchmarks/sregym/analysis/README.md).
The fixture-tested projection joins lifecycle events by incident, the broker's
`closure_phase_timestamps`, pre-injection audit clocks, and actual Pod agent
container start times. Dispatch timestamps can be backdated to incident open;
they are not proof that the container began then. Reflection's completed MCP
`check_memory` calls use execution clocks and stay nested inside worker time.
Missing endpoints are null (`unknown` or `incomplete`), never zero; reversed
clocks are `invalid`. Toolchain stages are elapsed-only measurements. Tests
include replay, so do not add a separate inferred replay duration. Pod startup
includes volume mounts and image handling; image-pull time is known only when
both Pulling and Pulled events were captured. Report overlapping intervals
separately. In particular, health closure may precede worker completion.
`controller_restart_relaunch` and `program_registry_rebuild` are recorded together
after successful startup; subtracting them would fabricate a zero-cost build.
Absent actual build and rollout clocks, leave those subphases unknown.
