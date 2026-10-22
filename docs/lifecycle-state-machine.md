# Controller lifecycle state machine

The durable schema is `sdo/contracts/proto/sdodev/contracts/v1alpha1/controller_state.proto`.
The controller exports and restores the generated `ControllerState` directly;
there is no second persistence struct. Canonical ProtoJSON uses enum names.
Unknown names and undefined numeric enum values fail validation. This is a
breaking format change; old phase strings require an operational cutover.

## States, events and effects

Responder dispatch phases: unspecified/idle, admission deferred, workspace
pending/running, pending/running, completed. Admission holds the incident's
identity and findings. Workspace preparation and responder dispatch are separate
effects. Completion retains results for independent verification; timeout is a
typed worker outcome, while API/transport failure rejoins the same worker.
Successful verification freezes the closure.

Closure phases: unspecified, pending/processing, committed/acknowledging, failed.
Its nested reflection sequence is dispatch, worker result, format, independent
validator, broker publication, done or failure. Outcome publication precedes
reflection. Validation rejection may dispatch a bounded correction;
infrastructure failure retries the same work. A committed closure waits for any
required controller rollout before acknowledgment and worktree release.

Events: incident opened, admission reassessed, workspace prepared/rejected,
responder completed/timed out, detector observation, verification elapsed,
closure frozen, reflection result, format result, validator accepted/rejected,
broker accepted/rejected, acknowledgment result, API fault, leadership loss,
controller restart and clock advanced. Events carry incident/effect identity;
clock-dependent decisions consume the event timestamp. Effects are plain values:
prepare workspace, dispatch agent, format proposal, validate proposal, publish
commit, acknowledge closure and release workspace. The driver owns contexts,
clocks, Kubernetes, files and Git.

Snapshots convert in-flight workspace/dispatch phases to pending, and closure
processing/acknowledging to pending/committed. Restart reuses persisted requests
and completed results. The persisted incident view is for readers; restore
ignores it and publishes fresh observations.

## Invariants and checking scope

* Go owns decisions and accepted commits; Python transports generated contracts.
* The broker alone accepts operational artifacts, with independent validation.
* A pure transition consumes only state and event, never wall time, RNG or I/O.
* Effect identity and request bytes depend on incident, accepted commit and step.
  Retrying an effect cannot alter its deadline or discard a completed result.
* A permanent failure belongs to one incident and must allow the next incident.
* Diagnosis precedes mitigation, with at most one graded mitigation per incident.
* Child deadlines fit the remaining parent budget with headroom.
* New faults observed during an incident survive its acknowledgment.

Slice 1 enforces generated, closed phase types and retains existing behavior.
Pure transitions, the exhaustive fault/restart explorer and real-driver simulation
are subsequent slices. The current controller still requires operator intervention
after exhausting closure retries; phase typing alone does not prove progress.

## Further functional-core extractions

| Subsystem | Pure core to extract | Private edge under that subsystem's `internal/...` |
|---|---|---|
| Broker | Ownership, commit eligibility, rejection/correction transitions and accepted-revision decisions | Git/worktree mutation, locks, validator Jobs and durable commit receipts |
| Memory | Logical artifact projection, schema validation, snapshot selection and content identities | Repository reads, physical layout, outcome persistence and migration |
| Late findings | Report selection, digest/correlation rules and pull-budget transitions | HTTP transport, snapshot observation and report persistence |
| Runtime controller | Scheduling, batching, incident/closure transitions and deadline calculation | Kubernetes caches/Jobs/ConfigMaps, timers, telemetry, leader election; lifecycle persistence is already `runtime/internal/lifecycleio` |
| Program learning / fast path | Trust credit, revision binding, match eligibility, one-attempt and verification transitions | Live snapshots, action application/rollback, capture replay and attempt-marker storage |

Each core accepts plain events and returns decisions/effects. Other subsystems
send events through its driver. Internal visibility prevents outsiders importing
the edge; depguard must also prevent the core importing its own edge. Other
subsystem extractions are follow-up work, not part of this task.
