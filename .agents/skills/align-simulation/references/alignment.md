# Alignment reference

## Traces

Record the same trace shape from the simulation and from the real lane, so the two diff mechanically.

Decision level (diff first; any difference here matters):

- detector findings per evaluation: detector, rule, primary resource role, status;
- incidents: open, attach, close, with the finding set that opened each;
- controller effects: dispatches, plan applications, verifications, rollbacks, broker commits;
- program verdicts: Match or NoMatch and the plan chosen;
- memory-host lifecycle: swap, crash, restart.

Edge level (diff second; explains decision differences):

- Kubernetes API calls: verb, kind, outcome (success or error class), preconditions, and the resource
  version order seen by the caller;
- informer events: kind, add/update/delete, order relative to the controller's own writes;
- pod and rollout state transitions;
- kubelet usage samples, log reads (counts and truncation), probe and dial results with error class.

Normalize before diffing:

- replace names, UIDs and IPs with stable roles (the finding's resource, its peer, the backing pod);
- replace timestamps with order, and with durations bucketed to the controller's own windows
  (persistence, debounce, probe interval);
- drop fields the controller never reads.

Real runs are noisier than the simulation. When a difference comes from real scheduling noise, make the
comparison robust (compare sets or order classes instead of exact sequences) and record the noise as a
fidelity bound; do not hide it by loosening a decision-level comparison.

## Gap classes

| Class | Real-world example | Simulation fix |
| --- | --- | --- |
| Missing behavior | an object kind, field or state the simulation does not represent | model it in the simulated cluster or edge |
| Wrong semantics | API defaulting, error codes, conflict handling, watch ordering differ | correct the model and add a conformance test |
| Missing fault | a real failure mode cannot be injected (partial write, stale list, dropped watch, slow kubelet) | add it to the seeded fault injector |
| Timing model | real event order the simulation never produces (rollout progression, readiness lag) | add the ordering to the scheduler of simulated events |
| Fidelity bound | behavior deliberately not modeled (kernel networking details, image pulls) | record it with its reason and cover it with an integration-tagged test |

## Choosing scenarios

- One scenario per edge: API writes under conflict, informer staleness, pod restart, rollout, probe failure,
  log truncation, memory-host crash.
- Every incident class a real run hit in the last week.
- At least one controller restart at a decision point.
