# Fault observation tools

`worker.py` resolves the pinned harness's problem metadata offline. `description.py`
validates tracking separately from production repair capability. The recipes in
`probe_catalog.py` are implemented by `fastloop/fault_tracker.py` and
`fastloop/object_probes.py`. Runtime decisions live in `fastloop/runtime_probes.py`;
`fastloop/runtime_observation.py` is their shared, fixed read-only transport.

Before allocating a lane for a plan, run:

```bash
uv run python -m benchmarks.sregym.fastloop check-plan --plan path/to/sequence_plan.json
```

The command accepts canonical sequence/draw plans and fastloop incident streams.
It discovers every selected problem in the pinned harness and rejects any missing
probe or duplicate fault identity. It makes no cluster, Docker or model calls.
`fastloop preflight --plan path/to/plan.json` applies the same check before host
checks. `run --require-fault-tracking` remains the final per-stream admission gate.

Configuration restoration probes freeze the relevant healthy live fields before
injection and require a current available rollout with those fields restored.
They are conservative: another valid configuration can remain unresolved, and a
peer fault on the same workload can delay their green sample. Feature-flag probes
observe the relevant flag only; admission probes observe the affected workload's
replica recovery. These are benchmark observations, never production learning or
repair instructions. Neither oracle verdicts nor Secret data are inputs.

Runtime probes observe state outside Kubernetes objects through bounded reads:
current Pod socket sysctls, Valkey memory/cgroup headroom, Kafka consumer offsets
and output-topic progress, dependent app GET handlers, a served public TLS
certificate, and fresh authenticated Mongo app-collection queries. Readiness is
only a prerequisite, never the repair evidence for these faults. Errors stay
unresolved; upstream command output/errors are not persisted in timelines.

Mongo geo/rate queries derive the development login only in memory from the
exact SDO → harness → nested-app Git pins and `cmd/{geo,rate}/db.go`. The program
travels over private stdin, with no login literals in Python, fixtures, arguments
or reports. There is no root or Secret/config/env lookup.
They open a fresh process and read the actual app collection, bypassing the hotel
services' caches. Other Mongo targets remain unsupported. Auth/stale-credential
HTTP probes call the dependent app's real read path, and TLS reads the certificate
presented by the ingress with hostname and validity checks; no Secret is read.
A valid repair with a different app login can conservatively stay unresolved.

The Phase 4 seed42 fixture and offline regression cover all 50 selected problems
(170 faults, 57 classes). A future class or unsupported variant still fails plan
admission. These independent observations remain benchmark timing evidence, not
production learning, repair instructions, or official grader proof.
