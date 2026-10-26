# Fault observation tools

`worker.py` resolves the pinned harness's problem metadata offline. `description.py`
validates tracking separately from production repair capability. The recipes in
`probe_catalog.py` are implemented by `fastloop/fault_tracker.py` and
`fastloop/object_probes.py`.

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

Some faults affect state outside Kubernetes objects (database users, Kafka
progress, Valkey runtime tuning, socket sysctls, served TLS certificates, resolved
credential freshness). They stay unsupported until an independent, read-only
observation for that state is implemented. Readiness alone does not establish
repair for these faults. The Phase 4 seed42 fixture and its offline regression
make the complete selection and every remaining unsupported class explicit.
