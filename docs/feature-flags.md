# Configuration boundaries

The Go `sdo-operator host` command owns production lifecycle orchestration. It accepts a clean Git repository, an application and health objective, agent registrations, and Kubernetes images and credentials. `host` starts an in-cluster `operate` Job, which accepts source and operational-memory proposals through the Go broker, verifies the deployed source and health detectors, and installs the Go controller.

## Incident and reflection policy

`--repair-policy {commit,recorded-actions}` selects the evidence the controller requires for a responder repair. `commit` is the default. `recorded-actions` is available for benchmark runs that repair live cluster state without a source commit.

`--reflection-session {fresh,resume}` defaults to `fresh`. `resume` requires a valid responder session token associated with the same accepted closure. The controller keeps private sessions outside the accepted repository for a bounded retention period and provides typed logical memory to reflection workers.

`--reflection-guidance {baseline,generalize,generalize-spec}` changes the agent's learning instructions. `baseline` learns from each confirmed cause; `generalize` asks the agent to compare related incident signatures and parameterize a shared playbook; `generalize-spec` favors predicates visible directly in resource spec or status when the cause permits. These modes do not change the Go detector SDK or the independent validator.

`--late-findings {off,pull}` defaults to `off`. In `pull`, the controller publishes a bounded, incident-specific report through a read-only worker projection. A responder uses `sdo-operator late-findings --incident-id ID` to fetch logical detector and playbook IDs. The Go broker checks the worker's pull claim against the controller-owned report before recording it in the outcome. Workers do not mount the accepted repository or controller telemetry.

## Healthy detector baseline

`--healthy-baseline` is an opt-in detector gate. A benchmark adapter captures healthy Kubernetes snapshots before injecting a fault and publishes them to a controller-owned directory outside the accepted repository using `sdo-operator baseline-write`. When a proposal changes incident detector source, the isolated validator mounts only that snapshot directory read-only and replays the proposed detector against it. A detector that fires on a healthy snapshot fails validation. The gate does not read benchmark verdicts. If the recorded baseline is missing when an incident detector is validated, validation fails closed.

The Go builder also accepts a `HealthyBaseline` directory for local detector tests. The directory contains one `*.json` file per `sdktest.Snapshot`; an empty directory or malformed snapshot is rejected.

## SREGym experiment configuration

SREGym options remain under `benchmarks/sregym/runner/` and its experiment configurations. Benchmark adapters may call the Go host, controller inspection, maintenance, and evidence commands; production code does not import benchmark packages.

`preserve_infrastructure = true` retains shared benchmark storage and observability between attempts after checking readiness. `defer_diagnosis_grading = true` runs benchmark diagnosis grading in the background while mitigation proceeds. `fast_namespace_teardown = true` accelerates benchmark cleanup. `source_build_cache = true` reuses source images with matching build-context digests. These settings do not alter the Go broker's acceptance rules.

Common configuration sections are:

| Section | Purpose |
|---|---|
| `[runner]` | Agent, model, parallelism, problem selection, and application workspace |
| `[runner.variants]` | Variant generation, ordering, limits, and seeds |
| `[runner.env]` | Benchmark worker and judge environment |
| `[agent.sdo_codex]` | SDO benchmark adapter model and timeout settings |
| `[pipeline]`, `[defaults]`, `[[stages]]` | Multi-stage experiments and stage overrides |

Use emitted configuration snapshots when analyzing results: they contain resolved defaults and stage overrides.
