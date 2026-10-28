# CI tooling map

- `prepare_go_ci.sh`: compiles the normal and race graphs, downloads modules,
  builds the source-verified CLI, and records the compiled normal test inventory.
- `go_cost_plan.py`: deterministic first-fit decreasing allocation of that inventory
  using `operator_test_costs.json` and `runtime_test_costs.json`. Each bin gets 210
  seconds of measured work inside the unchanged five-minute job, leaving 90
  seconds for setup. Unknown tests reserve 180 seconds. Capacity grows through
  the generated Actions matrix; no test placement lists are maintained. A single
  test above the work budget is included in the nightly overflow suite.
- `test_operator_shard.py`, `test_runtime_shard.py`: run a numbered computed bin
  (or `nightly`), validating its budget before executing the real race suite.
  Without `--plan`, they compile the inventory and compute a local plan.
- `go_test_timing.py`: streams Go JSON and reports actual active costs, including
  parallel children and excluding pauses. Actual assertion failures still fail.
- `update_go_test_costs.py`: refreshes costs from hosted JSON logs, with run/head
  provenance. Use `--log <log> --run <run-id> --head <sha> --write`. Fresh hosted
  costs change capacity automatically on the next run. Removed names are harmless.

`tests/unit/scripts/test_go_cost_plan.py` guards assignment, conservative defaults,
coverage, deterministic output and the over-budget negative control. The workflow
contract tests live in `tests/unit/sdo/operator/test_runtime_shards.py`.
